import os
import unittest
from unittest.mock import MagicMock
import sys
import concurrent.futures

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from idempotency_helpers import (
    RedisDistributedLock,
    LocalProcessLock,
    TaskLockAcquisitionError,
    RedisLockConnectionError
)

class TestDistributedLockFailClosed(unittest.TestCase):
    def test_missing_redis_client_raises_value_error(self):
        with self.assertRaises(ValueError):
            RedisDistributedLock(key="test", ttl_seconds=30, redis_client=None)

    def test_invalid_ttl_raises_value_error(self):
        mock_redis = MagicMock()
        with self.assertRaises(ValueError):
            RedisDistributedLock(key="test", ttl_seconds=-5, redis_client=mock_redis)

    def test_redis_connection_failure_fails_closed(self):
        mock_redis = MagicMock()
        mock_redis.set.side_effect = ConnectionError("Redis cluster unreachable")

        lock = RedisDistributedLock(key="order_123", ttl_seconds=30, redis_client=mock_redis)
        with self.assertRaises(RedisLockConnectionError):
            lock.acquire()

    def test_redis_successful_acquire_and_release(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.eval.return_value = 1

        lock = RedisDistributedLock(key="order_123", ttl_seconds=30, redis_client=mock_redis)
        self.assertTrue(lock.acquire())
        mock_redis.set.assert_called_once()

        self.assertTrue(lock.release())
        mock_redis.eval.assert_called_once()

    def test_redis_lock_already_held_raises_acquisition_error(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = False  # Lock already exists

        lock = RedisDistributedLock(key="order_123", ttl_seconds=30, redis_client=mock_redis)
        with self.assertRaises(TaskLockAcquisitionError):
            with lock:
                pass


class TestLocalProcessLock(unittest.TestCase):
    def test_local_acquire_and_release(self):
        lock = LocalProcessLock(key="task_local_1", ttl_seconds=5)
        self.assertTrue(lock.acquire())
        self.assertTrue(lock.release())

    def test_local_concurrent_rejection(self):
        lock1 = LocalProcessLock(key="task_local_2", ttl_seconds=5)
        lock2 = LocalProcessLock(key="task_local_2", ttl_seconds=5)

        self.assertTrue(lock1.acquire())
        self.assertFalse(lock2.acquire())

        lock1.release()
        self.assertTrue(lock2.acquire())
        lock2.release()

    def test_local_multithreaded_concurrency(self):
        results = []
        def try_lock():
            lock = LocalProcessLock(key="concurrent_thread_key", ttl_seconds=2)
            if lock.acquire():
                results.append("acquired")
                import time
                time.sleep(0.05)
                lock.release()
            else:
                results.append("rejected")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(try_lock) for _ in range(5)]
            concurrent.futures.wait(futures)

        # Only 1 thread should have acquired the lock while others were running concurrently
        self.assertIn("acquired", results)
        self.assertIn("rejected", results)

if __name__ == "__main__":
    unittest.main()
