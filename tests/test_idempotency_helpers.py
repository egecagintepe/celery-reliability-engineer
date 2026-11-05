import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from idempotency_helpers import IdempotentTaskLock, TaskLockAcquisitionError

class TestIdempotencyHelpers(unittest.TestCase):
    def test_single_lock_acquire_and_release(self):
        lock = IdempotentTaskLock(key="task_123", ttl_seconds=10)
        self.assertTrue(lock.acquire())
        self.assertTrue(lock.release())

    def test_concurrent_lock_fails(self):
        lock1 = IdempotentTaskLock(key="exclusive_task", ttl_seconds=10)
        lock2 = IdempotentTaskLock(key="exclusive_task", ttl_seconds=10)

        self.assertTrue(lock1.acquire())
        # Second acquire should fail
        self.assertFalse(lock2.acquire())

        # Release first lock
        self.assertTrue(lock1.release())

        # Now second acquire should succeed
        self.assertTrue(lock2.acquire())
        self.assertTrue(lock2.release())

    def test_context_manager_behavior(self):
        with IdempotentTaskLock(key="ctx_test", ttl_seconds=5) as lock:
            self.assertTrue(lock.acquired)
            # Re-entrant with another lock on same key should raise
            with self.assertRaises(TaskLockAcquisitionError):
                with IdempotentTaskLock(key="ctx_test", ttl_seconds=5):
                    pass

        # After exiting context, lock should be released
        with IdempotentTaskLock(key="ctx_test", ttl_seconds=5) as lock2:
            self.assertTrue(lock2.acquired)

if __name__ == "__main__":
    unittest.main()
