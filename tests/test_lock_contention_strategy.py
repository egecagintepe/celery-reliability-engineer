import os
import unittest
from unittest.mock import MagicMock
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "examples"))

from idempotency_helpers import TaskLockAcquisitionError
from resilient_task_example import resilient_sync_task

class TestLockContentionStrategy(unittest.TestCase):
    def test_lock_contention_when_already_completed_is_acknowledged(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = False  # Lock held by someone else
        db_state = {"order_999": "COMPLETED"}

        # Because DB says COMPLETED, concurrent delivery can be safely acknowledged without re-running
        res = resilient_sync_task(
            task_id="order_999",
            payload={},
            redis_client=mock_redis,
            db_state=db_state,
            current_attempt=1,
            max_attempts=3
        )
        self.assertEqual(res["status"], "ALREADY_COMPLETED")

    def test_lock_contention_when_pending_raises_for_retry(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = False  # Lock held
        db_state = {"order_999": "PENDING"}

        # Work is not completed yet: MUST NOT return success/dropped; MUST raise for retry
        with self.assertRaises(TaskLockAcquisitionError):
            resilient_sync_task(
                task_id="order_999",
                payload={},
                redis_client=mock_redis,
                db_state=db_state,
                current_attempt=1,
                max_attempts=3
            )
        self.assertEqual(db_state["order_999"], "PENDING")

    def test_lock_contention_exhausts_retries_marks_failed(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = False
        db_state = {"order_999": "PENDING"}

        with self.assertRaises(TaskLockAcquisitionError):
            resilient_sync_task(
                task_id="order_999",
                payload={},
                redis_client=mock_redis,
                db_state=db_state,
                current_attempt=3,
                max_attempts=3
            )
        self.assertEqual(db_state["order_999"], "LOCK_FAILED")

if __name__ == "__main__":
    unittest.main()
