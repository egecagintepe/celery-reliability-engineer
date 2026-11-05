import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from idempotency_helpers import JitteredBackoff

class TestBackoffJitter(unittest.TestCase):
    def setUp(self):
        self.backoff = JitteredBackoff(base_delay=2.0, max_delay=30.0, factor=2.0)

    def test_delay_is_within_jitter_bounds(self):
        for attempt in range(1, 6):
            expected_cap = min(30.0, 2.0 * (2.0 ** (attempt - 1)))
            for _ in range(50):
                delay = self.backoff.calculate_delay(attempt)
                self.assertGreaterEqual(delay, 0.1)
                self.assertLessEqual(delay, expected_cap)

    def test_max_delay_cap(self):
        # High attempt should not exceed max_delay
        for _ in range(50):
            delay = self.backoff.calculate_delay(attempt=10)
            self.assertLessEqual(delay, 30.0)

    def test_quota_reset_delay(self):
        now = 1000.0
        reset = 1060.0
        # 60s remaining + 10..60s buffer
        delay = JitteredBackoff.calculate_quota_reset_delay(now, reset, min_buffer=60.0)
        self.assertGreaterEqual(delay, 70.0)
        self.assertLessEqual(delay, 120.0)

if __name__ == "__main__":
    unittest.main()
