import os
import unittest
import tempfile
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from celery_doctor import extract_settings_from_python_file, lint_celery_config

class TestCeleryDoctor(unittest.TestCase):
    def test_lint_detects_missing_critical_settings(self):
        settings = {
            "broker_url": "redis://localhost:6379/0"
        }
        findings = lint_celery_config(settings)
        critical_settings = [f["setting"] for f in findings if f["severity"] == "CRITICAL"]
        self.assertIn("task_acks_late", critical_settings)
        self.assertIn("task_reject_on_worker_lost", critical_settings)

    def test_lint_detects_bad_prefetch(self):
        settings = {
            "task_acks_late": True,
            "task_reject_on_worker_lost": True,
            "worker_prefetch_multiplier": 4,  # Bad for mixed workloads
            "worker_max_tasks_per_child": 100,
            "task_time_limit": 600,
            "task_soft_time_limit": 500,
            "broker_connection_retry_on_startup": True
        }
        findings = lint_celery_config(settings)
        high_settings = [f["setting"] for f in findings if f["severity"] == "HIGH"]
        self.assertIn("worker_prefetch_multiplier", high_settings)

    def test_lint_detects_autoscale_hazard(self):
        settings = {
            "task_acks_late": True,
            "task_reject_on_worker_lost": True,
            "worker_prefetch_multiplier": 1,
            "worker_autoscale": [10, 2],
            "worker_max_tasks_per_child": 100,
            "task_time_limit": 600,
            "task_soft_time_limit": 500,
            "broker_connection_retry_on_startup": True
        }
        findings = lint_celery_config(settings)
        warnings = [f["setting"] for f in findings if f["severity"] == "WARNING"]
        self.assertIn("worker_autoscale", warnings)

    def test_lint_passes_recommended_config(self):
        settings = {
            "task_acks_late": True,
            "task_reject_on_worker_lost": True,
            "worker_prefetch_multiplier": 1,
            "worker_max_tasks_per_child": 500,
            "task_time_limit": 1800,
            "task_soft_time_limit": 1500,
            "broker_connection_retry_on_startup": True
        }
        findings = lint_celery_config(settings)
        self.assertEqual(len(findings), 0)

    def test_extract_settings_from_file(self):
        code = """
task_acks_late = True
worker_prefetch_multiplier = 1
"""
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_path = f.name
        try:
            extracted = extract_settings_from_python_file(temp_path)
            self.assertTrue(extracted.get("task_acks_late"))
            self.assertEqual(extracted.get("worker_prefetch_multiplier"), 1)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

if __name__ == "__main__":
    unittest.main()
