import os
import unittest
import tempfile
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from celery_doctor import extract_settings_from_python_file, lint_celery_config

class TestCeleryDoctorAST(unittest.TestCase):
    def test_lint_distinguishes_missing_and_unsafe(self):
        settings = {
            "broker_url": {"value": "redis://localhost:6379/0", "status": "STATIC_LITERAL"},
            "task_acks_late": {"value": False, "status": "STATIC_LITERAL"}  # Explicitly unsafe
        }
        res = lint_celery_config(settings, workload_profile="heterogeneous")
        self.assertFalse(res["is_safe"])
        unsafe_names = [f["setting"] for f in res["explicitly_unsafe"]]
        self.assertIn("task_acks_late", unsafe_names)

        missing_names = [f["setting"] for f in res["missing_config"]]
        self.assertIn("task_reject_on_worker_lost", missing_names)

    def test_lint_detects_dynamic_expressions(self):
        code = """
import os
task_acks_late = bool(os.environ.get("CELERY_ACKS_LATE", True))
worker_prefetch_multiplier = int(os.getenv("PREFETCH", "1"))
task_time_limit = 1800
"""
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_path = f.name
        try:
            extracted = extract_settings_from_python_file(temp_path)
            self.assertEqual(extracted["task_acks_late"]["status"], "DYNAMICALLY_COMPUTED")
            self.assertEqual(extracted["task_time_limit"]["status"], "STATIC_LITERAL")
            self.assertEqual(extracted["task_time_limit"]["value"], 1800)

            res = lint_celery_config(extracted)
            dyn_names = [d["setting"] for d in res["dynamically_computed"]]
            self.assertIn("task_acks_late", dyn_names)
            self.assertIn("worker_prefetch_multiplier", dyn_names)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_lint_workload_profile_differences(self):
        # Prefetch = 4 on homogeneous workload vs heterogeneous
        settings = {
            "task_acks_late": {"value": True, "status": "STATIC_LITERAL"},
            "task_reject_on_worker_lost": {"value": True, "status": "STATIC_LITERAL"},
            "worker_prefetch_multiplier": {"value": 4, "status": "STATIC_LITERAL"},
            "worker_max_tasks_per_child": {"value": 100, "status": "STATIC_LITERAL"},
            "task_time_limit": {"value": 600, "status": "STATIC_LITERAL"},
            "task_soft_time_limit": {"value": 500, "status": "STATIC_LITERAL"},
            "broker_connection_retry_on_startup": {"value": True, "status": "STATIC_LITERAL"}
        }
        res_hetero = lint_celery_config(settings, workload_profile="heterogeneous")
        self.assertTrue(len(res_hetero["defaults_requiring_verification"]) > 0)

        res_homo = lint_celery_config(settings, workload_profile="homogeneous")
        # In homogeneous profile, prefetch=4 is accepted
        self.assertEqual(len(res_homo["defaults_requiring_verification"]), 0)

    def test_lint_passes_recommended_config(self):
        settings = {
            "task_acks_late": {"value": True, "status": "STATIC_LITERAL"},
            "task_reject_on_worker_lost": {"value": True, "status": "STATIC_LITERAL"},
            "worker_prefetch_multiplier": {"value": 1, "status": "STATIC_LITERAL"},
            "worker_max_tasks_per_child": {"value": 500, "status": "STATIC_LITERAL"},
            "task_time_limit": {"value": 1800, "status": "STATIC_LITERAL"},
            "task_soft_time_limit": {"value": 1500, "status": "STATIC_LITERAL"},
            "broker_connection_retry_on_startup": {"value": True, "status": "STATIC_LITERAL"}
        }
        res = lint_celery_config(settings)
        self.assertTrue(res["is_safe"])
        self.assertEqual(len(res["explicitly_unsafe"]), 0)
        self.assertEqual(len(res["missing_config"]), 0)

if __name__ == "__main__":
    unittest.main()
