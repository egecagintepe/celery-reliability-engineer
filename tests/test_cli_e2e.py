import json
import os
import subprocess
import sys
import tempfile
import unittest

CLI_PATH = os.path.join(os.path.dirname(__file__), "..", "scripts", "celery_doctor.py")

class TestCeleryDoctorCLI(unittest.TestCase):
    def test_lint_config_cli_json_pass(self):
        config_path = os.path.join(os.path.dirname(__file__), "..", "examples", "celery_conf_recommended.py")
        cmd = [sys.executable, CLI_PATH, "lint-config", "--config", config_path, "--json"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(res.returncode, 0)
        data = json.loads(res.stdout)
        self.assertTrue(data["is_safe"])
        self.assertEqual(len(data["explicitly_unsafe"]), 0)

    def test_lint_config_cli_json_fail_on_unsafe(self):
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write("task_acks_late = False\nworker_autoscale = [10, 2]\n")
            temp_path = f.name
        try:
            cmd = [sys.executable, CLI_PATH, "lint-config", "--config", temp_path, "--json"]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.assertEqual(res.returncode, 1)  # Critical hazard triggers exit 1
            data = json.loads(res.stdout)
            self.assertFalse(data["is_safe"])
            self.assertTrue(len(data["explicitly_unsafe"]) > 0)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_calc_backoff_cli(self):
        cmd = [sys.executable, CLI_PATH, "calc-backoff", "--attempt", "3", "--json"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(res.returncode, 0)
        data = json.loads(res.stdout)
        self.assertEqual(data["attempt"], 3)
        self.assertEqual(len(data["jittered_samples_seconds"]), 5)

    def test_simulate_herd_cli(self):
        cmd = [sys.executable, CLI_PATH, "simulate-herd", "--workers", "30", "--attempts", "2", "--json"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(res.returncode, 0)
        data = json.loads(res.stdout)
        self.assertEqual(data["workers"], 30)
        self.assertIn("fixed_retry_worst_spike", data)
        self.assertIn("jittered_retry_worst_spike", data)

if __name__ == "__main__":
    unittest.main()

