#!/usr/bin/env python3
"""
Celery Doctor: Diagnostic CLI for distributed task queue configuration,
backoff schedules, and thundering herd analysis.
Standard library only (zero external dependencies).
"""

import argparse
import ast
import json
import os
import random
import sys
from typing import Dict, List, Any, Tuple

RECOMMENDED_RELIABILITY_RULES = [
    {
        "setting": "task_acks_late",
        "aliases": ["CELERY_TASK_ACKS_LATE", "CELERY_ACKS_LATE"],
        "expected": True,
        "critical_for": "heterogeneous",
        "description": "Late acknowledgment (ack after task completion). Prevents task loss if worker dies."
    },
    {
        "setting": "task_reject_on_worker_lost",
        "aliases": ["CELERY_TASK_REJECT_ON_WORKER_LOST"],
        "expected": True,
        "critical_for": "all",
        "description": "Rejects and requeues task if child worker process is killed (e.g. OOM or SIGKILL)."
    },
    {
        "setting": "worker_prefetch_multiplier",
        "aliases": ["CELERY_WORKER_PREFETCH_MULTIPLIER"],
        "expected": 1,
        "critical_for": "heterogeneous",
        "description": "Prefetch=1 ensures workers only pull one task at a time, eliminating queue starvation on mixed workloads."
    },
    {
        "setting": "worker_max_tasks_per_child",
        "aliases": ["CELERY_WORKER_MAX_TASKS_PER_CHILD"],
        "expected": "POSITIVE_INT",
        "critical_for": "long-running",
        "description": "Recycles worker processes after N tasks to prevent unbounded Python memory leaks."
    },
    {
        "setting": "task_time_limit",
        "aliases": ["CELERY_TASK_TIME_LIMIT"],
        "expected": "POSITIVE_INT",
        "critical_for": "all",
        "description": "Hard execution limit (SIGKILL). Prevents rogue or deadlocked tasks from running forever."
    },
    {
        "setting": "task_soft_time_limit",
        "aliases": ["CELERY_TASK_SOFT_TIME_LIMIT"],
        "expected": "POSITIVE_INT",
        "critical_for": "all",
        "description": "Soft execution limit (SoftTimeLimitExceeded exception). Allows clean task cleanup before SIGKILL."
    },
    {
        "setting": "broker_connection_retry_on_startup",
        "aliases": ["CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP"],
        "expected": True,
        "critical_for": "all",
        "description": "Retries connecting to message broker on worker startup rather than crashing immediately."
    }
]

def extract_settings_from_python_file(filepath: str) -> Dict[str, Dict[str, Any]]:
    """
    Parses Python source with AST, distinguishing static literal values
    from dynamically computed expressions (e.g. os.environ.get, function calls).
    """
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        source = f.read()

    tree = ast.parse(source, filename=filepath)
    extracted = {}

    def parse_value(val_node: ast.AST) -> Tuple[Any, str]:
        try:
            return ast.literal_eval(val_node), "STATIC_LITERAL"
        except Exception:
            return None, "DYNAMICALLY_COMPUTED"

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    val, status = parse_value(node.value)
                    extracted[target.id] = {"value": val, "status": status}
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "update":
                for arg in node.args:
                    if isinstance(arg, ast.Dict):
                        for k, v in zip(arg.keys, arg.values):
                            if isinstance(k, ast.Constant):
                                val, status = parse_value(v)
                                extracted[k.value] = {"value": val, "status": status}
    return extracted

def lint_celery_config(settings: Dict[str, Dict[str, Any]], workload_profile: str = "heterogeneous") -> Dict[str, Any]:
    """
    Audits Celery configuration distinguishing:
      - EXPLICITLY_UNSAFE
      - MISSING_CONFIG
      - DEFAULTS_REQUIRING_VERIFICATION
      - DYNAMICALLY_COMPUTED
      - VERIFIED_SAFE
    """
    findings = []
    verified_safe = []
    dynamically_computed = []

    # 1. Check for autoscale hazard
    for k in ["worker_autoscale", "CELERY_WORKER_AUTOSCALE"]:
        if k in settings:
            item = settings[k]
            if item["status"] == "DYNAMICALLY_COMPUTED":
                dynamically_computed.append({
                    "setting": k,
                    "status": "DYNAMICALLY_COMPUTED",
                    "note": "Autoscale expression evaluated at runtime; verify it is not enabled for long-running jobs."
                })
            elif item["value"]:
                findings.append({
                    "setting": k,
                    "classification": "EXPLICITLY_UNSAFE",
                    "severity": "CRITICAL",
                    "current_value": item["value"],
                    "message": "Autoscale terminates busy workers mid-job during backlog recovery. Use fixed concurrency for non-trivial tasks."
                })

    # 2. Check reliability rules
    for rule in RECOMMENDED_RELIABILITY_RULES:
        found_key = None
        current_data = None
        for alias in [rule["setting"]] + rule["aliases"]:
            if alias in settings:
                found_key = alias
                current_data = settings[alias]
                break

        if found_key is None:
            findings.append({
                "setting": rule["setting"],
                "classification": "MISSING_CONFIG",
                "severity": "HIGH" if rule["critical_for"] in ("all", workload_profile) else "MEDIUM",
                "current_value": None,
                "message": f"Setting missing. {rule['description']} (Defaults to Celery baseline which requires verification)."
            })
            continue

        if current_data["status"] == "DYNAMICALLY_COMPUTED":
            dynamically_computed.append({
                "setting": found_key,
                "classification": "DYNAMICALLY_COMPUTED",
                "severity": "INFO",
                "note": f"Evaluated dynamically at runtime. Verify in deployment environment that it satisfies: {rule['expected']}."
            })
            continue

        val = current_data["value"]
        exp = rule["expected"]

        if exp == "POSITIVE_INT":
            if isinstance(val, int) and val > 0:
                verified_safe.append({"setting": found_key, "value": val})
            else:
                findings.append({
                    "setting": found_key,
                    "classification": "EXPLICITLY_UNSAFE",
                    "severity": "HIGH",
                    "current_value": val,
                    "message": f"Expected positive integer, got: {val}."
                })
        elif exp is True:
            if val is True:
                verified_safe.append({"setting": found_key, "value": val})
            else:
                findings.append({
                    "setting": found_key,
                    "classification": "EXPLICITLY_UNSAFE",
                    "severity": "CRITICAL" if rule["critical_for"] in ("all", workload_profile) else "MEDIUM",
                    "current_value": val,
                    "message": f"Value is {val}, expected True. {rule['description']}"
                })
        elif exp == 1:
            if val == 1:
                verified_safe.append({"setting": found_key, "value": val})
            else:
                if workload_profile == "heterogeneous":
                    findings.append({
                        "setting": found_key,
                        "classification": "DEFAULTS_REQUIRING_VERIFICATION",
                        "severity": "HIGH",
                        "current_value": val,
                        "message": f"Value is {val}. On heterogeneous workloads, prefetch > 1 causes idle workers while one worker hoards long tasks."
                    })
                else:
                    verified_safe.append({"setting": found_key, "value": val, "note": "Acceptable for homogeneous short tasks."})

    has_critical = any(f["severity"] == "CRITICAL" for f in findings)

    return {
        "workload_profile": workload_profile,
        "is_safe": not has_critical,
        "explicitly_unsafe": [f for f in findings if f["classification"] == "EXPLICITLY_UNSAFE"],
        "missing_config": [f for f in findings if f["classification"] == "MISSING_CONFIG"],
        "defaults_requiring_verification": [f for f in findings if f["classification"] == "DEFAULTS_REQUIRING_VERIFICATION"],
        "dynamically_computed": dynamically_computed,
        "verified_safe": verified_safe
    }

def simulate_retry_schedule(workers: int, attempts: int, base_delay: float = 2.0, max_delay: float = 60.0):
    fixed_times = []
    jittered_times = []

    for _ in range(workers):
        current_fixed = 0.0
        current_jittered = 0.0
        for att in range(1, attempts + 1):
            current_fixed += 5.0
            fixed_times.append(round(current_fixed, 1))

            cap = min(max_delay, base_delay * (2 ** (att - 1)))
            delay = random.uniform(0.1, cap)
            current_jittered += delay
            jittered_times.append(round(current_jittered, 1))

    return sorted(fixed_times), sorted(jittered_times)

def main():
    parser = argparse.ArgumentParser(description="Celery Reliability Doctor CLI")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # Subcommand: lint-config
    lint_parser = subparsers.add_parser("lint-config", help="Audit Celery configuration file for reliability anti-patterns")
    lint_parser.add_argument("--config", required=True, help="Path to Python settings/config file")
    lint_parser.add_argument("--workload-profile", choices=["heterogeneous", "homogeneous"], default="heterogeneous",
                             help="Workload type: heterogeneous (mixed long/short) or homogeneous (fast uniform)")
    lint_parser.add_argument("--json", action="store_true", help="Output pure JSON")

    # Subcommand: calc-backoff
    calc_parser = subparsers.add_parser("calc-backoff", help="Calculate retry backoff with full jitter")
    calc_parser.add_argument("--attempt", type=int, default=1, help="Retry attempt number")
    calc_parser.add_argument("--base-delay", type=float, default=2.0, help="Base delay in seconds")
    calc_parser.add_argument("--max-delay", type=float, default=300.0, help="Maximum delay ceiling")
    calc_parser.add_argument("--count", type=int, default=5, help="Number of simulated samples")
    calc_parser.add_argument("--json", action="store_true", help="Output pure JSON")

    # Subcommand: simulate-herd
    sim_parser = subparsers.add_parser("simulate-herd", help="Simulate thundering herd on concurrent retries")
    sim_parser.add_argument("--workers", type=int, default=50, help="Number of concurrent workers")
    sim_parser.add_argument("--attempts", type=int, default=3, help="Number of retries")
    sim_parser.add_argument("--json", action="store_true", help="Output pure JSON")

    args = parser.parse_args()

    if args.subcommand == "lint-config":
        if not os.path.isfile(args.config):
            print(f"Error: File not found: {args.config}", file=sys.stderr)
            sys.exit(1)
        settings = extract_settings_from_python_file(args.config)
        audit_res = lint_celery_config(settings, workload_profile=args.workload_profile)

        if args.json:
            print(json.dumps(audit_res, indent=2))
        else:
            print(f"=== Celery Reliability Audit: {args.config} (Profile: {args.workload_profile}) ===")
            if audit_res["explicitly_unsafe"]:
                print("\n[EXPLICITLY UNSAFE]:")
                for item in audit_res["explicitly_unsafe"]:
                    print(f"  ! {item['setting']}: {item['message']}")

            if audit_res["defaults_requiring_verification"]:
                print("\n[DEFAULTS REQUIRING VERIFICATION]:")
                for item in audit_res["defaults_requiring_verification"]:
                    print(f"  ? {item['setting']}: {item['message']}")

            if audit_res["missing_config"]:
                print("\n[MISSING CONFIGURATION]:")
                for item in audit_res["missing_config"]:
                    print(f"  - {item['setting']}: {item['message']}")

            if audit_res["dynamically_computed"]:
                print("\n[DYNAMICALLY COMPUTED (Verify at Runtime)]:")
                for item in audit_res["dynamically_computed"]:
                    print(f"  ~ {item['setting']}: {item['note']}")

            if audit_res["verified_safe"]:
                print(f"\n[VERIFIED SAFE ({len(audit_res['verified_safe'])} settings)]: " + ", ".join(s["setting"] for s in audit_res["verified_safe"]))

            status_str = "PASS" if audit_res["is_safe"] else "FAIL (Critical Hazards Present)"
            print(f"\nFinal Audit Verdict: {status_str}")

        sys.exit(0 if audit_res["is_safe"] else 1)

    elif args.subcommand == "calc-backoff":
        samples = []
        for _ in range(args.count):
            cap = min(args.max_delay, args.base_delay * (2 ** (args.attempt - 1)))
            val = round(random.uniform(0.1, cap), 2)
            samples.append(val)

        res = {
            "attempt": args.attempt,
            "base_delay": args.base_delay,
            "max_delay": args.max_delay,
            "calculated_ceiling": min(args.max_delay, args.base_delay * (2 ** (args.attempt - 1))),
            "jittered_samples_seconds": samples
        }
        if args.json:
            print(json.dumps(res, indent=2))
        else:
            print(f"Attempt: {args.attempt} | Ceiling: {res['calculated_ceiling']}s")
            print(f"Jittered delay samples: {', '.join(f'{s}s' for s in samples)}")

    elif args.subcommand == "simulate-herd":
        fixed, jittered = simulate_retry_schedule(args.workers, args.attempts)
        from collections import Counter
        fixed_buckets = Counter([int(t) for t in fixed])
        jittered_buckets = Counter([int(t) for t in jittered])

        max_fixed_spike = fixed_buckets.most_common(1)[0] if fixed_buckets else (0, 0)
        max_jitter_spike = jittered_buckets.most_common(1)[0] if jittered_buckets else (0, 0)

        res = {
            "workers": args.workers,
            "attempts": args.attempts,
            "fixed_retry_worst_spike": {"second": max_fixed_spike[0], "concurrent_requests": max_fixed_spike[1]},
            "jittered_retry_worst_spike": {"second": max_jitter_spike[0], "concurrent_requests": max_jitter_spike[1]}
        }

        if args.json:
            print(json.dumps(res, indent=2))
        else:
            print(f"Simulating {args.workers} concurrent tasks over {args.attempts} retries:")
            print(f"Fixed Interval Retry -> Peak Herd: {max_fixed_spike[1]} simultaneous requests at T={max_fixed_spike[0]}s (THUNDERING HERD)")
            print(f"Full Jitter Retry   -> Peak Herd: {max_jitter_spike[1]} simultaneous requests at T={max_jitter_spike[0]}s (SMOOTH DISTRIBUTION)")

if __name__ == "__main__":
    main()
