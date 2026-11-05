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
from typing import Dict, List, Any

RECOMMENDED_RELIABILITY_RULES = [
    {
        "setting": "task_acks_late",
        "aliases": ["CELERY_TASK_ACKS_LATE", "CELERY_ACKS_LATE"],
        "expected": True,
        "severity": "CRITICAL",
        "description": "Enables late acknowledgment (ack after task completion). Prevents task loss if worker dies."
    },
    {
        "setting": "task_reject_on_worker_lost",
        "aliases": ["CELERY_TASK_REJECT_ON_WORKER_LOST"],
        "expected": True,
        "severity": "CRITICAL",
        "description": "Rejects and requeues task if child worker process is killed (e.g. OOM or SIGKILL)."
    },
    {
        "setting": "worker_prefetch_multiplier",
        "aliases": ["CELERY_WORKER_PREFETCH_MULTIPLIER"],
        "expected": 1,
        "severity": "HIGH",
        "description": "Prefetch=1 ensures workers only pull one task at a time, eliminating queue starvation."
    },
    {
        "setting": "worker_max_tasks_per_child",
        "aliases": ["CELERY_WORKER_MAX_TASKS_PER_CHILD"],
        "expected": "ANY_INT",
        "severity": "MEDIUM",
        "description": "Recycles worker processes after N tasks to prevent unbounded Python memory leaks."
    },
    {
        "setting": "task_time_limit",
        "aliases": ["CELERY_TASK_TIME_LIMIT"],
        "expected": "ANY_INT",
        "severity": "HIGH",
        "description": "Hard execution limit (SIGKILL). Prevents rogue or deadlocked tasks from running forever."
    },
    {
        "setting": "task_soft_time_limit",
        "aliases": ["CELERY_TASK_SOFT_TIME_LIMIT"],
        "expected": "ANY_INT",
        "severity": "MEDIUM",
        "description": "Soft execution limit (SoftTimeLimitExceeded exception). Allows clean teardown."
    },
    {
        "setting": "broker_connection_retry_on_startup",
        "aliases": ["CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP"],
        "expected": True,
        "severity": "MEDIUM",
        "description": "Retries connecting to message broker on worker startup rather than crashing immediately."
    }
]

def extract_settings_from_python_file(filepath: str) -> Dict[str, Any]:
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        source = f.read()

    tree = ast.parse(source, filename=filepath)
    extracted = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    var_name = target.id
                    try:
                        val = ast.literal_eval(node.value)
                        extracted[var_name] = val
                    except Exception:
                        pass
        # Support app.conf.update(...)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "update":
                for arg in node.args:
                    if isinstance(arg, ast.Dict):
                        for k, v in zip(arg.keys, arg.values):
                            if isinstance(k, ast.Constant):
                                try:
                                    val = ast.literal_eval(v)
                                    extracted[k.value] = val
                                except Exception:
                                    pass
    return extracted

def lint_celery_config(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    findings = []
    
    # Check for autoscale hazard
    for k in ["worker_autoscale", "CELERY_WORKER_AUTOSCALE"]:
        if k in settings and settings[k]:
            findings.append({
                "setting": k,
                "current_value": settings[k],
                "severity": "WARNING",
                "message": "Autoscale can terminate busy workers mid-job on long-running tasks. Prefer fixed concurrency."
            })

    for rule in RECOMMENDED_RELIABILITY_RULES:
        found_key = None
        current_val = None
        for alias in [rule["setting"]] + rule["aliases"]:
            if alias in settings:
                found_key = alias
                current_val = settings[alias]
                break

        if found_key is None:
            findings.append({
                "setting": rule["setting"],
                "current_value": None,
                "severity": rule["severity"],
                "message": f"Setting missing. {rule['description']}"
            })
        else:
            exp = rule["expected"]
            if exp == "ANY_INT":
                if not isinstance(current_val, int) or current_val <= 0:
                    findings.append({
                        "setting": found_key,
                        "current_value": current_val,
                        "severity": rule["severity"],
                        "message": f"Expected positive integer, got: {current_val}."
                    })
            elif current_val != exp:
                findings.append({
                    "setting": found_key,
                    "current_value": current_val,
                    "severity": rule["severity"],
                    "message": f"Value is {current_val}, expected {exp}. {rule['description']}"
                })

    return findings

def simulate_retry_schedule(workers: int, attempts: int, base_delay: float = 2.0, max_delay: float = 60.0):
    fixed_times = []
    jittered_times = []

    for _ in range(workers):
        current_fixed = 0.0
        current_jittered = 0.0
        for att in range(1, attempts + 1):
            # Fixed delay (e.g. 5s)
            current_fixed += 5.0
            fixed_times.append(round(current_fixed, 1))

            # Full jitter exponential
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
        findings = lint_celery_config(settings)
        critical_count = sum(1 for f in findings if f["severity"] == "CRITICAL")
        high_count = sum(1 for f in findings if f["severity"] == "HIGH")

        output = {
            "file": args.config,
            "total_findings": len(findings),
            "critical": critical_count,
            "high": high_count,
            "findings": findings
        }

        if args.json:
            print(json.dumps(output, indent=2))
        else:
            print(f"=== Celery Reliability Audit: {args.config} ===")
            if not findings:
                print("[PASS] Excellent! All critical reliability settings are properly configured.")
            else:
                for f in findings:
                    print(f"[{f['severity']}] {f['setting']}: {f['message']}")
            print(f"\nSummary: {critical_count} Critical, {high_count} High, {len(findings)} Total.")

        sys.exit(1 if critical_count > 0 else 0)

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
        # Find maximum collisions in any 1-second window
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
