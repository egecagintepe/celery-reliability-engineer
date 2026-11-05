# Celery Reliability Engineer

[![CI Status](https://github.com/egecagintepe/celery-reliability-engineer/actions/workflows/ci.yml/badge.svg)](https://github.com/egecagintepe/celery-reliability-engineer/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

Production-grade distributed task queue reliability toolkit, architectural guidelines, and diagnostic CLI for **Celery** and message brokers (**Redis**, RabbitMQ).

It provides automated configuration auditing, idempotency patterns, full-jitter exponential backoff calculators, thundering herd simulators, and battle-tested worker concurrency rules.

---

## Core Focus Areas

- **At-Least-Once Delivery & Idempotency**: Prevents duplicate execution bugs using state-machine transitions and distributed lease locks (`IdempotentTaskLock`).
- **Worker Crash Survivability**: Enforces late acknowledgment (`acks_late=True`) paired with `reject_on_worker_lost=True` to eliminate lost tasks when workers terminate unexpectedly.
- **Heterogeneous Workload Tuning**: Eliminates task starvation on mixed-duration workloads via `worker_prefetch_multiplier = 1`.
- **Autoscaling Hazard Prevention**: Explains why built-in Celery autoscale strands queued long-running tasks and provides fixed concurrency sizing formulas.
- **Thundering Herd Elimination**: Implements Full Jitter exponential backoff algorithms and differentiates transient HTTP 429 rate limits from daily quota resets.
- **Python Memory Leak Control**: Configures worker recycling (`worker_max_tasks_per_child`) to bound Python memory fragmentation over continuous operation.

---

## Installation & Setup

### 1. As an AI Agent Skill (Claude Code, Cursor, Antigravity)

Clone or link into your agent's skill directory:

```bash
git clone https://github.com/egecagintepe/celery-reliability-engineer.git .agents/skills/celery-reliability-engineer
```

Compatible agent environments will index `SKILL.md` to guide backend background task implementation and debugging.

### 2. Standalone Python Toolkit

```bash
git clone https://github.com/egecagintepe/celery-reliability-engineer.git
cd celery-reliability-engineer
```

---

## CLI Usage (`celery_doctor.py`)

### 1. Lint Celery Configuration
Audit any Celery settings file or Django settings module for 7 critical reliability rules:

```bash
python scripts/celery_doctor.py lint-config --config path/to/celery_conf.py
```

Sample output:
```
=== Celery Reliability Audit: celery_conf.py ===
[CRITICAL] task_acks_late: Setting missing. Enables late acknowledgment.
[HIGH] worker_prefetch_multiplier: Value is 4, expected 1. Prefetch=1 ensures workers only pull one task at a time.
```

### 2. Calculate Jittered Backoff Delay
Generate realistic retry delays with full jitter:

```bash
python scripts/celery_doctor.py calc-backoff --attempt 3 --base-delay 2.0 --max-delay 120.0
```

### 3. Simulate Thundering Herd
Demonstrate how fixed retries create massive request collisions compared to jittered backoff:

```bash
python scripts/celery_doctor.py simulate-herd --workers 50 --attempts 3
```

---

## Python Helpers (`scripts/idempotency_helpers.py`)

### Distributed Task Lock (Context Manager)
```python
from idempotency_helpers import IdempotentTaskLock, TaskLockAcquisitionError

@app.task(bind=True, acks_late=True)
def send_digest_email(self, user_id):
    try:
        with IdempotentTaskLock(key=f"digest_email_{user_id}", ttl_seconds=120):
            # Safe from concurrent duplicate runs
            deliver_email(user_id)
    except TaskLockAcquisitionError:
        logger.warning("Email task for user %s already running. Skipping.", user_id)
```

### Full-Jitter Backoff Calculator
```python
from idempotency_helpers import JitteredBackoff

backoff = JitteredBackoff(base_delay=2.0, max_delay=300.0)

@app.task(bind=True, max_retries=5)
def call_webhook(self, url, payload):
    try:
        post(url, json=payload)
    except TransientNetworkError as exc:
        delay = backoff.calculate_delay(self.request.retries + 1)
        raise self.retry(exc=exc, countdown=delay)
```

---

## Documentation

- [Idempotency Patterns & Distributed Locks](references/idempotency_patterns.md)
- [Worker Concurrency & Prefetch Tuning](references/concurrency_and_prefetch.md)
- [Retry Storms & API Quota Handling](references/retry_storm_prevention.md)
- [Broker Resilience & Connection Lifecycles](references/broker_resilience.md)
- [Recommended Configuration Reference](examples/celery_conf_recommended.py)

---

## Testing

Run unit tests:
```bash
python -m unittest discover -s tests
```

---

## License

This project is licensed under the [MIT License](LICENSE).
