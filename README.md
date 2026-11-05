<div align="center">

# Celery Reliability Engineer

**Make background jobs resilient to crashes, retries, and duplicate delivery.**

[![CI](https://github.com/egecagintepe/celery-reliability-engineer/actions/workflows/ci.yml/badge.svg)](https://github.com/egecagintepe/celery-reliability-engineer/actions)
[![Release](https://img.shields.io/github/v/release/egecagintepe/celery-reliability-engineer?color=blue)](https://github.com/egecagintepe/celery-reliability-engineer/releases/latest)
[![skills.sh](https://img.shields.io/badge/skills.sh-celery--reliability--engineer-blue)](https://skills.sh/egecagintepe/celery-reliability-engineer)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Agent Skills: v1](https://img.shields.io/badge/Agent%20Skills-v1.0-emerald.svg)](https://agentskills.io/specification)

*Distributed task queue reliability toolkit, configuration linter, fail-closed locks, and full-jitter backoff for Celery and Redis.*

<br />

```bash
npx skills add egecagintepe/celery-reliability-engineer
```

<br />

```
   Task Enqueued ──► [ AT-LEAST-ONCE BROKER ] ──► Worker Crash?
                             │                         │
                             ▼                         ▼
                 [ REDIS DISTRIBUTED LOCK ]      acks_late=True
                 (Fail-Closed Coordination)     (Re-queued to live worker)
                             │                         │
                             ▼                         ▼
                 [ DB TRANSACTION COMMIT ]  ──► Duplicate Prevented!
```

</div>

---

## Problem → Solution → Result

- **The Problem**: Celery and Redis are the industry standard for Python asynchronous processing, but default setups fail catastrophically under load:
  1. **At-Least-Once Duplication**: Brokers guarantee delivery, not single execution. A worker crash after a database commit causes the task to re-execute, double-billing customers or spamming emails.
  2. **Silent Distributed Lock Degradation**: Unsafe lock libraries silently fall back to local in-memory dictionaries when Redis connections drop, letting workers in separate containers execute the same task concurrently.
  3. **Thundering Herd Retry Storms**: When downstream APIs return HTTP 429 or 500, hundreds of workers retrying on fixed 10-second intervals strike the API at the exact same millisecond, turning temporary glitches into sustained outages.
  4. **Prefetch Starvation**: Default `worker_prefetch_multiplier = 4` on mixed workloads (100ms jobs mixed with 15-minute reports) causes one worker to hoard long tasks while other workers sit starved and idle.

- **The Solution**: `celery-reliability-engineer` provides fail-closed distributed locking (`RedisDistributedLock`), full-jitter exponential backoff schedules (`JitteredBackoff`), and an AST-based configuration linter (`celery_doctor.py`) that audits Celery settings against production failure modes.

- **The Result**: Tasks fail closed instead of corrupting data during network blips, retries are decorrelated across random windows, workers share mixed queues evenly (`prefetch_multiplier = 1`), and concurrent tasks verify durable database state rather than dropping pending work.

---

## Quick Install (30 Seconds)

### Option 1: Using Agent Skills CLI (`skills.sh`)
```bash
npx skills add egecagintepe/celery-reliability-engineer
```

### Option 2: Direct Agent Integration (Claude Code, Cursor, Codex)
Clone into your project's local agent skills directory:
```bash
git clone https://github.com/egecagintepe/celery-reliability-engineer.git .agents/skills/celery-reliability-engineer
```

### Option 3: Standalone Python Toolkit (Zero Dependencies)
```bash
git clone https://github.com/egecagintepe/celery-reliability-engineer.git
cd celery-reliability-engineer
python scripts/celery_doctor.py --help
```

---

## Before & After

| Scenario | Naive Celery Setup | Celery Reliability Engineer |
|---|---|---|
| **Redis connection blips** | Silent memory fallback (separate workers double-process same job). | **Fail-Closed (`RedisDistributedLock`)**: Raises `RedisLockConnectionError`; never executes uncoordinated writes. |
| **Worker dies during task execution (`SIGKILL`)** | Early ack: Task is marked done; work is lost forever. | **Late Ack (`acks_late=True`, `reject_on_worker_lost=True`)**: Unacknowledged message automatically requeued. |
| **Downstream API returns HTTP 429** | Fixed 10s retry: All 50 workers strike API simultaneously (Thundering Herd). | **Full-Jitter Exponential Backoff**: Retries randomly decorrelated across time window. |
| **Daily API Quota exhausted** | Worker retries every 10s all day (burns CPU and locks queue). | **Quota Reset Calculation**: Differentiates short rate-limits from daily quota resets; parks task until reset epoch. |
| **Mixed fast & slow task queue** | Default prefetch=4: One worker pulls 8 long jobs; other workers sit idle. | **`prefetch_multiplier = 1`**: Ensures even task distribution across active worker pool. |

---

## Core Components

### 1. `RedisDistributedLock` (Fail-Closed Coordination)
Zero-dependency, production-grade distributed lock with Lua atomic compare-and-delete:

```python
from idempotency_helpers import RedisDistributedLock, TaskLockAcquisitionError, JitteredBackoff

backoff = JitteredBackoff(base_delay=2.0, max_delay=120.0)

@app.task(bind=True, acks_late=True, max_retries=5)
def generate_monthly_invoice(self, customer_id):
    try:
        # Fails closed if Redis is down (never silently falls back to local memory)
        with RedisDistributedLock(key=f"invoice_{customer_id}", ttl_seconds=120, redis_client=redis_conn):
            # Safe execution boundary
            process_invoice(customer_id)
    except TaskLockAcquisitionError as exc:
        # Check durable database state: has the invoice already completed?
        if is_invoice_already_completed(customer_id):
            return {"status": "ALREADY_COMPLETED"}
        # Work is still pending: DO NOT silently acknowledge and drop! Re-queue with jitter
        delay = backoff.calculate_delay(self.request.retries + 1)
        logger.warning("Invoice in-flight by another worker. Re-queueing with jitter (delay: %.2fs).", delay)
        raise self.retry(exc=exc, countdown=delay)
```

### 2. `JitteredBackoff` (Full-Jitter Exponential Retry)
Eliminate thundering herds by decorrelating retry schedules:

```python
from idempotency_helpers import JitteredBackoff

backoff = JitteredBackoff(base_delay=2.0, max_delay=300.0)

@app.task(bind=True, max_retries=5)
def call_partner_webhook(self, url, payload):
    try:
        send_webhook(url, payload)
    except TransientNetworkError as exc:
        delay = backoff.calculate_delay(self.request.retries + 1)
        raise self.retry(exc=exc, countdown=delay)
```

See the full production task scenario in [examples/resilient_task_example.py](examples/resilient_task_example.py) and baseline settings in [examples/celery_conf_recommended.py](examples/celery_conf_recommended.py).

---

## CLI Diagnostics (`celery_doctor.py`)

### 1. Audit Celery Configuration
Audits any settings file or Django settings module using AST analysis, distinguishing static values from dynamic runtime variables:

```bash
# Heterogeneous mixed workload (default)
python scripts/celery_doctor.py lint-config --config examples/celery_conf_recommended.py

# Homogeneous uniform fast workload
python scripts/celery_doctor.py lint-config --config examples/celery_conf_recommended.py --workload-profile homogeneous
```

Sample Output:
```text
=== Celery Reliability Audit: celery_conf.py (Profile: heterogeneous) ===

[EXPLICITLY UNSAFE]:
  ! worker_autoscale: Autoscale terminates busy workers mid-job during backlog recovery. Use fixed concurrency for non-trivial tasks.

[DEFAULTS REQUIRING VERIFICATION]:
  ? worker_prefetch_multiplier: Value is 4. On heterogeneous workloads, prefetch > 1 causes idle workers while one worker hoards long tasks.

[VERIFIED SAFE (5 settings)]: task_acks_late, task_reject_on_worker_lost, worker_max_tasks_per_child, task_time_limit, broker_connection_retry_on_startup

Final Audit Verdict: PASS
```

### 2. Calculate Jittered Backoff Schedules
```bash
python scripts/celery_doctor.py calc-backoff --attempt 3 --base-delay 2.0 --max-delay 120.0
```

### 3. Simulate Thundering Herd Collision Elimination
```bash
python scripts/celery_doctor.py simulate-herd --workers 50 --attempts 3
```

Output:
```text
Simulating 50 concurrent tasks over 3 retries:
Fixed Interval Retry -> Peak Herd: 50 simultaneous requests at T=5s (THUNDERING HERD)
Full Jitter Retry   -> Peak Herd: 3 simultaneous requests at T=4s (SMOOTH DISTRIBUTION)
```

---

## Frequently Asked Questions (FAQ)

### Does a distributed lock guarantee durable idempotency?
**No.** A distributed lock provides *concurrency coordination* (preventing two workers from processing the same entity simultaneously). Durable idempotency requires transactional database status checkpoints (`UPDATE orders SET status = 'COMPLETED' WHERE id = 101 AND status = 'PENDING'`). Always combine locks with transactional state machine checks.

### What happens when a lock is contested by another worker?
Our resilient pattern does not silently acknowledge and drop work. When `TaskLockAcquisitionError` occurs, the task inspects durable database state:
- If the job is already `COMPLETED`, it acknowledges the duplicate safely (`ALREADY_COMPLETED`).
- If the job is still `PENDING`, it raises `retry` with exponential jitter backoff so it can run if the competing worker crashes.

### Why is `worker_prefetch_multiplier = 1` critical for mixed workloads?
Celery defaults to `prefetch_multiplier = 4`. If a worker with 4 concurrency starts, it reserves `4 * 4 = 16` tasks from the broker. If those tasks include 15-minute reports, that worker sits holding tasks for hours while other workers in the cluster sit idle with nothing to do. Setting prefetch to 1 guarantees fair work distribution.

### Does `celery_doctor.py` require a running Redis or Celery cluster?
**No.** The linter performs static Abstract Syntax Tree (AST) analysis on your Python configuration files. It executes with zero external dependencies and zero live infrastructure requirements.

---

## Deep Documentation

- [Idempotency Patterns & Distributed Locks](references/idempotency_patterns.md) — State machines vs locks
- [Worker Concurrency & Prefetch Tuning](references/concurrency_and_prefetch.md) — Sizing worker pools safely
- [Retry Storms & API Quota Handling](references/retry_storm_prevention.md) — Rate limits vs daily resets
- [Broker Resilience & Connection Lifecycles](references/broker_resilience.md) — Visibility timeouts & keepalives
- [Recommended Configuration Reference](examples/celery_conf_recommended.py) — Production baseline settings
- [Resilient Task Scenario Example](examples/resilient_task_example.py) — End-to-end Python implementation

---

## Verified Compatibility

| Environment | Supported | Notes |
|---|---|---|
| **Python** | 3.8, 3.9, 3.10, 3.11, 3.12 | Standard library only; optional `redis-py` integration. |
| **Celery** | 4.x, 5.x | Config-agnostic AST analysis. |
| **Brokers** | Redis, RabbitMQ | Locking primitives specialized for Redis. |
| **Agent Skills** | Specification v1.0 | Validated with official `skills-ref validate`. |
| **AI Agents** | Claude Code, Cursor, Codex, Antigravity | Indexed via `SKILL.md`. |

---

## Safety Boundaries & Limitations

- **Locks Do Not Equal Durable Idempotency**: A distributed lock provides *concurrency control*. Durable idempotency requires transactional database checkpoints (`UPDATE ... WHERE status = 'PENDING'`).
- **Lock TTL Expiration**: If a task takes longer than its TTL, the lock will expire in Redis. For long-running batch jobs, pair with soft time limits or heartbeat renewal.

---

## Testing

Run the full automated test suite (22 tests):
```bash
python -m unittest discover -s tests
```

---

## License

This project is licensed under the [MIT License](LICENSE).
