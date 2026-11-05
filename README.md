<div align="center">

# Celery Reliability Engineer

**Make background jobs resilient to crashes, retries, and duplicate delivery.**

[![CI](https://github.com/egecagintepe/celery-reliability-engineer/actions/workflows/ci.yml/badge.svg)](https://github.com/egecagintepe/celery-reliability-engineer/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Agent Skills: v1](https://img.shields.io/badge/Agent%20Skills-v1.0-emerald.svg)](https://agentskills.io/specification)

*Distributed task queue reliability toolkit, configuration linter, fail-closed locks, and full-jitter backoff for Celery and Redis.*

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

## The Problem

Celery and Redis are the industry standard for Python background processing, but naive setups fail catastrophically under load:
1. **At-Least-Once Duplication**: Brokers guarantee delivery, not single execution. A worker crash after database commit re-executes the job, double-charging customers or sending duplicate emails.
2. **Silent Distributed Lock Degradation**: Unsafe lock implementations silently fall back to local in-memory dictionaries when Redis drops, allowing multiple workers across containers to run conflicting tasks simultaneously.
3. **Thundering Herd Retry Storms**: When a downstream API returns HTTP 429 or 500, hundreds of workers retrying on fixed intervals slam the API at the exact same millisecond, turning temporary hiccups into sustained outages.
4. **Worker Starvation via Prefetch**: On mixed workloads (100ms fast tasks mixed with 10-minute batch jobs), default `worker_prefetch_multiplier = 4` causes idle workers to sit starved while one worker hoards tasks.
5. **Autoscale Pitfalls**: Celery's built-in `autoscale` terminates workers based on queue length, inadvertently killing workers executing long-running batch jobs mid-task.

---

## Quick Install (30 Seconds)

### Using Agent Skills CLI (`skills.sh`)
```bash
npx skills add egecagintepe/celery-reliability-engineer
```

### Direct Agent Integration (Claude Code, Cursor, Codex)
```bash
git clone https://github.com/egecagintepe/celery-reliability-engineer.git .agents/skills/celery-reliability-engineer
```

### Standalone Python Toolkit
```bash
git clone https://github.com/egecagintepe/celery-reliability-engineer.git
cd celery-reliability-engineer
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
from idempotency_helpers import RedisDistributedLock, TaskLockAcquisitionError

@app.task(bind=True, acks_late=True)
def generate_monthly_invoice(self, customer_id):
    try:
        # Fails closed if Redis is down (never silently falls back to local memory)
        with RedisDistributedLock(key=f"invoice_{customer_id}", ttl_seconds=120, redis_client=redis_conn):
            # Safe execution boundary
            process_invoice(customer_id)
    except TaskLockAcquisitionError:
        logger.warning("Invoice generation already in-flight by another worker.")
        return {"status": "SKIPPED_CONCURRENT"}
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

## Deep Documentation

- [Idempotency Patterns & Distributed Locks](references/idempotency_patterns.md) — State machines vs locks
- [Worker Concurrency & Prefetch Tuning](references/concurrency_and_prefetch.md) — Sizing worker pools safely
- [Retry Storms & API Quota Handling](references/retry_storm_prevention.md) — Rate limits vs daily resets
- [Broker Resilience & Connection Lifecycles](references/broker_resilience.md) — Visibility timeouts & keepalives
- [Recommended Configuration Reference](examples/celery_conf_recommended.py) — Production baseline settings

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

Run the full automated test suite (19 tests):
```bash
python -m unittest discover -s tests
```

---

## License

This project is licensed under the [MIT License](LICENSE).
