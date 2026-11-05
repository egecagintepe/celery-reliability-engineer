---
name: celery-reliability-engineer
description: Distributed task queue reliability and architecture specialist for Celery and Redis. Prevents duplicate task execution, ghost worker loss, thundering herd retry storms (especially 429 quota exhaustion), prefetch starvation, autoscale traps, and uncheckpointed broker failures.
---

# Celery Reliability Engineer

A battle-tested skill for designing, auditing, and maintaining high-throughput, resilient background processing systems using **Celery** and message brokers like **Redis** or RabbitMQ.

It addresses common distributed systems pitfalls: at-least-once message delivery duplicates, worker crashes during execution, prefetch starvation on mixed-duration workloads, unbounded retries, and rate limit thundering herds.

---

## Core Invariants

1. **At-Least-Once Delivery Is Default**: Brokers guarantee delivery, not single execution. Every state-mutating task must either be naturally idempotent, protected by unique database constraints, or gated by a distributed lease lock.
2. **Late Acknowledgments (`acks_late=True`)**: Celery tasks should acknowledge messages *after* completion. If a worker dies mid-task (`SIGKILL` or OOM), the unacknowledged message returns to the queue. Always pair this with `reject_on_worker_lost=True`.
3. **Prefetch Multiplier Tuning**:
   - For **heterogeneous workloads** (mixed fast sub-second tasks and slow multi-minute batch jobs), setting `worker_prefetch_multiplier = 1` ensures idle workers can pick up queued work rather than letting one busy worker hoard jobs.
   - For **homogeneous ultra-fast workloads**, higher prefetch values (e.g. 4) allow broker batching efficiency.
4. **Avoid Dangerous Autoscaling on Long Jobs**: Celery's built-in `autoscale` evaluates queue length, not job duration. Under heavy load, it can terminate workers holding long-running state. Prefer fixed concurrency (`worker_concurrency`) calibrated to host memory and CPU cores.
5. **Fail-Closed Distributed Locking**: Never fall back to in-memory locks when a distributed broker fails. Distributed locks provide concurrency control, not durable idempotency. Durable idempotency requires primary database transactional checkpoints.
6. **Full-Jitter Exponential Backoff**: Never retry failed tasks on a fixed interval. Always inject randomized jitter to prevent the "thundering herd" effect where hundreds of failing tasks slam the database or an external API at the exact same millisecond.
7. **Daily Quota vs Rate Limit Differentiation**: When third-party APIs return HTTP 429, differentiate between short-term burst rate limits (`Retry-After: 5s`) and daily quota exhaustion (`Quota Exceeded: 20/day`). Sleeping 10 seconds on a daily exhausted quota burns workers in useless loops; such tasks should be parked until the true daily reset epoch.

---

## Authoritative Workflow

```
       ┌───────────────────────────────┐
       │ 1. Audit Task Configuration   │ (Check acks_late, limits, prefetch)
       └───────────────┬───────────────┘
                       │ Pass
                       ▼
       ┌───────────────────────────────┐
       │ 2. Verify Idempotency Gate    │ (Lease lock or DB constraint)
       └───────────────┬───────────────┘
                       │
                       ▼
       ┌───────────────────────────────┐
       │ 3. Checkpoint Initial State   │ (Durable DB record: PENDING)
       └───────────────┬───────────────┘
                       │
                       ▼
       ┌───────────────────────────────┐
       │ 4. Execute Within Try/Except  │
       └───────────────┬───────────────┘
                       │
         ┌─────────────┴─────────────┐
         │                           │
      Success                     Failure
         │                           │
         ▼                           ▼
┌──────────────────┐   ┌─────────────────────────────┐
│ 5. DB Commit     │   │ Jittered Retry or Park      │
│ & Acknowledge    │   │ (Backoff with full jitter)  │
└──────────────────┘   └─────────────────────────────┘
```

---

## Diagnostic CLI Commands

### 1. Lint Celery Configuration
Audit any Celery settings file or Django settings module for dangerous anti-patterns:
```bash
# Heterogeneous workload profile (default)
python scripts/celery_doctor.py lint-config --config path/to/celery_conf.py

# Homogeneous workload profile
python scripts/celery_doctor.py lint-config --config path/to/celery_conf.py --workload-profile homogeneous
```

### 2. Calculate Jittered Backoff
Compute retry schedules with full jitter:
```bash
python scripts/celery_doctor.py calc-backoff --attempt 3 --base-delay 5 --max-delay 300
```

### 3. Simulate Thundering Herd Elimination
Simulate and visualize how fixed retries create massive spike herds compared to full-jitter schedules:
```bash
python scripts/celery_doctor.py simulate-herd --workers 50 --attempts 4
```

---

## Detailed References

- [Idempotency Patterns & Distributed Locks](references/idempotency_patterns.md)
- [Worker Concurrency, Prefetch & Memory Tuning](references/concurrency_and_prefetch.md)
- [Retry Storms & API Quota Handling](references/retry_storm_prevention.md)
- [Broker Resilience & Connection Lifecycles](references/broker_resilience.md)
- [Production-Ready Task Example](examples/resilient_task_example.py)
