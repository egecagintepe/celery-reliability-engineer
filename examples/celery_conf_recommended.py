"""
Production-Hardened Celery Configuration Reference.
Suitable for Django (settings.py) or standalone Celery configurations.
"""

# Message Acknowledgment & Worker Survival
task_acks_late = True
task_reject_on_worker_lost = True

# Workload Distribution & Prefetch Tuning
worker_prefetch_multiplier = 1
worker_concurrency = 4  # Tune based on CPU/RAM, avoid autoscaling for long jobs

# Worker Memory Leak Mitigation
worker_max_tasks_per_child = 500
worker_max_memory_per_child = 500000  # 500MB

# Time Limits (Hard & Soft)
task_time_limit = 1800       # 30 minutes hard timeout (SIGKILL)
task_soft_time_limit = 1500  # 25 minutes soft timeout (SoftTimeLimitExceeded)

# Broker Resilience (Redis Example)
broker_url = "redis://localhost:6379/0"
result_backend = "redis://localhost:6379/1"
broker_connection_retry_on_startup = True
broker_connection_max_retries = 30
broker_heartbeat = 10

# Visibility Timeout (must exceed duration of longest task)
broker_transport_options = {
    "visibility_timeout": 7200,  # 2 hours
}

# Task Result Expiration
result_expires = 86400  # 24 hours
task_ignore_result = False

