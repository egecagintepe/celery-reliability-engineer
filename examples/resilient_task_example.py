"""
Example of a Production-Resilient Celery Task.
Demonstrates:
- Late acknowledgment (acks_late=True)
- Distributed lock for concurrency protection
- State checkpointing
- Full-jitter exponential retry
- Safe exception handling
"""

import logging
from idempotency_helpers import IdempotentTaskLock, TaskLockAcquisitionError, JitteredBackoff

logger = logging.getLogger(__name__)
backoff = JitteredBackoff(base_delay=2.0, max_delay=180.0)

# Simulated Task Function (compatible with Celery @app.task)
def process_report_generation(task_id: str, report_spec: dict, current_retries: int = 0):
    lock_key = f"report_gen_{task_id}"

    try:
        # 1. Distributed lock prevents duplicate workers running the same report
        with IdempotentTaskLock(key=lock_key, ttl_seconds=300):
            logger.info("Lock acquired for task %s. Starting processing.", task_id)

            # 2. Checkpoint: Mark in-progress in database
            # ReportModel.objects.filter(id=task_id).update(status="PROCESSING")

            # 3. Simulate processing work
            if report_spec.get("simulate_transient_failure") and current_retries < 2:
                raise ConnectionResetError("Transient network drop during generation")

            # 4. Success Checkpoint
            # ReportModel.objects.filter(id=task_id).update(status="COMPLETED")
            logger.info("Report %s completed successfully.", task_id)
            return {"status": "COMPLETED", "task_id": task_id}

    except TaskLockAcquisitionError:
        logger.warning("Task %s is already being processed by another worker. Dropping duplicate delivery.", task_id)
        return {"status": "SKIPPED_DUPLICATE"}

    except ConnectionResetError as exc:
        delay = backoff.calculate_delay(current_retries + 1)
        logger.warning("Transient error in task %s (Attempt %d). Backing off for %.2fs.", task_id, current_retries + 1, delay)
        # In real Celery: raise self.retry(exc=exc, countdown=delay, max_retries=5)
        raise exc
