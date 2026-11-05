"""
Production Reference: Resilient Celery Background Processing.
Demonstrates:
  1. Distributed locking with fail-closed semantics (RedisDistributedLock)
  2. Database state-machine checkpointing (PENDING -> PROCESSING -> COMPLETED)
  3. Handling crash-after-commit before Celery ACK
  4. Distinguishing HTTP 429 rate limits from daily quota exhaustion
  5. Full-jitter exponential backoff
"""

import time
import logging
from typing import Dict, Any, Optional
from idempotency_helpers import (
    RedisDistributedLock,
    TaskLockAcquisitionError,
    RedisLockConnectionError,
    JitteredBackoff
)

logger = logging.getLogger("celery.tasks.resilient")
backoff = JitteredBackoff(base_delay=2.0, max_delay=300.0)

class SimulatedExternalAPIError(Exception):
    def __init__(self, message: str, status_code: int = 500, retry_after: Optional[int] = None, is_daily_quota: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after
        self.is_daily_quota = is_daily_quota

def resilient_sync_task(
    task_id: str,
    payload: Dict[str, Any],
    redis_client: Any,
    db_state: Dict[str, str],
    current_attempt: int = 1,
    max_attempts: int = 5
) -> Dict[str, Any]:
    """
    Executes an external synchronization with distributed locking and checkpointing.
    """
    lock_key = f"sync_task_{task_id}"

    # 1. Acquire distributed lease lock (fail closed)
    try:
        lock = RedisDistributedLock(key=lock_key, ttl_seconds=120, redis_client=redis_client)
        lock_context = lock
    except RedisLockConnectionError as e:
        # Broker/Redis failure: never execute uncoordinated writes blindly
        logger.error("Lock infrastructure unavailable for task %s. Re-queueing with backoff: %s", task_id, e)
        raise e

    try:
        with lock_context:
            logger.info("[Lock Acquired] Task %s entered execution boundary.", task_id)

            # 2. Database Checkpoint Verification: Check if already completed
            # In a real ORM: record = JobRecord.objects.select_for_update().get(id=task_id)
            current_status = db_state.get(task_id, "PENDING")
            if current_status == "COMPLETED":
                # Duplicate delivery caught after commit: return success safely without re-execution
                logger.info("[Duplicate Delivery Detected] Task %s was already completed in DB. Acknowledging message.", task_id)
                return {"status": "ALREADY_COMPLETED", "task_id": task_id}

            # Mark state as PROCESSING
            db_state[task_id] = "PROCESSING"

            # 3. Simulated API Call with Rate Limit Handling
            if payload.get("simulate_error") == "rate_limit_429":
                raise SimulatedExternalAPIError("Rate limit exceeded", status_code=429, retry_after=5)
            elif payload.get("simulate_error") == "daily_quota_429":
                raise SimulatedExternalAPIError("Daily quota exhausted", status_code=429, is_daily_quota=True)
            elif payload.get("simulate_error") == "transient_drop" and current_attempt < 3:
                raise ConnectionResetError("Connection dropped by remote server")

            # 4. Successful Processing & State Commit
            db_state[task_id] = "COMPLETED"
            logger.info("[Success] Task %s committed as COMPLETED.", task_id)
            return {"status": "SUCCESS", "task_id": task_id}

    except TaskLockAcquisitionError:
        # Another worker is actively executing this exact task right now
        logger.warning("[Concurrency Guard] Task %s is already in-progress by another worker. Dropping duplicate delivery.", task_id)
        return {"status": "LOCKED_IN_PROGRESS", "task_id": task_id}

    except SimulatedExternalAPIError as err:
        if err.is_daily_quota:
            # Daily quota exhaustion: Do NOT sleep 10s in useless loop.
            # Calculate sleep until midnight + random jitter buffer.
            now_epoch = time.time()
            # Simulate next day midnight epoch (86400s later)
            midnight_epoch = now_epoch + 14400  # 4 hours later
            park_delay = JitteredBackoff.calculate_quota_reset_delay(now_epoch, midnight_epoch)
            logger.error("[Daily Quota Exhausted] Parking task %s until reset epoch (delay: %.1fs).", task_id, park_delay)
            # In real Celery: raise self.retry(countdown=park_delay, exc=err)
            raise err
        else:
            # Short-term burst rate limit: use Retry-After header + small jitter
            delay = (err.retry_after or 5) + backoff.calculate_delay(current_attempt)
            logger.warning("[Rate Limit 429] Backing off task %s for %.2fs.", task_id, delay)
            raise err

    except ConnectionResetError as exc:
        if current_attempt >= max_attempts:
            logger.critical("[Budget Exhausted] Task %s reached maximum retries (%d). Marking FAILED.", task_id, max_attempts)
            db_state[task_id] = "FAILED"
            raise exc
        delay = backoff.calculate_delay(current_attempt)
        logger.warning("[Transient Error] Retrying task %s in %.2fs (Attempt %d/%d).", task_id, delay, current_attempt, max_attempts)
        raise exc
