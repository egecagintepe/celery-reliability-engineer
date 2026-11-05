"""
Idempotency, Distributed Locks, and Backoff Helpers for Distributed Tasks.
Provides fail-closed Redis coordination, explicit local testing locks, and Full-Jitter backoff.
"""

import time
import random
import uuid
import threading
from typing import Optional, Any

# Atomic compare-and-delete Lua script for Redis lock release
LUA_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

class TaskLockError(Exception):
    """Base exception for task locking failures."""
    pass

class TaskLockAcquisitionError(TaskLockError):
    """Raised when a task lock cannot be acquired because another worker holds it."""
    pass

class RedisLockConnectionError(TaskLockError):
    """Raised when the distributed lock backend (Redis) is unreachable (fail-closed)."""
    pass

class RedisDistributedLock:
    """
    Fail-closed distributed lease lock using Redis.
    Guarantees:
      - Fail closed: If Redis is unavailable, raises RedisLockConnectionError.
      - Unique token ownership: Workers only release their own acquired lock.
      - Atomic acquisition: SET key token NX EX ttl.
      - Atomic release: Lua script compares token before DEL.

    IMPORTANT SAFETY LIMITATIONS:
      - Lock TTL Expiry: If a task executes longer than `ttl_seconds`, the lock expires in Redis.
        Another worker may then acquire the lock. For long tasks, use heartbeat renewal or
        pair with database-level unique constraints / versioned state machines.
      - Distributed locks provide CONCURRENCY CONTROL, not durable idempotency.
        Durable idempotency requires primary database transactional checkpoints.
    """
    def __init__(self, key: str, ttl_seconds: int = 60, redis_client: Optional[Any] = None):
        if not key or not isinstance(key, str):
            raise ValueError("Lock key must be a non-empty string.")
        if ttl_seconds <= 0:
            raise ValueError(f"Lock TTL must be positive, got {ttl_seconds}")
        if redis_client is None:
            raise ValueError("RedisDistributedLock requires a valid redis_client instance. For local testing, use LocalProcessLock.")

        self.key = f"celery:dist_lock:{key}"
        self.ttl_seconds = int(ttl_seconds)
        self.redis_client = redis_client
        self.token = str(uuid.uuid4())
        self.acquired = False

    def acquire(self) -> bool:
        """
        Attempts to acquire the distributed lock.
        Returns True if acquired, False if already held by another worker.
        Raises RedisLockConnectionError if Redis is unreachable (fail-closed).
        """
        try:
            res = self.redis_client.set(self.key, self.token, nx=True, ex=self.ttl_seconds)
            self.acquired = bool(res)
            return self.acquired
        except Exception as e:
            # Strictly fail closed: never fall back to local memory silently
            raise RedisLockConnectionError(f"Distributed lock backend failure for key '{self.key}': {e}") from e

    def release(self) -> bool:
        """
        Releases the distributed lock atomically if the token matches.
        Returns True if released, False if lock was lost, expired, or held by another token.
        Raises RedisLockConnectionError if Redis is unreachable.
        """
        if not self.acquired:
            return False

        try:
            res = self.redis_client.eval(LUA_RELEASE_SCRIPT, 1, self.key, self.token)
            self.acquired = False
            return bool(res)
        except Exception as e:
            raise RedisLockConnectionError(f"Failed to release distributed lock for '{self.key}': {e}") from e

    def __enter__(self):
        if not self.acquire():
            raise TaskLockAcquisitionError(f"Could not acquire distributed lock for '{self.key}' (Task in-flight by another worker).")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.release()
        except RedisLockConnectionError:
            # Suppress release connection errors on exit if original exception exists
            if exc_type is None:
                raise


class LocalProcessLock:
    """
    Explicitly process-local memory lock for UNIT TESTING and SINGLE-PROCESS DEVELOPMENT ONLY.
    WARNING: Does NOT coordinate across separate processes, containers, or distributed workers.
    """
    _locks = {}
    _mutex = threading.Lock()

    def __init__(self, key: str, ttl_seconds: int = 60):
        if ttl_seconds <= 0:
            raise ValueError("Lock TTL must be positive.")
        self.key = f"local:lock:{key}"
        self.ttl_seconds = ttl_seconds
        self.token = str(uuid.uuid4())
        self.acquired = False

    def acquire(self) -> bool:
        now = time.time()
        with self._mutex:
            if self.key in self._locks:
                existing_token, expiry = self._locks[self.key]
                if now > expiry:
                    del self._locks[self.key]
                else:
                    return False

            self._locks[self.key] = (self.token, now + self.ttl_seconds)
            self.acquired = True
            return True

    def release(self) -> bool:
        if not self.acquired:
            return False
        with self._mutex:
            if self.key in self._locks:
                existing_token, _ = self._locks[self.key]
                if existing_token == self.token:
                    del self._locks[self.key]
                    self.acquired = False
                    return True
        return False

    def __enter__(self):
        if not self.acquire():
            raise TaskLockAcquisitionError(f"Could not acquire local lock for '{self.key}'.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class JitteredBackoff:
    """
    Computes exponential backoff with Full Jitter.
    Formula: t = random.uniform(0, min(max_delay, base_delay * (factor ** (attempt - 1))))
    Decorrelates retry waves across workers to eliminate the thundering herd effect.
    """
    def __init__(self, base_delay: float = 2.0, max_delay: float = 300.0, factor: float = 2.0):
        self.base_delay = max(0.1, float(base_delay))
        self.max_delay = max(self.base_delay, float(max_delay))
        self.factor = max(1.1, float(factor))

    def calculate_delay(self, attempt: int) -> float:
        """Calculate sleep delay for a given attempt with full jitter."""
        if attempt < 1:
            attempt = 1
        calculated_cap = min(self.max_delay, self.base_delay * (self.factor ** (attempt - 1)))
        return round(random.uniform(0.1, calculated_cap), 2)

    @staticmethod
    def calculate_quota_reset_delay(now_epoch: float, reset_epoch: float, min_buffer: float = 60.0) -> float:
        """
        Calculates seconds until a daily quota resets, adding a randomized buffer
        to prevent a burst of workers waking up at the exact second of quota reset.
        """
        remaining = reset_epoch - now_epoch
        if remaining <= 0:
            return min_buffer
        jitter_buffer = random.uniform(5.0, max(10.0, min_buffer))
        return round(remaining + jitter_buffer, 2)
