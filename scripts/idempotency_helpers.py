"""
Reusable Idempotency and Reliability Helpers for Distributed Celery Tasks.
Zero external required dependencies (supports optional redis-py if available, with in-memory fallback).
"""

import time
import random
import uuid
from typing import Optional, Callable, Any
from contextlib import contextmanager

class JitteredBackoff:
    """
    Computes exponential backoff with Full Jitter.
    Formula: t = random.uniform(0, min(max_delay, base_delay * (factor ** attempt)))
    """
    def __init__(self, base_delay: float = 2.0, max_delay: float = 300.0, factor: float = 2.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.factor = factor

    def calculate_delay(self, attempt: int) -> float:
        """Calculate sleep delay for a given attempt with full jitter."""
        if attempt < 1:
            attempt = 1
        calculated_cap = min(self.max_delay, self.base_delay * (self.factor ** (attempt - 1)))
        return random.uniform(0.1, calculated_cap)

    @staticmethod
    def calculate_quota_reset_delay(now_epoch: float, reset_epoch: float, min_buffer: float = 60.0) -> float:
        """
        Calculates seconds until a known daily quota resets (e.g. midnight UTC/Pacific),
        adding a small random buffer to stagger awakening workers.
        """
        remaining = reset_epoch - now_epoch
        if remaining <= 0:
            return min_buffer
        jitter_buffer = random.uniform(10.0, min_buffer)
        return remaining + jitter_buffer


class MemoryLockStorage:
    _locks = {}

    @classmethod
    def acquire(cls, key: str, token: str, ttl_seconds: float) -> bool:
        now = time.time()
        # Clean expired
        if key in cls._locks:
            existing_token, expiry = cls._locks[key]
            if now > expiry:
                del cls._locks[key]
        if key not in cls._locks:
            cls._locks[key] = (token, now + ttl_seconds)
            return True
        return False

    @classmethod
    def release(cls, key: str, token: str) -> bool:
        if key in cls._locks:
            existing_token, _ = cls._locks[key]
            if existing_token == token:
                del cls._locks[key]
                return True
        return False


class IdempotentTaskLock:
    """
    Distributed Task Lock protecting Celery tasks against concurrent or duplicate execution.
    Can be used as a context manager or decorator.
    """
    def __init__(self, key: str, ttl_seconds: int = 60, redis_client: Optional[Any] = None):
        self.key = f"celery:lock:{key}"
        self.ttl_seconds = ttl_seconds
        self.redis_client = redis_client
        self.token = str(uuid.uuid4())
        self.acquired = False

    def acquire(self) -> bool:
        if self.redis_client:
            # Redis SET key token NX EX ttl
            try:
                res = self.redis_client.set(self.key, self.token, nx=True, ex=self.ttl_seconds)
                self.acquired = bool(res)
                return self.acquired
            except Exception:
                # If Redis connection fails, fall back to memory
                pass
        self.acquired = MemoryLockStorage.acquire(self.key, self.token, self.ttl_seconds)
        return self.acquired

    def release(self) -> bool:
        if not self.acquired:
            return False
        if self.redis_client:
            # Lua script to release safely only if token matches
            lua_release = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """
            try:
                res = self.redis_client.eval(lua_release, 1, self.key, self.token)
                return bool(res)
            except Exception:
                pass
        return MemoryLockStorage.release(self.key, self.token)

    def __enter__(self):
        if not self.acquire():
            raise TaskLockAcquisitionError(f"Could not acquire lock for key: {self.key} (Task already in progress)")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class TaskLockAcquisitionError(Exception):
    """Raised when an idempotent task lock cannot be acquired."""
    pass
