# Idempotency Patterns & Distributed Task Locks

In distributed queue architectures like Celery, message brokers provide **at-least-once delivery guarantees**. Network blips, worker timeouts, broker reconnects, and Celery visibility timeouts guarantee that tasks **will** occasionally execute more than once.

This guide provides proven patterns to ensure state safety regardless of delivery duplicates.

---

## 1. Concurrency Control vs. Durable Idempotency

A common misconception is that a distributed lock (e.g. Redis SETNX) guarantees durable idempotency on its own. It does not:
- **Distributed Locks** provide *concurrency control* (preventing two workers from processing the same entity simultaneously).
- **Primary Database Constraints & State Machines** provide *durable idempotency* (preventing duplicate effects if a worker crashes after database commit but before message acknowledgment).

A robust architecture combines both.

---

## 2. Distributed Lease Locks (`RedisDistributedLock`)

When coordinating tasks across multiple containers or machines, use fail-closed distributed locks:

### Key Requirements:
1. **Fail Closed**: If the lock backend (Redis) is unreachable, the task must **not** execute uncoordinated writes. Never fall back to local in-memory locks in a distributed environment!
2. **Unique Ownership Token**: Always generate a unique UUID per lock acquisition and release via an atomic Lua script:
   ```lua
   if redis.call("get", KEYS[1]) == ARGV[1] then
       return redis.call("del", KEYS[1])
   else
       return 0
   end
   ```
3. **Lock Expiry Limitation**: If a job runs longer than its `ttl_seconds`, the lock expires. Another worker can acquire it while the first is still working. To mitigate this:
   - Size TTL comfortably above maximum task runtime.
   - Use soft execution time limits (`task_soft_time_limit`).
   - Guard database writes with optimistic locking / version checks (`UPDATE ... WHERE version = 1`).
4. **Lock Contention Strategy**:
   - If a lock cannot be acquired because another worker holds it, **never silently acknowledge and drop the message**. Silently dropping concurrent messages causes permanent data loss if the worker holding the lock crashes, or if the tasks carried distinct payloads.
   - Always verify durable database state: if state is already `COMPLETED`, it is safe to acknowledge; if state is `PENDING`, re-raise with jittered backoff (`self.retry(...)`).

---

## 3. Database State-Machine Transitions

The primary defense against duplicate execution is atomic state transitions:

```python
from django.db import transaction

@app.task(bind=True, acks_late=True)
def process_order(self, order_id):
    # 1. Atomic state verification
    with transaction.atomic():
        order = Order.objects.select_for_update().get(id=order_id)
        if order.status == OrderStatus.COMPLETED:
            logger.info("Order %s already completed. Acknowledging duplicate.", order_id)
            return
        if order.status != OrderStatus.PENDING:
            raise ValueError(f"Invalid state transition for order {order_id}: {order.status}")

        order.status = OrderStatus.PROCESSING
        order.save(update_fields=["status"])

    # 2. External gateway charge with idempotency key
    charge = payment_gateway.charge(
        amount=order.total,
        idempotency_key=f"order_charge_{order.id}"
    )

    # 3. Commit completion
    with transaction.atomic():
        order.refresh_from_db()
        order.status = OrderStatus.COMPLETED
        order.charge_id = charge.id
        order.save(update_fields=["status", "charge_id"])
```

If the worker crashes immediately after step 3, Celery will redeliver the message. Upon redelivery, step 1 detects `order.status == OrderStatus.COMPLETED` and safely exits without double-charging the customer.
