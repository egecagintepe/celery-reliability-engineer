# Idempotency Patterns & Distributed Task Locks

In distributed queue architectures like Celery, message brokers provide **at-least-once delivery guarantees**. Network partitions, worker timeouts, broker reconnects, and Celery visibility timeouts mean tasks **will** occasionally execute more than once.

This guide provides proven design patterns to ensure state safety regardless of how many times a message is redelivered.

---

## 1. Natural Idempotency (State-Machine Transitions)

The strongest idempotency pattern avoids blind `UPDATE` or `INSERT` operations and instead relies on strict state machine invariants.

### Anti-Pattern:
```python
# Dangerous: Running twice charges user twice
@app.task
def process_order(order_id):
    order = Order.objects.get(id=order_id)
    payment_gateway.charge(order.user, order.total)
    order.status = "PAID"
    order.save()
```

### Resilient Pattern:
```python
@app.task(bind=True, acks_late=True)
def process_order(self, order_id):
    # Atomic state check and lock
    with transaction.atomic():
        order = Order.objects.select_for_update().get(id=order_id)
        if order.status != OrderStatus.PENDING:
            logger.info("Order %s already processed (status=%s). Skipping.", order_id, order.status)
            return

        order.status = OrderStatus.PROCESSING
        order.save(update_fields=["status"])

    # Idempotent gateway charge using order ID as idempotency key
    charge_result = payment_gateway.charge(
        user=order.user,
        amount=order.total,
        idempotency_key=f"order_{order.id}"
    )

    with transaction.atomic():
        order.refresh_from_db()
        order.status = OrderStatus.PAID
        order.transaction_id = charge_result.id
        order.save(update_fields=["status", "transaction_id"])
```

---

## 2. Distributed Lease Locks (Redis Redlock Pattern)

When database rows cannot easily represent intermediate states, guard the task entry point with a distributed lease lock.

### Key Requirements:
1. **Time-To-Live (TTL)**: Always attach a TTL so that if the worker process is killed by `SIGKILL` or hardware failure, the lock automatically expires.
2. **Unique Token Release**: Workers must release the lock only if the token in Redis matches their own UUID. Otherwise, a slow worker could inadvertently release another worker's fresh lock.

```python
from idempotency_helpers import IdempotentTaskLock, TaskLockAcquisitionError

@app.task(bind=True, acks_late=True)
def synchronize_external_account(self, account_id):
    try:
        with IdempotentTaskLock(key=f"sync_account_{account_id}", ttl_seconds=120):
            # Heavy sync logic here
            do_external_sync(account_id)
    except TaskLockAcquisitionError:
        logger.warning("Synchronization for account %s already in flight.", account_id)
        return
```

---

## 3. Database Checkpointing Before Enqueueing

A common failure mode is enqueuing a task before the database transaction has committed:

```
Thread A: Start Tx -> Insert Record -> Enqueue Task -> Commit Tx
Worker B:                         Pulls Task -> Read Record -> DOES NOT EXIST (404)!
```

### The Rule:
Always enqueue tasks **after** database commit, e.g. using `transaction.on_commit(lambda: my_task.delay(id))` in Django, or persist an explicit `JobQueueRecord` in the DB first.
