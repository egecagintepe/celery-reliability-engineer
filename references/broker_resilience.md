# Broker Resilience & Connection Lifecycles

The message broker (Redis or RabbitMQ) is the communication spine of Celery. If broker connections drop or time out, unhandled exceptions can cascade across the cluster.

---

## 1. Redis Visibility Timeout (`visibility_timeout`)

When using Redis as the Celery broker, tasks claimed by a worker are moved to an unacknowledged queue. If the worker does not acknowledge the task within `visibility_timeout` seconds, Redis assumes the worker crashed and **redelivers the task to another worker**.

### The Hazard:
If you have a task that takes 2 hours (e.g. video encoding or large dataset migration) but your `visibility_timeout` is the default 3600 seconds (1 hour):
- At minute 60, Redis redelivers the task to Worker B.
- Worker A is still running the task!
- Now Worker A and Worker B are running the same task in parallel, leading to race conditions and corrupted data.

### The Rule:
`visibility_timeout` must exceed the duration of your longest task:
```python
broker_transport_options = {
    'visibility_timeout': 43200,  # 12 hours
}
```

---

## 2. Broker Startup Resilience

In containerized deployments, Celery workers often boot up faster than the Redis or RabbitMQ container. Without startup retries, workers crash immediately (`OperationalError: Error 111 connecting to localhost:6379`).

```python
broker_connection_retry_on_startup = True
broker_connection_max_retries = 30
```

---

## 3. Worker Heartbeats & Health Checks

To detect disconnected brokers without waiting for TCP timeout:
```python
broker_heartbeat = 10
broker_pool_limit = 10
```

For container health checks, avoid heavyweight `celery inspect ping` on busy workers (which can time out under high CPU load). Instead, inspect process existence and Redis responsiveness via `redis-cli ping`.
