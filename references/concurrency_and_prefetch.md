# Worker Concurrency, Prefetch & Memory Tuning

Proper worker sizing and queue configuration are the difference between a responsive background pipeline and a hung cluster.

---

## 1. Why `CELERY_WORKER_PREFETCH_MULTIPLIER = 1` is Essential

By default, Celery configures `worker_prefetch_multiplier = 4`. With concurrency `N`, a worker pre-fetches `4 * N` tasks from the broker into its internal memory buffer.

### The Heterogeneous Workload Problem:
Suppose your queue contains two types of tasks:
- **Fast tasks**: 100ms
- **Heavy tasks**: 5 minutes

If Worker A pulls 4 heavy tasks and 12 fast tasks into its prefetch buffer:
- Worker A is completely locked running the first heavy task.
- The remaining 3 heavy tasks and 12 fast tasks **sit starved in Worker A's private memory buffer**.
- Meanwhile, Worker B finishes its jobs and sits completely idle, unable to process the tasks Worker A hoarded!

### The Solution:
```python
worker_prefetch_multiplier = 1
```
With prefetch set to 1, a worker only pulls one task per process slot. As soon as a worker slot becomes free, it pulls the next task from the broker. This guarantees optimal workload distribution.

---

## 2. The Danger of Celery Autoscaling

Celery includes an `--autoscale=MAX,MIN` parameter. While tempting, it introduces severe hazards for mixed or stateful tasks:

1. **Queue Length vs Execution Time**: Celery scales down based on queue throughput. If a backlog clears but several workers are executing long jobs, the autoscaler can terminate child processes prematurely.
2. **Resource Thrashing**: Spawning and tearing down Python processes causes high memory fragmentation and CPU spikes.

### Recommendation:
Use **fixed worker concurrency** (`--concurrency=N`). Calculate `N` based on workload:
- **I/O-Bound Workloads** (HTTP requests, external APIs): 2x - 4x CPU cores (or gevent/eventlet pools).
- **CPU/Memory-Bound Workloads** (image processing, data science, document generation): 1x CPU cores, bounded strictly by available RAM.

---

## 3. Mitigating Python Memory Leaks

Python's garbage collector and memory allocators (glibc malloc) often suffer from fragmentation when processing large strings, images, or JSON trees over days of operation.

```python
worker_max_tasks_per_child = 500
worker_max_memory_per_child = 500000  # 500 MB (Celery 4.0+)
```
Configuring `max_tasks_per_child` instructs Celery to gracefully retire and respawn child worker processes after processing N tasks, releasing leaked memory back to the operating system without dropping queued jobs.
