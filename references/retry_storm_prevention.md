# Retry Storm Prevention & Rate Limit Handling

When downstream services or databases degrade, naive retry policies make the outage orders of magnitude worse.

---

## 1. The Thundering Herd Problem

Consider 50 worker processes querying an external API.
1. The API experiences a temporary blip and returns HTTP 500 or 429.
2. All 50 workers catch the error and execute `self.retry(countdown=10)`.
3. Ten seconds later, **all 50 workers strike the API at the exact same millisecond**.
4. The API crashes again under the 50-request synchronized spike.
5. Workers retry again in 10 seconds. The loop continues indefinitely.

---

## 2. Full Jitter Exponential Backoff

To break synchronization, combine exponential growth with uniform random distribution (**Full Jitter**):

$$\text{sleep} = \text{random.uniform}(0, \min(\text{max\_delay}, \text{base\_delay} \times 2^{\text{attempt}}))$$

### Implementation in Celery:
```python
from idempotency_helpers import JitteredBackoff

backoff_helper = JitteredBackoff(base_delay=2.0, max_delay=300.0)

@app.task(bind=True, max_retries=5)
def call_external_api(self, payload):
    try:
        return client.send(payload)
    except TransientNetworkError as exc:
        delay = backoff_helper.calculate_delay(self.request.retries + 1)
        logger.warning("Transient error on attempt %s. Retrying in %.2fs with jitter.", self.request.retries, delay)
        raise self.retry(exc=exc, countdown=delay)
```

---

## 3. Rate Limit (HTTP 429) vs Daily Quota Exhaustion

Many modern APIs (e.g. LLM providers, Google, GitHub, Stripe) return HTTP 429 for two completely different conditions:

1. **Short-Term Burst Rate Limit** (`RPM` or `TPM`):
   - Header: `Retry-After: 4`
   - Nature: Temporary traffic spike.
   - Action: Respect `Retry-After` header + small jitter (e.g. sleep 4s to 6s).
2. **Daily Quota Exhaustion** (`RPD` or Daily Tier Exhaustion):
   - Body: `RESOURCE_EXHAUSTED` / `GenerateRequestsPerDay quota exceeded`.
   - Nature: Permanent until midnight PST/UTC.
   - Hazard: If classified as a transient 429, workers wake up every 10–30 seconds all day long, burning CPU, polluting logs, and blocking other tasks.
   - Action: **Classify definitively as DAILY_EXHAUSTED**. Park the task until the reset timestamp (e.g. `midnight + jitter_seconds`), or fail the batch explicitly and notify operators.
