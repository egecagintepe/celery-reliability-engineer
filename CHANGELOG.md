# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.3] - 2026-11-05

### Added
- **Developer Experience & FAQ**: Enhanced README with verified `skills.sh` discoverability badge, Problem → Solution → Result narrative, prominent install command, and developer FAQ covering durable idempotency, contested lock behavior, prefetch starvation, and static AST analysis.

## [1.0.2] - 2026-11-05

### Fixed
- **Safe Lock Contention Handling**: Fixed resilient task examples and documentation where lock contention previously returned `SKIPPED_CONCURRENT`, preventing silent acknowledgement and permanent loss of in-flight work. Tasks now check durable database state: if already completed, duplicate safe acknowledgement is returned; if still pending, the task is re-queued with decorrelated jittered backoff.
- **Distributed Lock Boundaries Documented**: Clarified operational limitations of distributed locks (concurrency coordination vs durable state machine checkpoints) and optional `redis-py` client dependency.
- **Regression Test Coverage**: Added dedicated regression test suite verifying lock contention retry, durable state validation, and retry exhaustion handling.

## [1.0.1] - 2026-11-05

### Security & Hardening
- **CRITICAL LOCK FIX**: Removed silent process-local memory fallback from distributed locking. `RedisDistributedLock` now strictly fails closed (`RedisLockConnectionError`) when the Redis backend is unreachable.
- **Explicit Local Testing Lock**: Introduced `LocalProcessLock`, explicitly restricted and named for single-process development and unit testing.
- **AST Configuration Linter Upgrade**: `celery_doctor.py` now categorizes findings into `EXPLICITLY_UNSAFE`, `MISSING_CONFIG`, `DEFAULTS_REQUIRING_VERIFICATION`, `DYNAMICALLY_COMPUTED`, and `VERIFIED_SAFE`.
- **Workload Profile Awareness**: Added `--workload-profile` supporting both `heterogeneous` (mixed fast/slow jobs requiring prefetch=1) and `homogeneous` (fast batch jobs).
- **Realistic Production Scenario Examples**: Added production-tested example handling worker crashes, duplicate broker delivery, rate-limit 429 backoff, and daily quota reset parking.
- **Official Agent Skills Compliance**: Validated frontmatter and directory layout against the official Agent Skills specification (`npx skills-ref validate`).
- **Comprehensive E2E Test Suite**: Expanded test coverage to 19 unit and integration tests including multi-threaded concurrency.

## [1.0.0] - 2026-11-05

### Added
- Preliminary release of `celery-reliability-engineer`.
