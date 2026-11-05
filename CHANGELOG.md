# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
