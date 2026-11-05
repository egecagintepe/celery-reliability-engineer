# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-11-05

### Added
- Initial public release of `celery-reliability-engineer`.
- `celery_doctor.py` CLI for configuration linting, retry calculation, and thundering herd simulation.
- Zero-dependency `idempotency_helpers.py` providing `IdempotentTaskLock` and `JitteredBackoff`.
- Comprehensive reliability guides for late acknowledgment, prefetch multiplier, concurrency sizing, and broker visibility timeout.
- Reference implementation of production-hardened Celery configuration and resilient task structures.
- 100% passing automated test suite covering AST configuration parsing, distributed lock lifecycles, and jitter bounds.
- GitHub Actions CI workflow covering Python 3.8 through 3.12 across Ubuntu and Windows.
