# Contributing to Celery Reliability Engineer

We welcome contributions to help improve distributed queue reliability for developers and agents.

## Principles

1. **Lightweight & Portable**: Keep helper scripts portable with zero required third-party packages in core tests.
2. **Empirical & Hardened**: Recommendations and rules must reflect genuine distributed systems best practices.
3. **Comprehensive Tests**: Any change to lock behaviors, AST linter rules, or backoff calculations must include unit tests under `tests/`.

## Local Testing

```bash
python -m unittest discover -s tests
```

