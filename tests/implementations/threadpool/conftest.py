"""Pytest fixtures for the threading backend implementation tests.

All fixtures are re-exported from ``tests.fixtures.threadpool_backend``.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from tests.fixtures import threadpool_backend as _threadpool_backend

executor = _threadpool_backend.executor
limiter = _threadpool_backend.limiter
_reset_limiter_class_state = _threadpool_backend._reset_limiter_class_state
