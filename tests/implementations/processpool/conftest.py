"""Pytest fixtures for the process pool backend implementation tests.

All fixtures are re-exported from ``tests.fixtures.processpool_backend``.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from tests.fixtures import processpool_backend as _processpool_backend

executor = _processpool_backend.executor
limiter = _processpool_backend.limiter
_reset_limiter_class_state = _processpool_backend._reset_limiter_class_state
