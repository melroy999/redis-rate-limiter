"""Pytest fixtures for the Huey backend implementation tests.

All fixtures are re-exported from ``tests.fixtures.huey_backend``.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from tests.fixtures import huey_backend as _huey_backend

huey_instance = _huey_backend.huey_instance
limiter = _huey_backend.limiter
_reset_limiter_class_state = _huey_backend._reset_limiter_class_state
