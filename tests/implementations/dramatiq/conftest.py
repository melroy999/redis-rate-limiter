"""Pytest fixtures for the Dramatiq backend implementation tests.

All fixtures are re-exported from ``tests.fixtures.dramatiq_backend``.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from tests.fixtures import dramatiq_backend as _dramatiq_backend

dramatiq_broker = _dramatiq_backend.dramatiq_broker
limiter = _dramatiq_backend.limiter
_reset_limiter_class_state = _dramatiq_backend._reset_limiter_class_state
