"""Pytest fixtures for the Celery backend implementation tests.

All fixtures are re-exported from ``tests.fixtures.celery_backend``.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
"""

from tests.fixtures import celery_backend as _celery_backend

celery_app = _celery_backend.celery_app
celery_config = _celery_backend.celery_config
limiter = _celery_backend.limiter
_reset_limiter_class_state = _celery_backend._reset_limiter_class_state
