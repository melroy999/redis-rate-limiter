"""Pytest fixtures for the RQ backend implementation tests.

All fixtures are re-exported from ``tests.fixtures.rq_backend``.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.

The entire module is skipped on Windows because the ``rq`` package calls
``multiprocessing.get_context("fork")`` at import time, which raises
``ValueError`` on platforms without fork support.
"""

import sys

import pytest

if sys.platform == "win32":
    pytest.skip(
        "RQ requires fork(), which is unavailable on Windows.",
        allow_module_level=True,
    )

from tests.fixtures import rq_backend as _rq_backend

rq_queue = _rq_backend.rq_queue
limiter = _rq_backend.limiter
_reset_limiter_class_state = _rq_backend._reset_limiter_class_state
