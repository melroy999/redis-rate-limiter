import importlib

import redis
from celery import shared_task, Celery

from src import CeleryRateLimiterFactory


# Create the redis and celery instances here so everyone can use the same configuration.
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
celery_app = Celery('rate_limiter_demo', broker='redis://localhost:6379/0')

# The rate limiter factory.
factory = CeleryRateLimiterFactory(redis_client, celery_app)

# Create a test limiter.
limiter = factory.create_limiter(
    limiter_id="test_api",
    limit=50,
    window=10,
    max_concurrency=10
)


def import_string(import_path: str):
    """Converts 'module.submodule.func' into the actual function object."""
    module_path, func_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)

@shared_task(name="rate_limiter.generic_worker")
def generic_rate_limited_worker(limiter_id: str, func_path: str, payload: dict, task_id: str = None):
    """Executes a function by its import path."""
    # limiter = get_limiter(limiter_id)

    # Use the context manager to ensure concurrency is released and the next drain is triggered.
    with limiter.task_lifecycle(task_id=task_id):
        target_func = import_string(func_path)
        return target_func(**payload)