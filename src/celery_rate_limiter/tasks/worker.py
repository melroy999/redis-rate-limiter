import importlib
from celery import shared_task

from celery_rate_limiter.decorators import rate_limited


def import_string(import_path: str):
    """Converts 'module.submodule.func' into the actual function object."""
    module_path, func_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)

@shared_task(name="celery_rate_limiter.generic_worker")
@rate_limited()
def generic_rate_limited_worker(limiter_id: str, func_path: str, payload: dict):
    """Executes a function by its import path."""
    # limiter_id needs to be included.
    _ = limiter_id

    # Execute the function.
    target_func = import_string(func_path)
    return target_func(**payload)