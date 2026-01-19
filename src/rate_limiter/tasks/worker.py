import importlib
from celery import shared_task

from config import factory


def import_string(import_path: str):
    """Converts 'module.submodule.func' into the actual function object."""
    module_path, func_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)

@shared_task(name="rate_limiter.generic_worker")
def generic_rate_limited_worker(limiter_id: str, func_path: str, payload: dict, task_id: str = None):
    """Executes a function by its import path."""
    limiter = factory.registry.get(limiter_id)

    # Use the context manager to ensure concurrency is released and the next drain is triggered.
    with limiter.task_lifecycle(task_id=task_id):
        target_func = import_string(func_path)
        return target_func(**payload)