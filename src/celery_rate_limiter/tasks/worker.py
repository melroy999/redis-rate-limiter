import importlib
from typing import Any, Callable, cast

from celery import shared_task

from celery_rate_limiter.decorators import rate_limited


def import_string(import_path: str) -> Callable[..., Any]:
    """Convert 'module.submodule.func' into the actual function object.

    Args:
        import_path: The dot-separated path to the function.

    Returns:
        The callable function object.

    Raises:
        TypeError: If the object at the path is not callable.
    """
    module_path, func_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    if not callable(func):
        raise TypeError(f"Object at {import_path} is not callable.")

    # noinspection PyUnnecessaryCast
    # This cast is in fact necessary for mypy validation.
    return cast(Callable[..., Any], func)


@shared_task(name="celery_rate_limiter.generic_worker")
@rate_limited()
def generic_rate_limited_worker(limiter_id: str, func_path: str, payload: dict) -> Any:
    """Execute a function by its import path.

    Args:
        limiter_id: The id of the rate limiter instance.
        func_path: The dot-separated path to the function to execute.
        payload: The arguments to pass to the function.

    Returns:
        The result of the function execution.
    """
    # limiter_id needs to be included.
    _ = limiter_id

    # Execute the function.
    target_func = import_string(func_path)
    return target_func(**payload)
