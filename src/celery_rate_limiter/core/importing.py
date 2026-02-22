import importlib
import logging
from typing import Any, Callable, cast

logger = logging.getLogger(__name__)


def import_string(import_path: str) -> Callable[..., Any]:
    """Resolve a dotted import path of the form 'module.submodule.callable' into the corresponding callable object."""
    module_path, func_name = import_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)
    if not callable(func):
        raise TypeError(f"Object at {import_path} is not callable.")

    logger.debug(
        "Dynamic import resolved for worker execution: import_path=%s, module=%s, callable=%s.",
        import_path,
        module_path,
        func_name,
    )

    # noinspection PyUnnecessaryCast
    # This cast is, in fact, necessary to satisfy mypy type validation.
    return cast(  # pragma: no mutate
        Callable[..., Any], func
    )
