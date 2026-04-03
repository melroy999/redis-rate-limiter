"""Utility for loading Lua scripts from package resources."""

from __future__ import annotations

import logging
from importlib import resources

logger = logging.getLogger(__name__)

DEFAULT_RESOURCE_PACKAGES: tuple[str, ...] = (
    "redis_rate_limiter.lua",
    "src.redis_rate_limiter.lua",
)


def load_lua_script(
    script_name: str,
    resource_packages: tuple[str, ...] = DEFAULT_RESOURCE_PACKAGES,
) -> str:
    """Load a Lua script from package resources by filename.

    The function attempts each package in ``resource_packages`` in order and
    returns the script content from the first package that contains the file.

    Args:
        script_name: The filename of the Lua script to load (e.g., ``"consume.lua"``).
        resource_packages: An ordered tuple of importable package names to search.
            The first entry supports installed wheels; subsequent entries serve as
            fallbacks for source-tree imports.

    Returns:
        The Lua script source text.

    Raises:
        ImportError: If the script cannot be found in any of the specified packages.
    """
    errors: list[str] = []
    for resource_package in resource_packages:
        try:
            source = resources.files(resource_package).joinpath(script_name)
            content = source.read_text(encoding="utf-8")
            logger.debug(
                "[ScriptLoader] Lua script loaded: script=%s, package=%s.",
                script_name,
                resource_package,
            )
            return content
        except (ModuleNotFoundError, OSError) as error:
            errors.append(f"{resource_package}: {error}")

    raise ImportError(
        f"Could not load {script_name}; attempted packages: {', '.join(errors)}"
    )
