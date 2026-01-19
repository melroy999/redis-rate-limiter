import os
from typing import Dict
from .limiters import CeleryRateLimiter


class RateLimiterRegistry:
    """
    A registry for rate limiter instances.
    """
    def __init__(self):
        self._limiters: Dict[str, CeleryRateLimiter] = {}

    def register(self, limiter: CeleryRateLimiter, override: bool = False, persist: bool = True) -> None:
        """
        Register a limiter instance by its limiter_id.
        :param limiter: The limiter to register--the name of the limiter will be used as the key.
        :param override: Whether to overwrite existing limiters.
        :param persist: Whether the rate limiter configuration should be stored in the redis database.
        :exception ValueError: If the limiter ID is already registered.
        """
        # Raise an exception if the operation will overwrite an existing limiter (unless force=True).
        if limiter in self._limiters and not override:
            raise ValueError(
                f"Limiter ID '{limiter.id}' is already registered. "
                "If this is intentional (e.g., in tests), use force=True."
            )

        # Register the limiter.
        self._limiters[limiter.id] = limiter

    def get(self, limiter_id: str) -> CeleryRateLimiter:
        """
        Get a limiter by its ID.
        :param limiter_id: The id of the limiter to get.
        :return: The limiter associated with the id.
        :exception ValueError: If the limiter ID doesn't exist.
        """
        if limiter_id not in self._limiters:
            raise ValueError(f"Limiter with ID '{limiter_id}' not found in registry. "
                             "Ensure it was registered at startup.")
        return self._limiters[limiter_id]

    def __contains__(self, limiter_id: str) -> bool:
        """
        Check if a limiter exists by its ID.
        :param limiter_id: The id of the limiter to check.
        :return: True if the limiter exists, False otherwise.
        """
        return limiter_id in self._limiters

    def list_all(self):
        """Returns all registered limiter IDs."""
        return list(self._limiters.keys())

    def clear(self):
        """Clear all registered limiters."""
        self._limiters.clear()


# The global registry of rate limiter instances.
_global_registry: RateLimiterRegistry = RateLimiterRegistry()


def register_limiter(limiter: CeleryRateLimiter, force: bool = False) -> None:
    """
    Register a limiter instance by its limiter_id in the global registry.
    :param limiter: The limiter to register--the name of the limiter will be used as the key.
    :param force: Whether to overwrite existing limiters.
    :exception ValueError: If the limiter ID is already registered.
    """
    _global_registry.register(limiter, force)

def get_limiter(limiter_id: str) -> CeleryRateLimiter:
    """
    Get a limiter by its ID from the global registry.
    :param limiter_id: The id of the limiter to get.
    :return: The limiter associated with the id.
    :exception ValueError: If the limiter ID doesn't exist.
    """
    return _global_registry.get(limiter_id)