from typing import Dict
from .limiters import CeleryRateLimiter

# Global dictionary to store limiter instances
_LIMITER_REGISTRY: Dict[str, CeleryRateLimiter] = {}

def register_limiter(limiter: CeleryRateLimiter) -> None:
    """Register a limiter instance by its base_key."""
    _LIMITER_REGISTRY[limiter.base_key] = limiter

def get_limiter(limiter_id: str) -> CeleryRateLimiter:
    """Retrieve a limiter by its ID (base_key)."""
    if limiter_id not in _LIMITER_REGISTRY:
        raise ValueError(f"Limiter with ID '{limiter_id}' not found in registry. "
                         "Ensure it was registered at startup.")
    return _LIMITER_REGISTRY[limiter_id]