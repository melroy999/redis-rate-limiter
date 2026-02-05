"""Shared test utilities and helper functions.

This module contains utility functions used across multiple test files.
"""

import math


def dict_equals_approx(left, right, relative_tolerance=1e-9, absolute_tolerance=1e-9):
    """Check if two values are approximately equal, handling nested structures.

    For floats, uses approximate equality with configurable tolerance.
    For nested dicts and lists, recursively compares elements.
    For other types, uses exact equality.

    Args:
        left: First value to compare.
        right: Second value to compare.
        relative_tolerance: Relative tolerance for float comparisons.
        absolute_tolerance: Absolute tolerance for float comparisons.

    Returns:
        True if values are approximately equal, False otherwise.
    """
    # Handle None.
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False

    # Handle different types.
    if type(left) != type(right):
        return False

    # Handle floats with approximate equality.
    if isinstance(left, float):
        return math.isclose(
            left, right, rel_tol=relative_tolerance, abs_tol=absolute_tolerance
        )

    # Handle dicts recursively.
    if isinstance(left, dict):
        if set(left.keys()) != set(right.keys()):
            return False
        return all(
            dict_equals_approx(
                left[key], right[key], relative_tolerance, absolute_tolerance
            )
            for key in left.keys()
        )

    # Handle lists recursively.
    if isinstance(left, list):
        if len(left) != len(right):
            return False
        return all(
            dict_equals_approx(
                left[i], right[i], relative_tolerance, absolute_tolerance
            )
            for i in range(len(left))
        )

    # For all other types (int, str, bool), use exact equality.
    return left == right


def is_subset(target: dict, superset: dict):
    """Check if the given target is a subset of the given superset.

    Args:
        target: The data that is considered the subset in the comparison.
        superset: The superset data.

    Returns:
        True if 'target' is a recursive subset of 'superset,' False otherwise.
    """
    for key, value in target.items():
        if key not in superset:
            return False
        if isinstance(value, dict):
            if not is_subset(value, superset.get(key, {})):
                return False
        elif value != superset[key]:
            return False
    return True
