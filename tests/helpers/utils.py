"""Shared test utilities and helper functions.

This module provides utility functions that are used across multiple test files.
"""

import math


def dict_equals_approx(left, right, relative_tolerance=1e-9, absolute_tolerance=1e-9):
    """Determine whether two values are approximately equal, with support for nested structures.

    For float values, approximate equality is evaluated using a configurable tolerance.
    For nested dictionaries and lists, the comparison is performed recursively.
    For all other types, exact equality is used.

    Args:
        left: The first value to compare.
        right: The second value to compare.
        relative_tolerance: The relative tolerance applied to float comparisons.
        absolute_tolerance: The absolute tolerance applied to float comparisons.

    Returns:
        True if the values are approximately equal, False otherwise.
    """
    # Handle the case where both values are None.
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False

    # Handle the case where the types differ.
    if type(left) is not type(right):
        return False

    # Handle float values with approximate equality.
    if isinstance(left, float):
        return math.isclose(
            left, right, rel_tol=relative_tolerance, abs_tol=absolute_tolerance
        )

    # Handle dictionaries recursively.
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

    # For all remaining types (i.e., int, str, bool), exact equality is used.
    return left == right


def is_subset(target: dict, superset: dict):
    """Determine whether the given target dictionary is a recursive subset of the given superset.

    Args:
        target: The dictionary that is considered the subset in the comparison.
        superset: The dictionary that is considered the superset in the comparison.

    Returns:
        True if ``target`` is a recursive subset of ``superset``, False otherwise.
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
