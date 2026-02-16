"""Property-based tests for the is_subset helper function.

These tests employ Hypothesis to verify the mathematical properties of the
is_subset function with arbitrary nested dictionaries.
"""

from hypothesis import given
from hypothesis import strategies as st

from tests.helpers.strategies import nested_dict
from tests.helpers.utils import is_subset


class TestIsSubsetProperties:
    """Property-based tests verifying the mathematical invariants of is_subset."""

    @staticmethod
    @given(target=nested_dict)
    def test_reflexivity_dict_is_subset_of_itself(target):
        """Property: every dictionary is a subset of itself (reflexivity)."""
        assert is_subset(target, target), (
            f"dictionary should be a subset of itself: {target}"
        )

    @staticmethod
    @given(superset=nested_dict)
    def test_empty_dict_is_subset_of_any_dict(superset):
        """Property: the empty dictionary is a subset of any dictionary."""
        empty = {}
        assert is_subset(empty, superset), f"empty dict should be subset of: {superset}"

    @staticmethod
    @given(
        base=st.dictionaries(
            st.text(min_size=1, max_size=10),
            st.one_of(st.integers(), st.text(max_size=20)),
            min_size=1,
            max_size=10,
        ),
        extra_key=st.text(min_size=1, max_size=10),
        extra_value=st.one_of(st.integers(), st.text(max_size=20)),
    )
    def test_adding_keys_to_superset_preserves_subset_relation(
        base, extra_key, extra_value
    ):
        """Property: adding keys to the superset does not break the subset relationship."""
        from hypothesis import assume

        # Arrange
        # Ensure that extra_key is not already present in base to avoid overwriting
        assume(extra_key not in base)

        subset = base.copy()
        superset = base.copy()
        superset[extra_key] = extra_value

        # Act & Assert
        assert is_subset(subset, superset), (
            f"adding key to superset should preserve subset relation\n"
            f"subset: {subset}\n"
            f"superset: {superset}"
        )

    @staticmethod
    @given(
        key=st.text(min_size=1, max_size=10),
        value=st.one_of(st.integers(), st.text(max_size=20)),
    )
    def test_dict_with_extra_key_is_not_subset(key, value):
        """Property: a dictionary containing a key absent from the superset is not a subset."""
        # Arrange
        subset = {key: value, "extra": "value"}
        superset = {key: value}

        # Act & Assert
        assert not is_subset(subset, superset), (
            f"dict with extra keys should not be subset\n"
            f"subset: {subset}\n"
            f"superset: {superset}"
        )

    @staticmethod
    @given(
        key=st.text(min_size=1, max_size=10),
        value1=st.integers(),
        value2=st.integers(),
    )
    def test_dict_with_different_value_is_not_subset(key, value1, value2):
        """Property: if the values differ for the same key, the relation does not hold."""
        # Arrange
        # Ensure that the values are actually different
        if value1 == value2:
            value2 = value1 + 1

        subset = {key: value1}
        superset = {key: value2}

        # Act & Assert
        assert not is_subset(subset, superset), (
            f"dict with different value should not be subset\n"
            f"subset: {subset}\n"
            f"superset: {superset}"
        )

    @staticmethod
    @given(
        dict1=st.dictionaries(
            st.text(min_size=1, max_size=5),
            st.integers(),
            min_size=1,
            max_size=3,
        ),
        dict2=st.dictionaries(
            st.text(min_size=1, max_size=5),
            st.integers(),
            min_size=1,
            max_size=3,
        ),
        dict3=st.dictionaries(
            st.text(min_size=1, max_size=5),
            st.integers(),
            min_size=1,
            max_size=3,
        ),
    )
    def test_transitivity(dict1, dict2, dict3):
        """Property: if A ⊆ B and B ⊆ C, then A ⊆ C (transitivity)."""
        # Arrange
        # Build the nested relationship: dict1 ⊆ dict2 ⊆ dict3
        subset = dict1.copy()
        middle = {**dict1, **dict2}
        superset = {**dict1, **dict2, **dict3}

        # Act
        is_sub_mid = is_subset(subset, middle)
        is_mid_super = is_subset(middle, superset)

        # Assert
        # If both relations hold, then transitivity must hold
        if is_sub_mid and is_mid_super:
            assert is_subset(subset, superset), (
                f"transitivity violated\n"
                f"subset: {subset}\n"
                f"middle: {middle}\n"
                f"superset: {superset}"
            )
