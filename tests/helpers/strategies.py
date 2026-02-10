"""Shared Hypothesis strategies for property-based testing.

This module contains reusable Hypothesis strategies used across multiple
property-based test files.
"""

from hypothesis import strategies as st

# JSON-compatible strategy: recursive structure of primitives.
# This generates arbitrary JSON-serializable Python objects.
json_value = st.recursive(
    base=st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-(2**31), max_value=2**31 - 1),
        st.floats(
            allow_nan=False,
            allow_infinity=False,
        ),
        st.text(
            alphabet=st.characters(
                blacklist_categories=["Cs"],
                blacklist_characters="\x00",
            ),
            max_size=100,
        ),
    ),
    extend=lambda children: st.one_of(
        st.lists(children, max_size=10),
        st.dictionaries(
            st.text(
                alphabet=st.characters(
                    blacklist_categories=["Cs"],
                    blacklist_characters="\x00",
                ),
                min_size=1,
                max_size=50,
            ),
            children,
            max_size=10,
        ),
    ),
    max_leaves=20,
)

# Strategy for generating only dictionaries from JSON_value.
nested_dict = json_value.filter(lambda value: isinstance(value, dict))
