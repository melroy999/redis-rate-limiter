"""Property-based tests for the ASGI key extraction functions.

These tests employ Hypothesis to verify the invariants of ``by_client_ip`` and
``by_header`` with arbitrary inputs drawn from the valid ASGI domain.
"""

from hypothesis import given
from hypothesis import strategies as st

from celery_rate_limiter.backends.asgi.keys import by_client_ip, by_header

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# IPv4 addresses as dotted-quad strings.
ipv4_addresses = st.tuples(
    st.integers(0, 255),
    st.integers(0, 255),
    st.integers(0, 255),
    st.integers(0, 255),
).map(lambda octets: ".".join(str(o) for o in octets))

# Valid TCP/UDP port numbers.
ports = st.integers(min_value=0, max_value=65535)

# Latin-1 encodable text (codepoints 0x01..0xFF), excluding NUL.
# HTTP header names and values must be encodable as Latin-1 in ASGI.
latin1_text = st.text(
    alphabet=st.characters(min_codepoint=1, max_codepoint=255),
    min_size=1,
    max_size=100,
)

# HTTP header names: lowercase ASCII tokens (RFC 7230 defines a header name
# as a token of visible ASCII characters excluding delimiters).
header_names = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789-_"),
    min_size=1,
    max_size=50,
)


class TestByClientIpProperties:
    """Property-based tests verifying the invariants of ``by_client_ip``."""

    @staticmethod
    @given(ip=ipv4_addresses, port=ports)
    def test_identity_preservation(ip, port):
        """Property: the extracted IP equals the string representation of the client address."""
        # Arrange
        scope = {"client": (ip, port)}

        # Act
        result = by_client_ip(scope)

        # Assert
        assert result == ip, (
            f"extracted IP should equal input: got {result!r}, expected {ip!r}"
        )

    @staticmethod
    @given(ip=latin1_text, port=ports)
    def test_non_ip_client_values_are_returned_as_strings(ip, port):
        """Property: any client address value is returned as its string representation."""
        # Arrange
        scope = {"client": (ip, port)}

        # Act
        result = by_client_ip(scope)

        # Assert
        assert result == str(ip), (
            f"result should be the string form of the client address: "
            f"got {result!r}, expected {str(ip)!r}"
        )


class TestByHeaderProperties:
    """Property-based tests verifying the invariants of ``by_header``."""

    @staticmethod
    @given(name=header_names, value=latin1_text)
    def test_round_trip_preservation(name, value):
        """Property: a header value present in the scope is returned unchanged."""
        # Arrange
        key_func = by_header(name)
        encoded_name = name.lower().encode("latin-1")
        encoded_value = value.encode("latin-1")
        scope = {"headers": [(encoded_name, encoded_value)]}

        # Act
        result = key_func(scope)

        # Assert
        assert result == value, (
            f"header value should survive round-trip: got {result!r}, expected {value!r}"
        )

    @staticmethod
    @given(name=header_names, value=latin1_text)
    def test_case_insensitive_name_matching(name, value):
        """Property: header name matching is case-insensitive regardless of input casing."""
        # Arrange
        key_func = by_header(name.upper())
        encoded_name = name.lower().encode("latin-1")
        encoded_value = value.encode("latin-1")
        scope = {"headers": [(encoded_name, encoded_value)]}

        # Act
        result = key_func(scope)

        # Assert
        assert result == value, (
            f"case-insensitive match should succeed: "
            f"configured {name.upper()!r}, actual header {name.lower()!r}"
        )

    @staticmethod
    @given(name=header_names, other_name=header_names, value=latin1_text)
    def test_absent_header_returns_none(name, other_name, value):
        """Property: when the target header is absent, ``None`` is returned."""
        from hypothesis import assume

        # Arrange
        # Ensure the header names are distinct after case folding.
        assume(name.lower() != other_name.lower())

        key_func = by_header(name)
        encoded_other = other_name.lower().encode("latin-1")
        encoded_value = value.encode("latin-1")
        scope = {"headers": [(encoded_other, encoded_value)]}

        # Act
        result = key_func(scope)

        # Assert
        assert result is None, (
            f"absent header should yield None: "
            f"looking for {name!r}, scope has {other_name!r}"
        )
