"""Tests for the Cloudflare Access defensive gate in the portal.

The gate is a pure decision function `_is_cf_access_allowed(headers, require)`
plus a middleware that calls it. Threat model: prevent exposure on
misconfiguration (Access policy disabled / LAN access bypassing Cloudflare),
NOT to resist header forgery. v1 does not verify the JWT signature.
"""
from newsprism.runtime.portal.app import _is_cf_access_allowed


def _hdr(value=None):
    """Build a header mapping like Starlette's request.headers."""
    return {"cf-access-jwt-assertion": value} if value else {}


class TestFlagOff:
    def test_flag_false_admits_without_header(self):
        assert _is_cf_access_allowed({}, require=False) is True

    def test_flag_false_ignores_any_header_state(self):
        assert _is_cf_access_allowed(_hdr("garbage"), require=False) is True


class TestFlagOn:
    def test_missing_header_blocked(self):
        assert _is_cf_access_allowed({}, require=True) is False

    def test_valid_three_segment_header_admitted(self):
        token = "eyJhbG.eyJzdWI.sFlKxw"  # three dot-separated segments
        assert _is_cf_access_allowed(_hdr(token), require=True) is True

    def test_two_segments_blocked(self):
        assert _is_cf_access_allowed(_hdr("only.two"), require=True) is False

    def test_one_segment_blocked(self):
        assert _is_cf_access_allowed(_hdr("noseparator"), require=True) is False

    def test_empty_string_header_blocked(self):
        assert _is_cf_access_allowed(_hdr(""), require=True) is False
