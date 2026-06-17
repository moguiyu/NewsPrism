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


import pytest
from fastapi.testclient import TestClient

from newsprism.repo.db import init_db
from newsprism.runtime.portal.app import create_app


_VALID_CF_HEADER = {"Cf-Access-Jwt-Assertion": "eyJhbG.eyJzdWI.sFlKxw"}


def _app_client(monkeypatch, db_path, require_cf: bool):
    """Build a portal app + TestClient under a controlled CF-Access flag."""
    if require_cf:
        monkeypatch.setenv("PORTAL_REQUIRE_CF_ACCESS", "true")
    else:
        monkeypatch.setenv("PORTAL_REQUIRE_CF_ACCESS", "false")
    init_db(db_path)
    return TestClient(create_app(db_path=db_path))


class TestMiddlewareFlagOn:
    def test_root_blocked_without_header(self, tmp_path, monkeypatch):
        client = _app_client(monkeypatch, tmp_path / "n.db", require_cf=True)
        assert client.get("/").status_code == 401

    def test_root_admitted_with_valid_header(self, tmp_path, monkeypatch):
        client = _app_client(monkeypatch, tmp_path / "n.db", require_cf=True)
        r = client.get("/", headers=_VALID_CF_HEADER)
        assert r.status_code == 200

    def test_write_api_also_blocked(self, tmp_path, monkeypatch):
        client = _app_client(monkeypatch, tmp_path / "n.db", require_cf=True)
        r = client.post("/api/verdict", json={"cluster_id": 1, "verdict": 1})
        assert r.status_code == 401


class TestMiddlewareFlagOff:
    def test_root_admitted_without_header(self, tmp_path, monkeypatch):
        client = _app_client(monkeypatch, tmp_path / "n.db", require_cf=False)
        assert client.get("/").status_code == 200


class TestHealthzExempt:
    def test_healthz_passes_without_header_when_required(self, tmp_path, monkeypatch):
        client = _app_client(monkeypatch, tmp_path / "n.db", require_cf=True)
        assert client.get("/healthz").status_code == 200


class TestSecureByDefault:
    def test_unset_env_defaults_to_required(self, tmp_path, monkeypatch):
        monkeypatch.delenv("PORTAL_REQUIRE_CF_ACCESS", raising=False)
        init_db(tmp_path / "n.db")
        client = TestClient(create_app(db_path=tmp_path / "n.db"))
        # No env var set -> must still block a headerless request.
        assert client.get("/").status_code == 401
