"""Tests for the editor-feedback CLI channel (feedback.py).

The Telegram inline-keyboard poller was removed in favor of the portal
backend; remaining coverage is for record_feedback_cli and
format_feedback_list (the shared, channel-agnostic helpers).
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from newsprism.runtime.feedback import (
    format_feedback_list,
    record_feedback_cli,
)


# ─── record_feedback_cli ────────────────────────────────────────────────────────

def test_record_feedback_cli_accept_maps_to_plus_one(monkeypatch):
    calls: list[tuple] = []

    def fake_insert(cluster_id, verdict, channel="cli", note="", **_):
        calls.append((cluster_id, verdict, channel, note))
        return 1

    monkeypatch.setattr(
        "newsprism.runtime.feedback.insert_editorial_feedback", fake_insert
    )
    row_id = record_feedback_cli(99, "accept")
    assert row_id == 1
    assert calls == [(99, 1, "cli", "")]


def test_record_feedback_cli_reject_maps_to_minus_one(monkeypatch):
    calls: list[tuple] = []

    def fake_insert(cluster_id, verdict, channel="cli", note="", **_):
        calls.append((cluster_id, verdict, channel, note))
        return 2

    monkeypatch.setattr(
        "newsprism.runtime.feedback.insert_editorial_feedback", fake_insert
    )
    row_id = record_feedback_cli(7, "reject", note="too biased")
    assert row_id == 2
    assert calls == [(7, -1, "cli", "too biased")]


def test_record_feedback_cli_plus_one_string(monkeypatch):
    calls: list[tuple] = []

    def fake_insert(cluster_id, verdict, channel="cli", note="", **_):
        calls.append((cluster_id, verdict, channel, note))
        return 3

    monkeypatch.setattr(
        "newsprism.runtime.feedback.insert_editorial_feedback", fake_insert
    )
    record_feedback_cli(5, "+1")
    assert calls[0][1] == 1


def test_record_feedback_cli_minus_one_string(monkeypatch):
    calls: list[tuple] = []

    def fake_insert(cluster_id, verdict, channel="cli", note="", **_):
        calls.append((cluster_id, verdict, channel, note))
        return 4

    monkeypatch.setattr(
        "newsprism.runtime.feedback.insert_editorial_feedback", fake_insert
    )
    record_feedback_cli(5, "-1")
    assert calls[0][1] == -1


# ─── format_feedback_list ───────────────────────────────────────────────────────

def test_format_feedback_list_structure(monkeypatch):
    rows = [
        {
            "id": 1,
            "cluster_id": 10,
            "verdict": 1,
            "channel": "telegram",
            "note": "",
            "created_at": "2026-06-12 08:00:00",
            "cluster_summary": "Big news about AI regulation",
            "report_date": "2026-06-12",
        },
        {
            "id": 2,
            "cluster_id": 11,
            "verdict": -1,
            "channel": "cli",
            "note": "off-topic",
            "created_at": "2026-06-12 09:00:00",
            "cluster_summary": "Local weather report from Warsaw",
            "report_date": "2026-06-12",
        },
    ]
    monkeypatch.setattr(
        "newsprism.runtime.feedback.list_editorial_feedback", lambda limit: rows
    )
    result = format_feedback_list(limit=30)
    lines = result.strip().splitlines()
    # First line is the header with total count
    assert "2" in lines[0]
    # Subsequent lines contain cluster_id, date, and summary prefix
    assert "cluster=10" in result
    assert "cluster=11" in result
    assert "2026-06-12" in result
    assert "Big news about AI" in result
