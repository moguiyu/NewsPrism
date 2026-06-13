"""Tests for the editor-feedback channel (feedback.py).

Covers:
- feedback_keyboard layout, callback_data, None-cluster_id skipping, row size
- record_feedback_cli maps accept/reject → +1/-1
- FeedbackPoller.poll_once parses a mocked getUpdates response
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from newsprism.runtime.feedback import (
    FeedbackPoller,
    feedback_keyboard,
    format_feedback_list,
    record_feedback_cli,
)


# ─── feedback_keyboard ──────────────────────────────────────────────────────────

def test_feedback_keyboard_button_count():
    stories = [
        {"index": 1, "cluster_id": 10},
        {"index": 2, "cluster_id": 20},
        {"index": 3, "cluster_id": 30},
    ]
    kb = feedback_keyboard(stories)
    buttons = [btn for row in kb["inline_keyboard"] for btn in row]
    # Each story → 2 buttons (👍 + 👎)
    assert len(buttons) == 6


def test_feedback_keyboard_callback_data_format():
    stories = [{"index": 1, "cluster_id": 42}]
    kb = feedback_keyboard(stories)
    buttons = [btn for row in kb["inline_keyboard"] for btn in row]
    data_values = {btn["callback_data"] for btn in buttons}
    assert "fb:42:+1" in data_values
    assert "fb:42:-1" in data_values


def test_feedback_keyboard_skips_none_cluster_id():
    stories = [
        {"index": 1, "cluster_id": None},
        {"index": 2, "cluster_id": 0},    # falsy
        {"index": 3, "cluster_id": 55},
    ]
    kb = feedback_keyboard(stories)
    buttons = [btn for row in kb["inline_keyboard"] for btn in row]
    # Only cluster_id=55 should produce buttons; None and 0 are skipped
    assert len(buttons) == 2
    assert all("55" in btn["callback_data"] for btn in buttons)


def test_feedback_keyboard_at_most_two_stories_per_row():
    stories = [{"index": i, "cluster_id": i * 10} for i in range(1, 6)]
    kb = feedback_keyboard(stories)
    for row in kb["inline_keyboard"]:
        # Each row has buttons for at most 2 stories → ≤ 4 buttons
        assert len(row) <= 4


def test_feedback_keyboard_empty_when_no_valid_stories():
    stories = [{"index": 1, "cluster_id": None}]
    kb = feedback_keyboard(stories)
    assert kb == {"inline_keyboard": []}


def test_feedback_keyboard_empty_input():
    kb = feedback_keyboard([])
    assert kb == {"inline_keyboard": []}


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


# ─── FeedbackPoller.poll_once ──────────────────────────────────────────────────

def _make_poller(tmp_path: Path, token: str = "tok", chat_id: str = "123") -> FeedbackPoller:
    cfg = MagicMock()
    cfg.telegram_bot_token = token
    cfg.telegram_chat_id = chat_id
    cfg.evolution = {}
    poller = FeedbackPoller(cfg)
    poller._offset_path = tmp_path / ".telegram_feedback_offset"
    return poller


def _getUpdates_response(updates: list[dict]) -> dict:
    return {"ok": True, "result": updates}


def _callback_update(update_id: int, cq_id: str, data: str) -> dict:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": cq_id,
            "data": data,
            "from": {"id": 1001},
        },
    }


def test_poll_once_records_valid_feedback_and_ignores_malformed(monkeypatch, tmp_path):
    """One valid fb:<id>:+1 callback and one malformed → exactly 1 row recorded, offset advanced."""
    poller = _make_poller(tmp_path)

    response_body = _getUpdates_response([
        _callback_update(100, "cq1", "fb:42:+1"),
        _callback_update(101, "cq2", "not_feedback_data"),  # malformed
    ])

    recorded: list[tuple] = []

    def fake_insert(cluster_id, verdict, channel="cli", note="", **_):
        recorded.append((cluster_id, verdict, channel))
        return len(recorded)

    monkeypatch.setattr(
        "newsprism.runtime.feedback.insert_editorial_feedback", fake_insert
    )

    # Mock httpx.Client
    mock_response = MagicMock()
    mock_response.json.return_value = response_body

    mock_answer_response = MagicMock()
    mock_answer_response.raise_for_status.return_value = None

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_response
    mock_client.post.return_value = mock_answer_response

    with patch("newsprism.runtime.feedback.httpx.Client", return_value=mock_client):
        count = poller.poll_once()

    assert count == 1
    assert recorded == [(42, 1, "telegram")]

    # Offset should be max(update_id) + 1 = 102
    offset = int((tmp_path / ".telegram_feedback_offset").read_text())
    assert offset == 102


def test_poll_once_returns_zero_when_no_valid_callbacks(monkeypatch, tmp_path):
    poller = _make_poller(tmp_path)
    response_body = _getUpdates_response([
        _callback_update(200, "cq3", "bad_data"),
    ])

    monkeypatch.setattr(
        "newsprism.runtime.feedback.insert_editorial_feedback",
        lambda *a, **k: 1,
    )

    mock_response = MagicMock()
    mock_response.json.return_value = response_body
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_response
    mock_client.post.return_value = MagicMock()

    with patch("newsprism.runtime.feedback.httpx.Client", return_value=mock_client):
        count = poller.poll_once()

    assert count == 0
    assert int((tmp_path / ".telegram_feedback_offset").read_text()) == 201


def test_poll_once_returns_zero_when_token_missing(tmp_path):
    poller = _make_poller(tmp_path, token="", chat_id="")
    count = poller.poll_once()
    assert count == 0


def test_poll_once_returns_zero_when_feedback_disabled(tmp_path):
    cfg = MagicMock()
    cfg.telegram_bot_token = "tok"
    cfg.telegram_chat_id = "123"
    cfg.evolution = {"feedback_enabled": False}
    poller = FeedbackPoller(cfg)
    poller._offset_path = tmp_path / ".telegram_feedback_offset"
    count = poller.poll_once()
    assert count == 0


def test_poll_once_does_not_raise_on_network_error(monkeypatch, tmp_path):
    poller = _make_poller(tmp_path)

    def bad_get(*a, **k):
        raise ConnectionError("network down")

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.side_effect = bad_get

    with patch("newsprism.runtime.feedback.httpx.Client", return_value=mock_client):
        count = poller.poll_once()  # must not raise

    assert count == 0


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
