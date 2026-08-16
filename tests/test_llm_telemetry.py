"""LLM telemetry wrapper: records usage without changing call semantics."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import litellm

from newsprism.repo.db import init_db
from newsprism.service.llm_telemetry import tracked_completion


def _fake_response(content: str) -> SimpleNamespace:
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content=content),
                finish_reason="stop",
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=12,
            completion_tokens=7,
            total_tokens=19,
        ),
        model="test-model",
    )


def test_tracked_completion_disabled_returns_raw_response(monkeypatch):
    fake = _fake_response("{}")

    def fake_completion(**kwargs):
        assert kwargs["model"] == "m"
        return fake

    monkeypatch.setattr(litellm, "completion", fake_completion)
    response = tracked_completion(
        stage="clustering",
        enabled=False,
        model="m",
        messages=[{"role": "user", "content": "hello"}],
    )
    assert response is fake


def test_tracked_completion_records_usage_and_can_mark_malformed(monkeypatch, tmp_path):
    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{bad json")

    def fake_completion(**kwargs):
        return fake

    monkeypatch.setattr(litellm, "completion", fake_completion)
    tracked = tracked_completion(
        stage="clustering",
        enabled=True,
        model="m",
        messages=[{"role": "user", "content": "hello"}],
        report_date="2026-08-16",
        item_count=10,
        db_path=db,
    )
    assert tracked.choices[0].message.content == "{bad json"
    tracked.mark("malformed_json")

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT stage, status, prompt_tokens, completion_tokens, "
            "total_tokens, finish_reason, item_count, report_date "
            "FROM llm_call_events"
        ).fetchone()
    assert row == ("clustering", "malformed_json", 12, 7, 19, "stop", 10, "2026-08-16")


def test_tracked_completion_records_api_error(monkeypatch, tmp_path):
    db = tmp_path / "newsprism.db"
    init_db(db)

    def boom(**kwargs):
        raise RuntimeError("network")

    monkeypatch.setattr(litellm, "completion", boom)
    try:
        tracked_completion(
            stage="impact",
            enabled=True,
            model="m",
            messages=[],
            db_path=db,
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError")

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT stage, status FROM llm_call_events").fetchone()
    assert row == ("impact", "api_error")
