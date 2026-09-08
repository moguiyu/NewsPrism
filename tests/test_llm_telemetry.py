"""LLM telemetry wrapper: records usage without changing call semantics."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import litellm

from newsprism.repo.db import init_db
from newsprism.service.llm_telemetry import (
    llm_run_context,
    record_llm_parse_failure,
    tracked_completion,
)


def _fake_response(content: str, cost: float | None = None) -> SimpleNamespace:
    usage = SimpleNamespace(
        prompt_tokens=12,
        completion_tokens=7,
        total_tokens=19,
    )
    if cost is not None:
        usage.cost = cost
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content=content),
                finish_reason="stop",
            )
        ],
        usage=usage,
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


def test_tracked_completion_records_deepseek_cache_usage(monkeypatch, tmp_path):
    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{}")
    fake.usage = SimpleNamespace(
        prompt_tokens=100,
        completion_tokens=20,
        total_tokens=120,
        prompt_cache_hit_tokens=70,
        prompt_cache_miss_tokens=30,
    )

    monkeypatch.setattr(litellm, "completion", lambda **_kwargs: fake)
    tracked_completion(
        stage="clustering",
        enabled=True,
        model="deepseek/deepseek-v4-flash",
        messages=[{"role": "user", "content": "hello"}],
        db_path=db,
    )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT prompt_tokens, completion_tokens, total_tokens, "
            "prompt_cache_hit_tokens, prompt_cache_miss_tokens "
            "FROM llm_call_events"
        ).fetchone()
    assert row == (100, 20, 120, 70, 30)


def test_tracked_completion_records_litellm_nested_cache_usage(monkeypatch, tmp_path):
    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{}")
    fake.usage = {
        "prompt_tokens": 100,
        "completion_tokens": 20,
        "total_tokens": 120,
        "prompt_tokens_details": {"cached_tokens": 70},
        "cache_creation_input_tokens": 30,
    }

    monkeypatch.setattr(litellm, "completion", lambda **_kwargs: fake)
    tracked_completion(
        stage="clustering",
        enabled=True,
        model="deepseek/deepseek-v4-flash",
        messages=[{"role": "user", "content": "hello"}],
        db_path=db,
    )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT prompt_cache_hit_tokens, prompt_cache_miss_tokens "
            "FROM llm_call_events"
        ).fetchone()
    assert row == (70, 30)


def test_tracked_completion_ignores_litellm_private_zero_cache_defaults(monkeypatch, tmp_path):
    from litellm.types.utils import Usage

    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{}")
    fake.usage = Usage(
        prompt_tokens=100,
        completion_tokens=20,
        total_tokens=120,
        prompt_tokens_details={"cached_tokens": 70, "cache_creation_tokens": 30},
    )

    monkeypatch.setattr(litellm, "completion", lambda **_kwargs: fake)
    tracked_completion(
        stage="clustering",
        enabled=True,
        model="deepseek/deepseek-v4-flash",
        messages=[{"role": "user", "content": "hello"}],
        db_path=db,
    )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT prompt_cache_hit_tokens, prompt_cache_miss_tokens "
            "FROM llm_call_events"
        ).fetchone()
    assert row == (70, 30)


def test_tracked_completion_leaves_cache_usage_null_when_provider_omits_it(
    monkeypatch, tmp_path
):
    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{}")

    monkeypatch.setattr(litellm, "completion", lambda **_kwargs: fake)
    tracked_completion(
        stage="impact",
        enabled=True,
        model="m",
        messages=[{"role": "user", "content": "hello"}],
        db_path=db,
    )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT prompt_cache_hit_tokens, prompt_cache_miss_tokens "
            "FROM llm_call_events"
        ).fetchone()
    assert row == (None, None)


def test_tracked_completion_uses_context_report_date_when_not_explicit(
    monkeypatch, tmp_path
):
    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{}")

    monkeypatch.setattr(litellm, "completion", lambda **_kwargs: fake)
    with llm_run_context(report_date="2026-08-20"):
        tracked_completion(
            stage="clustering",
            enabled=True,
            model="m",
            messages=[{"role": "user", "content": "hello"}],
            db_path=db,
        )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT report_date FROM llm_call_events").fetchone()
    assert row == ("2026-08-20",)


def test_parse_failure_uses_context_report_date_when_not_explicit(tmp_path):
    db = tmp_path / "newsprism.db"
    init_db(db)

    with llm_run_context(report_date="2026-08-20"):
        record_llm_parse_failure(
            stage="impact",
            enabled=True,
            model="m",
            db_path=db,
        )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT report_date FROM llm_call_events").fetchone()
    assert row == ("2026-08-20",)


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


def test_resolve_stage_models_uses_override_and_fallback():
    from newsprism.service.llm_telemetry import resolve_stage_models

    primary, fallback = resolve_stage_models(
        stage="seeker_keyword",
        default_model="openai/deepseek-v4-flash",
        fallback_model="openai/deepseek-v4-flash",
        stage_models={"seeker_keyword": "openai/cheap"},
    )
    assert primary == "openai/cheap"
    assert fallback == "openai/deepseek-v4-flash"


def test_resolve_stage_models_uses_default_when_no_override():
    from newsprism.service.llm_telemetry import resolve_stage_models

    primary, fallback = resolve_stage_models(
        stage="clustering",
        default_model="openai/deepseek-v4-flash",
        fallback_model="openai/deepseek-v4-flash",
        stage_models={"seeker_keyword": "openai/cheap"},
    )
    assert primary == "openai/deepseek-v4-flash"
    assert fallback == "openai/deepseek-v4-flash"


def test_tracked_completion_with_fallback_uses_fallback_on_api_error(monkeypatch, tmp_path):
    from newsprism.service.llm_telemetry import tracked_completion_with_fallback

    db = tmp_path / "newsprism.db"
    init_db(db)
    calls: list[str] = []

    def fake_completion(**kwargs):
        calls.append(kwargs["model"])
        if kwargs["model"] == "openai/cheap":
            raise RuntimeError("primary down")
        return _fake_response("{}")

    monkeypatch.setattr(litellm, "completion", fake_completion)
    response = tracked_completion_with_fallback(
        stage="seeker_keyword",
        enabled=True,
        model="openai/deepseek-v4-flash",
        fallback_model="openai/deepseek-v4-flash",
        stage_models={"seeker_keyword": "openai/cheap"},
        messages=[{"role": "user", "content": "hello"}],
        db_path=db,
    )
    assert calls == ["openai/cheap", "openai/deepseek-v4-flash"]
    assert response.choices[0].message.content == "{}"


def test_tracked_completion_persists_billed_cost(monkeypatch, tmp_path):
    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{}", cost=0.0000125)

    monkeypatch.setattr(litellm, "completion", lambda **kwargs: fake)
    tracked_completion(
        stage="clustering",
        enabled=True,
        model="m",
        messages=[{"role": "user", "content": "hello"}],
        db_path=db,
    )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT billed_cost_usd FROM llm_call_events"
        ).fetchone()
    assert row == (0.0000125,)


def test_tracked_completion_billed_cost_null_when_provider_omits_cost(monkeypatch, tmp_path):
    db = tmp_path / "newsprism.db"
    init_db(db)
    fake = _fake_response("{}")

    monkeypatch.setattr(litellm, "completion", lambda **kwargs: fake)
    tracked_completion(
        stage="clustering",
        enabled=True,
        model="m",
        messages=[{"role": "user", "content": "hello"}],
        db_path=db,
    )

    import sqlite3

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT billed_cost_usd FROM llm_call_events"
        ).fetchone()
    assert row == (None,)


def test_tracked_completion_warns_when_blended_rate_exceeds_ceiling(monkeypatch, tmp_path, caplog):
    from newsprism.service import llm_telemetry

    db = tmp_path / "newsprism.db"
    init_db(db)
    over = llm_telemetry.BLENDED_RATE_ALERT_USD_PER_1M
    fake = _fake_response("{}", cost=over * 2 * 19 / 1e6)  # fake usage totals 19 tokens -> blended = 2x ceiling

    monkeypatch.setattr(litellm, "completion", lambda **kwargs: fake)
    with caplog.at_level("WARNING", logger="newsprism.service.llm_telemetry"):
        tracked_completion(
            stage="clustering",
            enabled=True,
            model="m",
            messages=[{"role": "user", "content": "hello"}],
            db_path=db,
        )
    assert any("blended" in record.message.lower() for record in caplog.records)


def test_tracked_completion_stays_quiet_at_healthy_rate(monkeypatch, tmp_path, caplog):
    from newsprism.service import llm_telemetry

    db = tmp_path / "newsprism.db"
    init_db(db)
    under = llm_telemetry.BLENDED_RATE_ALERT_USD_PER_1M / 2
    fake = _fake_response("{}", cost=under * 19 / 1e6)  # blended = ceiling/2

    monkeypatch.setattr(litellm, "completion", lambda **kwargs: fake)
    with caplog.at_level("WARNING", logger="newsprism.service.llm_telemetry"):
        tracked_completion(
            stage="clustering",
            enabled=True,
            model="m",
            messages=[{"role": "user", "content": "hello"}],
            db_path=db,
        )
    assert not any("blended" in record.message.lower() for record in caplog.records)
