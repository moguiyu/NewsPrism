"""Tests for LLM cache accounting stats used by cache experiments."""
from __future__ import annotations

from pathlib import Path

from newsprism.repo.db import init_db, insert_llm_call_event
from newsprism.runtime.cache_stats import cache_stats
from newsprism.types import LLMCallEvent


def _make_db(tmp_path: Path) -> Path:
    db = tmp_path / "newsprism.db"
    init_db(db)
    insert_llm_call_event(
        LLMCallEvent(
            stage="clustering",
            model="m",
            prompt_tokens=100,
            completion_tokens=10,
            total_tokens=110,
            prompt_cache_hit_tokens=20,
            prompt_cache_miss_tokens=80,
            created_at=__import__("datetime").datetime(2026, 8, 21, 5, 0, 0),
        ),
        db_path=db,
    )
    insert_llm_call_event(
        LLMCallEvent(
            stage="clustering",
            model="m",
            prompt_tokens=200,
            completion_tokens=20,
            total_tokens=220,
            prompt_cache_hit_tokens=None,
            prompt_cache_miss_tokens=None,
            created_at=__import__("datetime").datetime(2026, 8, 20, 5, 0, 0),
        ),
        db_path=db,
    )
    return db


def test_cache_stats_returns_aggregate(tmp_path):
    db = _make_db(tmp_path)
    stats = cache_stats(db)
    assert stats["events"] == 2
    assert stats["total_tokens"] == 330
    assert stats["cache_hit_tokens"] == 20
    assert stats["cache_miss_tokens"] == 80
    assert stats["with_cache_fields"] == 1
    assert stats["cache_hit_rate"] == 20 / 100


def test_cache_stats_filters_by_day(tmp_path):
    db = _make_db(tmp_path)
    stats = cache_stats(db, day="2026-08-21")
    assert stats["events"] == 1
    assert stats["cache_hit_tokens"] == 20
    assert stats["cache_miss_tokens"] == 80
