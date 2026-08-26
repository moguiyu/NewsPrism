"""Helpers for measuring DeepSeek cache-hit accounting from llm_call_events."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def cache_stats(db_path: str | Path, day: str | None = None) -> dict[str, Any]:
    """Return aggregate cache/usage stats from the telemetry table.

    If ``day`` is provided (YYYY-MM-DD), only rows whose created_at starts
    with that date are included.
    """
    query = """
        SELECT
            COUNT(*) AS events,
            COUNT(prompt_cache_hit_tokens) AS with_cache_fields,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
            COALESCE(SUM(prompt_cache_hit_tokens), 0) AS cache_hit_tokens,
            COALESCE(SUM(prompt_cache_miss_tokens), 0) AS cache_miss_tokens
        FROM llm_call_events
    """
    params: tuple[str, ...] = ()
    if day:
        query += " WHERE substr(created_at, 1, 10) = ?"
        params = (day,)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(query, params).fetchone()

    events, with_cache_fields, total_tokens, prompt_tokens, cache_hit_tokens, cache_miss_tokens = row
    cache_denominator = cache_hit_tokens + cache_miss_tokens
    return {
        "events": events,
        "with_cache_fields": with_cache_fields,
        "total_tokens": total_tokens,
        "prompt_tokens": prompt_tokens,
        "cache_hit_tokens": cache_hit_tokens,
        "cache_miss_tokens": cache_miss_tokens,
        "cache_hit_rate": (cache_hit_tokens / cache_denominator) if cache_denominator else 0.0,
        "cache_share_of_prompt": (cache_hit_tokens / prompt_tokens) if prompt_tokens else 0.0,
    }
