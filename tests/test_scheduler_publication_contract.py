from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import litellm

from newsprism.repo.db import init_db
from newsprism.runtime.scheduler import (
    _drop_blocked_summaries,
    _is_real_article,
    _run_llm_stage,
    _summary_publication_rejection,
)
from newsprism.service.llm_telemetry import tracked_completion
from newsprism.types import Article, ArticleCluster, ClusterSummary


def _summary(
    *,
    url: str = "https://example.com/event",
    is_placeholder: bool = False,
    quality_status: str = "publishable",
    quality_flags: list[str] | None = None,
    summary: str = "**Confirmed event**\n\nSeveral sources describe the development and its public consequences.",
) -> ClusterSummary:
    article = Article(
        url=url,
        title="Event",
        source_name="Example News",
        published_at=datetime.now(tz=timezone.utc),
        content="Several sources describe the development and its public consequences.",
        is_placeholder=is_placeholder,
    )
    return ClusterSummary(
        cluster=ArticleCluster(topic_category="World News", articles=[article]),
        summary=summary,
        quality_status=quality_status,
        quality_flags=list(quality_flags or []),
    )


def test_scheduler_real_article_gate_uses_placeholder_url_as_source_of_truth():
    article = _summary(url="placeholder:ua:cluster", is_placeholder=False).cluster.articles[0]

    assert _is_real_article(article) is False
    assert _summary_publication_rejection(
        _summary(url="placeholder:ua:cluster", is_placeholder=False)
    )


def test_scheduler_withholds_review_and_malformed_numeric_summaries():
    assert _summary_publication_rejection(
        _summary(quality_status="needs_review", quality_flags=["unsupported_numeric_claim"])
    )
    assert _summary_publication_rejection(
        _summary(summary="**Event**\n\n70,000.")
    )


def test_post_translation_gate_drops_summaries_downgraded_after_the_first_gate():
    """2026-09-13: translation-stage numeric grounding set needs_review on
    cluster 7299 after the freshness gate had already admitted it.

    The reason string is the publication-contract code, because
    ``_summary_publication_rejection`` runs ``validate_publication_contract``
    before it inspects ``quality_status``; a non-publishable status therefore
    reports ``quality_status_not_publishable``. This matches the 2026-09-13
    production log line ``reason=quality_status_not_publishable;unsupported_numeric_claim``.
    """
    blocked = _summary(
        quality_status="needs_review",
        quality_flags=["unsupported_numeric_claim", "numeric_safety_failed"],
    )
    healthy = _summary()
    family = {"macro_topic_key": "single-e8b64fc9", "summaries": [blocked, healthy]}

    regular, positive, hot, dropped = _drop_blocked_summaries([blocked], [], [family])

    assert regular == []
    assert positive == []
    # ``blocked`` is reachable from both the regular lane and the family; it is
    # withheld and reported exactly once.
    assert len(dropped) == 1
    assert dropped[0].startswith("quality_status_not_publishable")
    assert "unsupported_numeric_claim" in dropped[0]
    assert hot[0]["summaries"] == [healthy]
    assert hot[0]["member_count"] == 1


def test_post_translation_gate_removes_emptied_families():
    blocked = _summary(quality_status="needs_review")
    family = {"macro_topic_key": "single-empty", "summaries": [blocked]}

    regular, _positive, hot, dropped = _drop_blocked_summaries([], [], [family])

    assert regular == []
    assert hot == []
    assert len(dropped) == 1


def test_post_translation_gate_keeps_healthy_render_set_untouched():
    """The gate runs on every report; a healthy set must pass through intact."""
    healthy = _summary()
    family = {"macro_topic_key": "single-ok", "summaries": [healthy], "member_count": 1}

    regular, positive, hot, dropped = _drop_blocked_summaries([healthy], [], [family])

    assert regular == [healthy]
    assert positive == []
    assert dropped == []
    assert hot[0]["summaries"] == [healthy]
    assert hot[0]["member_count"] == 1


def test_run_llm_stage_attributes_nested_telemetry_to_report(monkeypatch, tmp_path: Path):
    db = tmp_path / "newsprism.db"
    init_db(db)
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="{}"), finish_reason="stop")],
        usage=SimpleNamespace(prompt_tokens=2, completion_tokens=1, total_tokens=3),
        model="test-model",
    )
    monkeypatch.setattr(litellm, "completion", lambda **_kwargs: response)

    _run_llm_stage(
        "2026-08-20",
        lambda: tracked_completion(
            stage="impact",
            enabled=True,
            model="m",
            messages=[{"role": "user", "content": "test"}],
            db_path=db,
        ),
    )

    import sqlite3

    with sqlite3.connect(db) as conn:
        report_date = conn.execute("SELECT report_date FROM llm_call_events").fetchone()[0]
    assert report_date == "2026-08-20"
