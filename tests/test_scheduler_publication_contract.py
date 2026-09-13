import asyncio
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import litellm

from newsprism.config import Config, SourceConfig
from newsprism.repo.db import init_db
from newsprism.runtime.scheduler import (
    Scheduler,
    _drop_blocked_summaries,
    _is_real_article,
    _run_llm_stage,
    _summary_publication_rejection,
)
from newsprism.service.llm_telemetry import tracked_completion
from newsprism.types import Article, ArticleCluster, ClusterSummary, EditorialReportPlan


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


def _distinct_summary(headline: str) -> ClusterSummary:
    return _summary(
        summary=f"**{headline}**\n\nSeveral independent sources describe the development and its consequences."
    )


def test_gate_two_keeps_clean_chinese_card_when_english_trips_a_pattern():
    """Gate 2 decides the CHINESE render set, so the English translation must not
    remove a card.

    ``_MALFORMED_NUMERIC_REMNANT_PATTERN`` carries the English alternative
    ``\\b(?:kills?|killed|dead|deaths?|injured?)\\s*[,.;:]``, which ordinary
    grammatical English satisfies. Because the translation stage fills
    ``summary_en`` even when the English edition is later withheld, validating it
    here would silently delete casualty and death-toll stories -- this product's
    core coverage -- from the Chinese edition for an English-only reason.
    """
    summary = _summary()
    summary.summary_en = "Several people were killed, according to local officials."

    regular, _positive, _hot, dropped = _drop_blocked_summaries([summary], [], [])

    assert regular == [summary]
    assert dropped == []


def test_gate_two_still_drops_summaries_downgraded_after_translation():
    """The include_english switch must not neuter F3: a summary that the
    translation stage downgraded is still withheld."""
    downgraded = _summary(
        quality_status="needs_review",
        quality_flags=["unsupported_numeric_claim", "numeric_safety_failed"],
    )
    downgraded.summary_en = "Several people were killed, according to local officials."

    regular, _positive, _hot, dropped = _drop_blocked_summaries([downgraded], [], [])

    assert regular == []
    assert len(dropped) == 1
    assert dropped[0].startswith("quality_status_not_publishable")
    assert "unsupported_numeric_claim" in dropped[0]


def test_gate_two_preserves_unrecognised_family_members(caplog):
    """The contract is "drop blocked summaries", not "drop anything unrecognised".

    A non-ClusterSummary member is preserved and named in a warning instead of
    being silently discarded (which could also empty and delete its family).
    """
    healthy = _distinct_summary("Healthy member")
    sentinel = "not-a-cluster-summary"
    mixed = {"macro_topic_key": "single-mixed", "summaries": [sentinel, healthy]}
    only_sentinel = {"macro_topic_key": "single-odd", "summaries": [sentinel]}

    with caplog.at_level(logging.WARNING, logger="newsprism.runtime.scheduler"):
        _regular, _positive, hot, dropped = _drop_blocked_summaries(
            [], [], [mixed, only_sentinel]
        )

    assert hot[0]["summaries"] == [sentinel, healthy]
    assert hot[0]["member_count"] == 2
    # A family whose only member is unrecognised survives rather than vanishing.
    assert hot[1]["summaries"] == [sentinel]
    assert hot[1]["member_count"] == 1
    assert dropped == []
    assert "single-mixed" in caplog.text
    assert "single-odd" in caplog.text


def _runtime_scheduler(monkeypatch, tmp_path: Path):
    """Build a Scheduler whose publish() drives the real pipeline to the gates."""
    cfg = Config(
        raw={},
        sources=[
            SourceConfig("Reuters", "Reuters", "https://reuters.com", None, "rss", 1.0, "en", region="us"),
        ],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={"max_clusters_per_report": 3},
        dedup={},
        summarizer={},
        output={
            "hot_topics": {
                "enabled": True,
                "max_topic_tabs": 3,
                "tab_name_max_chars": 10,
                "icon_allowlist": ["globe"],
            },
            # Enabled so the translation stage runs, which is the stage that
            # downgrades a summary after gate 1 has already admitted it.
            "english": {"enabled": True},
        },
        active_search={},
        topic_equivalence={},
    )
    hot = _distinct_summary("Hot topic member")
    regular = _distinct_summary("Regular story")
    positive = _distinct_summary("Positive story")
    plan = EditorialReportPlan(
        hot_topics=[{"macro_topic_key": "hot", "macro_topic_name": "热点", "summaries": [hot]}],
        regular_summaries=[regular],
        positive_summaries=[positive],
    )
    input_cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/input",
                title="Input",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="Input body",
            )
        ],
    )
    summaries = [hot, regular, positive]

    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = cfg
    scheduler._pipeline_lock = asyncio.Lock()
    scheduler.schedule_timezone = timezone.utc
    scheduler.output_dir = tmp_path
    scheduler.staging_dir = tmp_path / "staging"
    scheduler.publish_complete_flag = scheduler.staging_dir / ".publish_complete"
    scheduler.clusterer = SimpleNamespace(cluster=lambda articles, **kwargs: [input_cluster])
    scheduler.cluster_validator = SimpleNamespace(validate=lambda clusters: clusters)
    scheduler.impact_assessor = SimpleNamespace(
        rank_candidates=lambda clusters, window: clusters,
        assess_clusters=lambda clusters: None,
        recompute_local=lambda cluster: None,
    )
    scheduler.seeker = SimpleNamespace(enhance_clusters=lambda clusters: clusters)
    scheduler.storyline_resolver = SimpleNamespace(resolve=lambda *args, **kwargs: None)
    scheduler.storyline_state_machine = SimpleNamespace(apply=lambda *args, **kwargs: None)
    scheduler.freshness_evaluator = SimpleNamespace(
        classify_all=lambda items, historical: [
            (summary.cluster, summary.summary, SimpleNamespace(state="new", continues_cluster_id=None))
            for summary in summaries
        ]
    )
    scheduler.summarizer = SimpleNamespace(
        summarize_all_batch=lambda clusters: summaries,
        translate_report_content=lambda *args, **kwargs: True,
    )
    scheduler.editorial_planner = SimpleNamespace(
        base_plan=lambda kept: plan,
        finalize=lambda base, positive_summaries: plan,
    )
    scheduler.renderer = SimpleNamespace(render=lambda *args, **kwargs: tmp_path / "index.html")

    async def _noop_publish(*args, **kwargs):
        return None

    scheduler.publisher = SimpleNamespace(publish=_noop_publish)

    monkeypatch.setattr(
        "newsprism.runtime.scheduler.select_report_clusters",
        lambda clusters, cfg: ([], [input_cluster]),
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.select_positive_summaries",
        lambda kept, cfg: [positive],
    )
    monkeypatch.setattr("newsprism.runtime.scheduler.get_recent_clusters", lambda **kwargs: [])
    monkeypatch.setattr("newsprism.runtime.scheduler.insert_cluster", lambda cluster: 123)
    monkeypatch.setattr("newsprism.runtime.scheduler.insert_article", lambda article: 456)
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.link_cluster_evaluation", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.upsert_storyline_state", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered", lambda ids: None
    )
    monkeypatch.setattr(
        Scheduler, "_persist_impact_evaluations", lambda self, clusters, report_date: None
    )
    monkeypatch.setattr(
        Scheduler, "_promote_latest_symlink", lambda self, report_date, total_story_count: None
    )
    return scheduler, plan, input_cluster


def test_publish_invokes_the_post_translation_gate(monkeypatch, tmp_path: Path):
    """Direct wiring assertion.

    The gate's own unit tests prove what it drops, but nothing else proves that
    ``Scheduler.publish`` actually calls it -- a refactor could delete the call
    site without turning any test red.
    """
    scheduler, plan, input_cluster = _runtime_scheduler(monkeypatch, tmp_path)
    gate_calls: list[tuple] = []

    def spy(*args):
        gate_calls.append(args)
        return _drop_blocked_summaries(*args)

    monkeypatch.setattr("newsprism.runtime.scheduler._drop_blocked_summaries", spy)

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles),
            push_after_render=True,
        )
    )

    assert len(gate_calls) == 1
    called_regular, called_positive, called_hot = gate_calls[0]
    assert called_regular == plan.regular_summaries
    assert called_positive == plan.positive_summaries
    assert called_hot == plan.hot_topics
