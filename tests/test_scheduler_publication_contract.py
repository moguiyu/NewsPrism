import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import litellm

from newsprism.config import Config, SourceConfig
from newsprism.repo.db import init_db
from newsprism.runtime.scheduler import (
    Scheduler,
    _drop_blocked_summaries,
    _is_real_article,
    _render_funnel_drop_reasons,
    _run_llm_stage,
    _summary_publication_rejection,
)
from newsprism.service.llm_telemetry import tracked_completion
from newsprism.types import Article, ArticleCluster, ClusterSummary, EditorialReportPlan


def test_render_funnel_reasons_name_duplicate_and_unselected_ids():
    duplicate = _summary()
    duplicate.cluster_db_id = 1
    duplicate.duplicate_action = "suppressed"
    duplicate.duplicate_reason = "shared_article_url"
    unselected = _summary()
    unselected.cluster_db_id = 2
    rendered = _summary()
    rendered.cluster_db_id = 3

    reasons = _render_funnel_drop_reasons([duplicate, unselected, rendered], {3})

    assert reasons == {
        1: "display_duplicate:shared_article_url",
        2: "not_selected",
    }


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


def _runtime_scheduler(monkeypatch, tmp_path: Path, *, stub_impact_persistence: bool = True):
    """Build a Scheduler whose publish() drives the real pipeline to the gates.

    ``stub_impact_persistence`` keeps the impact/audit writes off the disk for tests
    that only assert on the funnel; a test that checks what gets persisted has to
    turn it off so the real ``_persist_impact_evaluations`` runs.
    """
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
        classify_all=lambda items, historical, report_date=None, **kwargs: [
            (summary.cluster, summary.summary, SimpleNamespace(state="new", continues_cluster_id=None))
            for summary in summaries
        ]
    )
    scheduler.summarizer = SimpleNamespace(
        summarize_all_batch=lambda clusters: summaries,
        translate_report_content=lambda *args, **kwargs: True,
    )
    scheduler.editorial_planner = SimpleNamespace(
        base_plan=lambda kept, **kwargs: plan,
        finalize=lambda base, positive_summaries, **kwargs: plan,
    )
    scheduler.renderer = SimpleNamespace(render=lambda *args, **kwargs: tmp_path / "index.html")

    async def _noop_publish(*args, **kwargs):
        return None

    scheduler.publisher = SimpleNamespace(publish=_noop_publish)

    monkeypatch.setattr(
        "newsprism.runtime.scheduler.select_report_clusters",
        lambda clusters, cfg, drop_reasons=None: ([], [input_cluster]),
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
    if stub_impact_persistence:
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


def test_publish_consumes_every_validated_cluster_before_editorial_selection(monkeypatch, tmp_path: Path):
    """Phase 0: unselected clusters must not return as unclustered articles.

    The candidate window and editorial lanes intentionally drop most clusters,
    but clustering already consumed their articles. Marking only stored clusters
    fed the rest back into the next publish, inflating the 48h input and LLM
    clustering cost. The scheduler must mark the full validated set.
    """
    scheduler, plan, input_cluster = _runtime_scheduler(monkeypatch, tmp_path)
    input_cluster.articles[0].id = 4242
    marked: list[list[int]] = []
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered",
        lambda ids: marked.append(list(ids)),
    )

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles),
            push_after_render=True,
        )
    )

    assert marked, "publish did not mark any articles"
    assert 4242 in marked[0]


def _omitted_article(article_id: int, *, published: datetime) -> Article:
    return Article(
        url=f"https://example.com/omitted/{article_id}",
        title=f"Omitted {article_id}",
        source_name="Reuters",
        published_at=published,
        content="A story the clusterer left out of every event group.",
        id=article_id,
    )


def test_publish_retires_an_omitted_article_after_one_retry(monkeypatch, tmp_path: Path):
    """An article the LLM drops from every cluster must not be re-fed forever.

    The 48h input window would otherwise resubmit it on the next publish and pay
    for the same tokens twice. Retire it once it is older than one publish
    interval, which proves it already sat in an earlier pool.
    """
    scheduler, _plan, input_cluster = _runtime_scheduler(monkeypatch, tmp_path)
    input_cluster.articles[0].id = 4242
    stale_omitted = _omitted_article(
        7001, published=datetime.now(tz=timezone.utc) - timedelta(hours=30)
    )
    marked: list[list[int]] = []
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered",
        lambda ids: marked.append(list(ids)),
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.get_unclustered_articles",
        lambda **kwargs: [stale_omitted],
    )

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles) + [stale_omitted],
            push_after_render=True,
        )
    )

    assert marked, "publish did not mark any articles"
    assert 4242 in marked[0], "clustered article was not marked"
    assert 7001 in marked[0], "omitted article older than one interval must be retired"


def test_publish_keeps_a_recent_omitted_article_for_a_second_attempt(monkeypatch, tmp_path: Path):
    """A story published just before this publish keeps its retry.

    Age, not the calendar date, decides: an article published yesterday
    afternoon is new to today's run and tomorrow's coverage may cluster it.
    """
    scheduler, _plan, input_cluster = _runtime_scheduler(monkeypatch, tmp_path)
    input_cluster.articles[0].id = 4242
    fresh_omitted = _omitted_article(
        7002, published=datetime.now(tz=timezone.utc) - timedelta(hours=6)
    )
    marked: list[list[int]] = []
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered",
        lambda ids: marked.append(list(ids)),
    )

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles) + [fresh_omitted],
            push_after_render=True,
        )
    )

    assert marked
    assert 4242 in marked[0]
    assert 7002 not in marked[0], "a recently published article must get its retry"


def test_omitted_retirement_can_be_disabled_with_zero_hours(monkeypatch, tmp_path: Path):
    """Setting omitted_retry_age_hours to 0 must restore the old behaviour.

    The retirement is a cost/coverage trade the operator may want to reverse
    without a code change.
    """
    scheduler, _plan, input_cluster = _runtime_scheduler(monkeypatch, tmp_path)
    scheduler.cfg.clustering["omitted_retry_age_hours"] = 0
    input_cluster.articles[0].id = 4242
    stale_omitted = _omitted_article(
        7003, published=datetime.now(tz=timezone.utc) - timedelta(hours=30)
    )
    marked: list[list[int]] = []
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered",
        lambda ids: marked.append(list(ids)),
    )

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles) + [stale_omitted],
            push_after_render=True,
        )
    )

    assert marked
    assert 4242 in marked[0]
    assert 7003 not in marked[0], "0 must disable omitted-article retirement"


def test_audit_key_survives_articles_appended_by_the_seeker():
    """The audit row must stay findable after the funnel mutates the cluster.

    ``impact.cluster_key`` hashes the first four real article URLs, and both the
    seeker and display-dedup append articles later, so recomputing the key at
    write-back time missed the row it was recorded under. Production 2026-10-29:
    3 of 15 rendered cards (8604, 8611, 8617) had no audit row at all, and 72 of
    90 in-window candidates were never given a final verdict.
    """
    from newsprism.runtime.scheduler import _audit_key_of

    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url=f"https://example.com/{index}",
                title=f"Story {index}",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="Body",
            )
            for index in range(3)
        ],
    )
    frozen = "0123456789abcdef"
    cluster._audit_key = frozen  # type: ignore[attr-defined]

    cluster.articles.append(
        Article(
            url="https://example.com/searched-later",
            title="Searched later",
            source_name="AP",
            published_at=datetime.now(tz=timezone.utc),
            content="Body",
        )
    )

    assert _audit_key_of(cluster) == frozen


def test_audit_key_falls_back_when_no_row_was_written():
    """Shadow runs and unit paths never freeze a key; recompute instead."""
    from newsprism.runtime.scheduler import _audit_key_of
    from newsprism.service.impact import cluster_key

    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/only",
                title="Only",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="Body",
            )
        ],
    )

    assert _audit_key_of(cluster) == cluster_key(cluster)


def test_every_in_window_candidate_receives_a_final_audit_verdict(monkeypatch, tmp_path: Path):
    """No in-window candidate may be left at "candidate".

    Production 2026-10-29 wrote 1351 audit rows but finalized only 18 of the 90
    in-window candidates: the write-back loop covered ``kept_summaries`` only, so
    stories dropped by the lane quota, the impact status or the publication gate
    kept the provisional "candidate" verdict forever.
    """
    scheduler, plan, input_cluster = _runtime_scheduler(monkeypatch, tmp_path)
    dropped = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/dropped-by-quota",
                title="Dropped by quota",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="Body",
            )
        ],
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered", lambda ids: None
    )
    written: list[list[dict]] = []
    verdicts: list[dict[str, tuple[str, str]]] = []
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.insert_cluster_audit",
        lambda rows: written.append(list(rows)),
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.update_cluster_audit_outcomes",
        lambda report_date, outcomes: verdicts.append(dict(outcomes)) or len(outcomes),
    )
    # Window holds two clusters; the lane quota keeps only one of them.
    scheduler.impact_assessor = SimpleNamespace(
        rank_candidates=lambda clusters, window: list(clusters),
        assess_clusters=lambda clusters: None,
        recompute_local=lambda cluster: None,
        _signal=lambda cluster: (0.5, []),
    )
    scheduler.clusterer = SimpleNamespace(
        cluster=lambda articles, **kwargs: [input_cluster, dropped]
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.select_report_clusters",
        lambda clusters, cfg, drop_reasons=None: ([], [input_cluster]),
    )

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles),
            push_after_render=True,
        )
    )

    assert verdicts, "publish never wrote final audit verdicts"
    final = verdicts[-1]
    from newsprism.runtime.scheduler import _audit_key_of

    for cluster in (input_cluster, dropped):
        key = _audit_key_of(cluster)
        assert key in final, f"{cluster.topic_category} left without a verdict"
        assert final[key][0] != "candidate", f"{cluster.topic_category} kept the provisional verdict"


def test_publish_backfills_the_score_selection_actually_uses(monkeypatch, tmp_path: Path):
    """The audit row must end up carrying `composite`, not only `signal`.

    `_persist_cluster_audit` runs before scoring, so the row it writes can only
    hold the local signal. Selection ranks on `composite` and then caps each
    display category, so without the backfill the table cannot answer why a
    top-ranked story lost -- exactly the 2026-10-29 case where five publishable
    candidates outscored the weakest published card and nothing recorded it.
    """
    from newsprism.types import ImpactAssessment

    scheduler, plan, input_cluster = _runtime_scheduler(
        monkeypatch, tmp_path, stub_impact_persistence=False
    )
    input_cluster.impact = ImpactAssessment(
        cluster_key="whatever",
        dims={"scope": 6.0, "severity": 7.0, "novelty": 9.0},
        signal=0.61,
        composite=0.682,
        display_category="Science & Health",
        status="publishable",
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered", lambda ids: None
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.insert_cluster_audit", lambda rows: len(rows)
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.update_cluster_audit_outcomes",
        lambda report_date, outcomes: len(outcomes),
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.insert_cluster_evaluation",
        lambda **kwargs: None,
    )
    backfilled: list[list[dict]] = []
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.backfill_cluster_audit_scores",
        lambda rows: backfilled.append(list(rows)) or len(rows),
    )

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles),
            push_after_render=True,
        )
    )

    assert backfilled, "publish never backfilled the audit scores"
    rows = {row["cluster_key"]: row for row in backfilled[-1]}
    from newsprism.runtime.scheduler import _audit_key_of

    key = _audit_key_of(input_cluster)
    assert key in rows, "the assessed cluster has no backfilled row"
    assert rows[key]["composite"] == 0.682
    assert rows[key]["display_category"] == "Science & Health"
    assert rows[key]["lane"] in {"main", "hot"}
    assert rows[key]["report_date"] == "2026-06-19"


def test_publish_audits_the_constraint_that_dropped_a_candidate(monkeypatch, tmp_path: Path):
    """The audit reason must name the binding constraint, not just "lane_quota".

    A candidate cut by the World display-category cap and one cut because every
    slot went to a higher score need different answers, and 2026-10-29 produced
    five such candidates that the table could not distinguish.
    """
    scheduler, plan, input_cluster = _runtime_scheduler(monkeypatch, tmp_path)
    dropped = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/dropped-by-cap",
                title="Dropped by cap",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="Body",
            )
        ],
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.mark_articles_clustered", lambda ids: None
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.insert_cluster_audit", lambda rows: len(rows)
    )
    verdicts: list[dict[str, tuple[str, str]]] = []
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.update_cluster_audit_outcomes",
        lambda report_date, outcomes: verdicts.append(dict(outcomes)) or len(outcomes),
    )
    scheduler.clusterer = SimpleNamespace(
        cluster=lambda articles, **kwargs: [input_cluster, dropped]
    )

    def _fake_select(clusters, cfg, drop_reasons=None):
        for cluster in clusters:
            if cluster is dropped and drop_reasons is not None:
                drop_reasons[id(cluster)] = "category_cap:World News"
        return [], [input_cluster]

    monkeypatch.setattr(
        "newsprism.runtime.scheduler.select_report_clusters", _fake_select
    )

    asyncio.run(
        scheduler.publish(
            report_date=date(2026, 6, 19),
            articles_override=list(input_cluster.articles),
            push_after_render=True,
        )
    )

    assert verdicts, "publish never wrote final audit verdicts"
    from newsprism.runtime.scheduler import _audit_key_of

    assert verdicts[-1][_audit_key_of(dropped)] == ("not_selected", "category_cap:World News")


def test_publish_warns_when_a_weak_card_displaces_a_stronger_story(caplog):
    """A category cap may drop a stronger story, but never quietly.

    Production 2026-10-29 published a 0.635 card while a publishable 0.682 card
    was dropped, and nothing in the logs named the trade. The ranking stays; the
    displacement gets a warning.
    """
    from newsprism.runtime.scheduler import _warn_on_outranked_candidates
    from newsprism.types import ImpactAssessment

    def _cluster(url: str, composite: float, status: str = "publishable") -> ArticleCluster:
        cluster = ArticleCluster(
            topic_category="World News",
            articles=[
                Article(
                    url=url,
                    title=url,
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Body",
                )
            ],
        )
        cluster.impact = ImpactAssessment(
            cluster_key=url, composite=composite, status=status, display_category="World"
        )
        return cluster

    weak = _cluster("https://example.com/weak", 0.435)
    strong = _cluster("https://example.com/strong", 0.682)
    blocked = _cluster("https://example.com/blocked", 0.700, status="needs_review")

    with caplog.at_level(logging.WARNING):
        _warn_on_outranked_candidates([weak], [weak, strong, blocked])

    messages = [record.getMessage() for record in caplog.records]
    assert messages, "the displacement was silent"
    joined = "\n".join(messages)
    assert "1 publishable candidate" in joined
    assert "0.682" in joined
    assert "0.700" not in joined, "a non-publishable candidate is not a loss"
