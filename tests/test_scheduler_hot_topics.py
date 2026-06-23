import asyncio
import logging
from datetime import date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from newsprism.config import Config, SourceConfig
from newsprism.runtime.scheduler import Scheduler
from newsprism.service.editorial_planner import EditorialPlanner, select_report_clusters
from newsprism.types import Article, ArticleCluster, ClusterSummary, EditorialReportPlan, PerspectiveGroup


def _config(main_limit: int = 3) -> Config:
    return Config(
        raw={},
        sources=[
            SourceConfig("Reuters", "Reuters", "https://reuters.com", None, "rss", 1.0, "en", region="us"),
            SourceConfig("BBC", "BBC", "https://bbc.com", None, "rss", 1.0, "en", region="gb"),
        ],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={"max_clusters_per_report": main_limit},
        dedup={},
        summarizer={},
        output={
            "hot_topics": {
                "enabled": True,
                "max_topic_tabs": 3,
                "min_items_per_topic": 5,
                "candidate_window": 40,
                "tab_name_max_chars": 10,
                "icon_allowlist": ["globe", "war", "trade", "chip", "ai", "energy"],
            }
        },
        active_search={},
        topic_equivalence={},
    )


def _article(source: str, title: str, hours_ago: int = 0) -> Article:
    return Article(
        url=f"https://example.com/{source}/{title}",
        title=title,
        source_name=source,
        published_at=datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago),
        content=f"{title} body",
    )


def _cluster(
    name: str,
    role: str = "none",
    storyline_key: str | None = None,
    storyline_name: str | None = None,
    membership_status: str | None = None,
    anchor_labels: list[str] | None = None,
) -> ArticleCluster:
    cluster = ArticleCluster(topic_category="World News", articles=[_article("Reuters", name)])
    cluster.storyline_key = storyline_key
    cluster.storyline_name = storyline_name
    cluster.storyline_role = role
    cluster.storyline_membership_status = membership_status or role
    cluster.storyline_anchor_labels = list(anchor_labels or [])
    cluster.macro_topic_key = storyline_key
    cluster.macro_topic_name = storyline_name
    cluster.macro_topic_icon_key = "globe"
    return cluster


def _summary(cluster: ArticleCluster, headline: str, freshness_state: str = "new") -> ClusterSummary:
    return ClusterSummary(
        cluster=cluster,
        summary=f"**{headline}**\n\n{headline} body.",
        perspectives={article.source_name: f"{article.source_name} angle" for article in cluster.articles},
        freshness_state=freshness_state,
        storyline_key=cluster.storyline_key,
        storyline_name=cluster.storyline_name,
        storyline_role=cluster.storyline_role,
        storyline_confidence=cluster.storyline_confidence,
        macro_topic_key=cluster.macro_topic_key,
        macro_topic_name=cluster.macro_topic_name,
        macro_topic_icon_key=cluster.macro_topic_icon_key,
    )


def test_report_clusters_promote_storyline_with_core_and_direct_spillover():
    cfg = _config(main_limit=2)
    clusters = [
        _cluster("Tariff hike announced", role="core", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Supply chains disrupted", role="spillover", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Market selloff deepens", role="spillover", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Port delays grow", role="spillover", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Air cargo rates spike", role="spillover", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Other story 1"),
        _cluster("Other story 2"),
    ]

    hot_clusters, main_clusters = select_report_clusters(clusters, cfg)

    assert len(hot_clusters) == 5
    assert all(cluster.is_hot_topic for cluster in hot_clusters)
    assert {cluster.storyline_key for cluster in hot_clusters} == {"tariff-shock"}
    assert hot_clusters[0].macro_topic_member_count == 5
    assert len(main_clusters) == 2


def test_report_clusters_do_not_promote_storyline_without_core_anchor():
    cfg = _config(main_limit=4)
    clusters = [
        _cluster(f"Spillover {idx}", role="spillover", storyline_key="market-rumor", storyline_name="市场传闻")
        for idx in range(5)
    ]

    hot_clusters, main_clusters = select_report_clusters(clusters, cfg)

    assert hot_clusters == []
    assert main_clusters == clusters[:4]
    assert all(cluster.is_hot_topic is False for cluster in clusters)


def test_report_clusters_exclude_non_members_from_hot_topic_quota():
    cfg = _config(main_limit=3)
    clusters = [
        _cluster("Tariff core", role="core", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Tariff spillover 1", role="spillover", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Tariff spillover 2", role="spillover", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Tariff spillover 3", role="spillover", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Tariff stray none", role="none", storyline_key="tariff-shock", storyline_name="关税冲击"),
        _cluster("Other story"),
    ]

    hot_clusters, main_clusters = select_report_clusters(clusters, cfg)

    assert hot_clusters == []
    assert main_clusters == clusters[:3]
    assert all(cluster.is_hot_topic is False for cluster in clusters)


def test_scheduler_ignores_focus_storylines_in_public_report_runtime(monkeypatch, tmp_path, caplog):
    cfg = _config(main_limit=3)
    cfg.output["english"] = {"enabled": True}
    today = date(2026, 6, 19)

    hot_summary = _summary(_cluster("Hot", storyline_key="hot", storyline_name="热点"), "Hot")
    focus_summary = _summary(_cluster("Focus", storyline_key="focus", storyline_name="兼容主线"), "Focus")
    regular_summary = _summary(_cluster("Regular"), "Regular")
    positive_summary = _positive_summary("Reuters", "https://example.com/positive", "Positive")
    summaries = [hot_summary, focus_summary, regular_summary, positive_summary]

    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = cfg
    scheduler._pipeline_lock = asyncio.Lock()
    scheduler.schedule_timezone = timezone.utc
    scheduler.output_dir = tmp_path
    scheduler.staging_dir = tmp_path / "staging"
    scheduler.publish_complete_flag = scheduler.staging_dir / ".publish_complete"

    input_articles = [_article("Reuters", "Input")]
    input_cluster = _cluster("Input")
    scheduler.clusterer = SimpleNamespace(cluster=lambda articles: [input_cluster])
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
        translate_calls=[],
    )

    def translate_report_content(summaries_arg, **kwargs):
        scheduler.summarizer.translate_calls.append((summaries_arg, kwargs))
        return True

    scheduler.summarizer.translate_report_content = translate_report_content

    plan = EditorialReportPlan(
        hot_topics=[{"macro_topic_key": "hot", "macro_topic_name": "热点", "summaries": [hot_summary]}],
        focus_storylines=[{"storyline_key": "focus", "storyline_name": "兼容主线", "summaries": [focus_summary]}],
        regular_summaries=[regular_summary],
        positive_summaries=[positive_summary],
    )
    scheduler.editorial_planner = SimpleNamespace(
        base_plan=lambda kept: plan,
        finalize=lambda base, positive_summaries: plan,
    )

    render_calls = []

    def render(*args, **kwargs):
        render_calls.append((args, kwargs))
        return Path(tmp_path / "index.html")

    scheduler.renderer = SimpleNamespace(render=render)

    published = []

    async def publish(summaries_arg, report_date):
        published.extend(summaries_arg)

    scheduler.publisher = SimpleNamespace(publish=publish)

    monkeypatch.setattr("newsprism.runtime.scheduler.select_report_clusters", lambda clusters, cfg: ([], [input_cluster]))
    monkeypatch.setattr("newsprism.runtime.scheduler.select_positive_summaries", lambda kept, cfg: [positive_summary])
    monkeypatch.setattr("newsprism.runtime.scheduler.get_recent_clusters", lambda **kwargs: [])
    monkeypatch.setattr("newsprism.runtime.scheduler.insert_cluster", lambda cluster: 123)
    monkeypatch.setattr("newsprism.runtime.scheduler.insert_article", lambda article: 456)
    monkeypatch.setattr("newsprism.runtime.scheduler.link_cluster_evaluation", lambda *args, **kwargs: None)
    monkeypatch.setattr("newsprism.runtime.scheduler.upsert_storyline_state", lambda *args, **kwargs: None)
    monkeypatch.setattr("newsprism.runtime.scheduler.mark_articles_clustered", lambda ids: None)
    monkeypatch.setattr(Scheduler, "_persist_impact_evaluations", lambda self, clusters, report_date: None)
    monkeypatch.setattr(Scheduler, "_promote_latest_symlink", lambda self, report_date, total_story_count: None)

    caplog.set_level(logging.INFO, logger="newsprism.runtime.scheduler")

    asyncio.run(
        scheduler.publish(
            report_date=today,
            articles_override=input_articles,
            push_after_render=True,
        )
    )

    assert scheduler.summarizer.translate_calls
    _, translate_kwargs = scheduler.summarizer.translate_calls[0]
    assert translate_kwargs["hot_topics"] == plan.hot_topics
    assert translate_kwargs.get("focus_storylines") == []

    assert render_calls
    _, render_kwargs = render_calls[0]
    assert render_kwargs["hot_topics"] == plan.hot_topics
    assert render_kwargs["focus_storylines"] == []
    assert render_kwargs["positive_summaries"] == [positive_summary]

    assert published == [hot_summary, positive_summary, regular_summary]
    assert focus_summary not in published
    assert "focus storyline" not in caplog.text
    assert "3 kept stories after freshness (1 regular main, 1 positive, 1 hot topic stories)" in caplog.text


def _positive_summary(source: str, url: str, headline: str) -> ClusterSummary:
    cluster = ArticleCluster(
        topic_category="Culture",
        articles=[
            Article(
                url=url,
                title=headline,
                source_name=source,
                published_at=datetime.now(tz=timezone.utc),
                content=f"{headline} body",
            )
        ],
    )
    return ClusterSummary(
        cluster=cluster,
        summary=f"**{headline}**\n\n{headline} body.",
        perspectives={},
    )


