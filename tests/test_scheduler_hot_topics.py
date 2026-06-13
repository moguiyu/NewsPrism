import asyncio
from datetime import datetime, timedelta, timezone
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




