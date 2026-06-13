"""Tests for the impact-driven editorial planner: selection, 正能量, display dedup."""
from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.service.editorial_planner import (
    resolve_display_duplicates,
    select_positive_summaries,
    select_report_clusters,
)
from newsprism.types import Article, ArticleCluster, ClusterSummary, ImpactAssessment


def _config(max_per_category: int = 8, max_clusters: int = 20) -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={"max_clusters_per_report": max_clusters},
        dedup={},
        summarizer={},
        output={
            "hot_topics": {"enabled": False},
            "positive_energy": {"enabled": True, "max_items": 5},
        },
        active_search={},
        editorial_values={
            "impact": {
                "diversity": {"max_per_category": max_per_category},
                "positive": {"min_feelgood": 7.0, "max_severity": 4.0},
            }
        },
    )


def _article(title: str, url: str | None = None, region: str = "us", embedding=None) -> Article:
    return Article(
        url=url or f"https://example.com/{title}",
        title=title,
        source_name="Reuters",
        published_at=datetime.now(tz=timezone.utc),
        content=f"{title} body",
        origin_region=region,
        embedding=embedding,
    )


def _cluster(title, composite, category="国际时政", feelgood=0.0, severity=5.0, url=None, embedding=None):
    cluster = ArticleCluster(topic_category=title, articles=[_article(title, url=url, embedding=embedding)])
    cluster.impact = ImpactAssessment(
        cluster_key=title,
        dims={"feelgood": feelgood, "severity": severity},
        composite=composite,
        display_category=category,
        status="publishable",
    )
    cluster.display_category = category
    return cluster


def _summary(title, composite, category="国际时政", feelgood=0.0, severity=5.0, url=None, embedding=None):
    cluster = _cluster(title, composite, category, feelgood, severity, url=url, embedding=embedding)
    summary = ClusterSummary(cluster=cluster, summary=f"**{title}**\n\n{title} body")
    summary.impact = cluster.impact
    summary.display_category = category
    summary.quality_status = "publishable"
    return summary


def test_select_ranks_by_composite():
    clusters = [_cluster("low", 0.20), _cluster("high", 0.80), _cluster("mid", 0.50)]
    _hot, main = select_report_clusters(clusters, _config())
    assert [c.topic_category for c in main] == ["high", "mid", "low"]


def test_select_respects_main_limit():
    clusters = [_cluster(f"c{i}", 0.9 - i * 0.01) for i in range(40)]
    _hot, main = select_report_clusters(clusters, _config(max_clusters=20))
    assert len(main) == 20


def test_select_enforces_category_diversity_cap():
    clusters = [_cluster(f"geo{i}", 0.9 - i * 0.01, category="国际时政") for i in range(10)]
    clusters += [_cluster("tech", 0.40, category="科技创新")]
    _hot, main = select_report_clusters(clusters, _config(max_per_category=3, max_clusters=5))
    assert "科技创新" in [c.display_category for c in main]


def test_positive_selects_high_feelgood_low_severity():
    summaries = [
        _summary("serious", 0.8, feelgood=0.0, severity=9.0),
        _summary("cute animal", 0.4, category="文化艺术", feelgood=9.0, severity=1.0, url="https://a.com/x"),
        _summary("uplifting", 0.4, category="社会民生", feelgood=8.0, severity=2.0, url="https://b.com/y"),
    ]
    titles = [s.cluster.topic_category for s in select_positive_summaries(summaries, _config())]
    assert "cute animal" in titles and "uplifting" in titles and "serious" not in titles


def test_positive_excludes_high_severity_even_if_feelgood():
    summaries = [_summary("bittersweet", 0.5, feelgood=8.0, severity=7.0)]
    assert select_positive_summaries(summaries, _config()) == []


def test_positive_domain_diversity():
    summaries = [
        _summary("a", 0.4, feelgood=9.0, severity=1.0, url="https://same.com/1"),
        _summary("b", 0.4, feelgood=8.5, severity=1.0, url="https://same.com/2"),
    ]
    assert len(select_positive_summaries(summaries, _config())) == 1


def test_positive_respects_max_items():
    summaries = [_summary(f"good{i}", 0.4, feelgood=9.0, severity=1.0, url=f"https://d{i}.com/x") for i in range(8)]
    cfg = _config()
    cfg.output["positive_energy"]["max_items"] = 3
    assert len(select_positive_summaries(summaries, cfg)) == 3


def test_display_dedup_merges_shared_url():
    shared = "https://wire.com/story"
    left = _summary("left", 0.8, url=shared, embedding=[1.0, 0.0])
    right = _summary("right", 0.6, url=shared, embedding=[0.0, 1.0])
    _h, _f, regular, _p = resolve_display_duplicates([], [], [left, right], [])
    assert len(regular) == 1 and regular[0].cluster.topic_category == "left"


def test_display_dedup_merges_near_identical_embeddings():
    left = _summary("event A", 0.8, url="https://a.com/1", embedding=[1.0, 0.0, 0.0])
    right = _summary("event A restated", 0.6, url="https://b.com/2", embedding=[0.99, 0.01, 0.0])
    _h, _f, regular, _p = resolve_display_duplicates([], [], [left, right], [])
    assert len(regular) == 1


def test_display_dedup_keeps_distinct_stories():
    left = _summary("event A", 0.8, url="https://a.com/1", embedding=[1.0, 0.0, 0.0])
    right = _summary("event B", 0.6, url="https://b.com/2", embedding=[0.0, 1.0, 0.0])
    _h, _f, regular, _p = resolve_display_duplicates([], [], [left, right], [])
    assert len(regular) == 2
