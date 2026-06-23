"""Tests for the impact-driven editorial planner: selection, 正能量, display dedup."""
from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.service.editorial_planner import (
    EditorialPlanner,
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


def _storyline_summary(title, composite, key, role="spillover", category="国际时政"):
    summary = _summary(title, composite, category=category)
    summary.cluster.storyline_key = key
    summary.cluster.storyline_name = "小型专题"
    summary.cluster.storyline_role = role
    summary.cluster.storyline_membership_status = role
    summary.cluster.macro_topic_key = key
    summary.cluster.macro_topic_name = "小型专题"
    summary.cluster.macro_topic_icon_key = "globe"
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


def test_select_normalizes_legacy_categories_for_diversity_cap():
    clusters = [
        _cluster("sport legacy 1", 0.99, category="体育运动"),
        _cluster("sport public 2", 0.98, category="Culture & Sports"),
        _cluster("sport legacy 3", 0.97, category="体育运动"),
        _cluster("sport public 4", 0.96, category="Culture & Sports"),
        _cluster("world", 0.50, category="World"),
    ]

    _hot, main = select_report_clusters(clusters, _config(max_per_category=2, max_clusters=5))

    assert [cluster.topic_category for cluster in main] == [
        "sport legacy 1",
        "sport public 2",
        "world",
        "sport legacy 3",
        "sport public 4",
    ]


def test_base_plan_returns_small_non_hot_group_to_regular_pool_without_focus_lane():
    cfg = _config(max_clusters=3)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "min_items_per_topic": 5,
        "tab_name_max_chars": 10,
    }
    summaries = [
        _storyline_summary("small group core", 0.95, "small-topic", role="core"),
        _storyline_summary("small group follow", 0.90, "small-topic"),
        _summary("standalone high", 0.80, category="商业财经"),
        _summary("standalone low", 0.10, category="科技创新"),
    ]

    plan = EditorialPlanner(cfg).base_plan(summaries)

    assert plan.hot_topics == []
    assert plan.focus_storylines == []
    assert [summary.cluster.topic_category for summary in plan.regular_summaries] == [
        "small group core",
        "small group follow",
        "standalone high",
    ]


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


def test_display_dedup_merges_crosslang_same_event_in_positive_lane():
    """Two clusters covering the same event in different languages (centroid
    cosine ~0.78, no shared URL, no title overlap) must collapse to one entry."""
    # cos ≈ 0.78: below the old 0.80 bar, above the new 0.75 bar.
    zh = _summary("尼克斯夺冠", 0.4, category="社会民生", feelgood=9.0, severity=1.0,
                  url="https://cn.example/nba", embedding=[0.78, 0.626, 0.0])
    en = _summary("Knicks win title", 0.4, category="社会民生", feelgood=9.0, severity=1.0,
                  url="https://en.example/nba", embedding=[1.0, 0.0, 0.0])
    _h, _f, _r, pos = resolve_display_duplicates([], [], [], [zh, en])
    assert len(pos) == 1


def test_finalize_positive_member_of_family_renders_in_positive_lane():
    """A positive pick that is also a storyline member must render once, in the
    positive lane — not be suppressed out of every lane by self-collision."""
    from newsprism.service.editorial_planner import EditorialPlanner
    from newsprism.types import EditorialReportPlan

    story = _summary("nba final", 0.4, category="社会民生", feelgood=9.0, severity=1.0)
    base = EditorialReportPlan(
        hot_topics=[],
        focus_storylines=[{"storyline_key": "s1", "summaries": [story], "member_count": 1}],
        regular_summaries=[],
        positive_summaries=[],
    )
    plan = EditorialPlanner.__new__(EditorialPlanner)
    result = plan.finalize(base, positive_summaries=[story])
    assert len(result.positive_summaries) == 1
    assert result.focus_storylines == []


def test_display_dedup_keeps_positive_over_main_duplicate():
    """A positive pick must win dedup against its higher-composite main twin.

    feelgood carries no composite weight, so without lane priority the positive
    copy always loses the tiebreak and the 正能量 lane silently empties.
    """
    shared = "https://wire.com/feelgood"
    main = _summary("main twin", 0.8, url=shared, feelgood=9.0, severity=1.0)
    positive = _summary("positive twin", 0.4, url=shared, feelgood=9.0, severity=1.0)
    _h, _f, regular, pos = resolve_display_duplicates([], [], [main], [positive])
    assert len(pos) == 1 and pos[0].cluster.topic_category == "positive twin"
    assert regular == []
