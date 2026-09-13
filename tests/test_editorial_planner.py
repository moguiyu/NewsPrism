"""Tests for the impact-driven editorial planner: selection, 正能量, display dedup."""
import logging
from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.service.editorial_planner import (
    EditorialPlanner,
    resolve_display_duplicates,
    select_positive_summaries,
    select_report_clusters,
)
from newsprism.types import Article, ArticleCluster, ClusterSummary, ImpactAssessment, PerspectiveGroup


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


def test_base_plan_two_member_storyline_claims_tab_under_new_policy():
    """Issue #2 rec #4: every storyline family with ≥2 members gets a tab.

    The previous behavior (small groups → main lane) produced the "spilled
    into main lane" symptom for ongoing-conflict families. Under the new
    policy, this 2-member family claims a tab; only the standalone summaries
    flow to the main lane.
    """
    cfg = _config(max_clusters=3)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "min_items_per_topic": 3,
        "tab_name_max_chars": 10,
    }
    summaries = [
        _storyline_summary("small group core", 0.95, "small-topic", role="core"),
        _storyline_summary("small group follow", 0.90, "small-topic"),
        _summary("standalone high", 0.80, category="商业财经"),
        _summary("standalone low", 0.10, category="科技创新"),
    ]

    plan = EditorialPlanner(cfg).base_plan(summaries)

    assert len(plan.hot_topics) == 1
    assert plan.hot_topics[0]["macro_topic_key"] == "small-topic"
    assert plan.hot_topics[0]["member_count"] == 2
    assert plan.focus_storylines == []
    # Standalones flow to the main lane (max_clusters=3, both fit), ranked by composite.
    assert [summary.cluster.topic_category for summary in plan.regular_summaries] == [
        "standalone high",
        "standalone low",
    ]


def test_base_plan_single_member_storyline_stays_in_main_lane():
    """Single-member storyline families do NOT get a tab — they are genuinely standalone."""
    cfg = _config(max_clusters=3)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "min_items_per_topic": 3,
        "tab_name_max_chars": 10,
    }
    summaries = [
        _storyline_summary("solo storyline", 0.95, "solo-topic", role="core"),
        _summary("standalone high", 0.80, category="商业财经"),
    ]

    plan = EditorialPlanner(cfg).base_plan(summaries)

    assert plan.hot_topics == []
    assert plan.focus_storylines == []
    assert [summary.cluster.topic_category for summary in plan.regular_summaries] == [
        "solo storyline",
        "standalone high",
    ]


def test_base_plan_caps_tabs_at_three_promoting_largest_families():
    """Additional fix #1: at most 3 storyline tabs per day. When 4 families
    are eligible, only the 3 largest claim a tab; the smallest flows to the
    main lane (its members carry the shared-storyline label there).
    """
    cfg = _config(max_clusters=10)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "min_items_per_topic": 3,
        "tab_name_max_chars": 10,
    }
    summaries = [
        # 4 families, sizes 4 / 3 / 3 / 2 — the size-2 one should lose.
        _storyline_summary("A1", 0.95, "topic-A", role="core"),
        _storyline_summary("A2", 0.90, "topic-A"),
        _storyline_summary("A3", 0.85, "topic-A"),
        _storyline_summary("A4", 0.80, "topic-A"),
        _storyline_summary("B1", 0.75, "topic-B", role="core"),
        _storyline_summary("B2", 0.70, "topic-B"),
        _storyline_summary("B3", 0.65, "topic-B"),
        _storyline_summary("C1", 0.60, "topic-C", role="core"),
        _storyline_summary("C2", 0.55, "topic-C"),
        _storyline_summary("C3", 0.50, "topic-C"),
        _storyline_summary("D1", 0.45, "topic-D", role="core"),
        _storyline_summary("D2", 0.40, "topic-D"),
    ]

    plan = EditorialPlanner(cfg).base_plan(summaries)

    tab_keys = {ht["macro_topic_key"] for ht in plan.hot_topics}
    assert len(plan.hot_topics) == 3
    # The size-4 family wins, two size-3 families tie, size-2 loses.
    assert "topic-A" in tab_keys
    assert "topic-D" not in tab_keys
    # The demoted family's members flow to the main lane.
    main_titles = [s.cluster.topic_category for s in plan.regular_summaries]
    assert "D1" in main_titles
    assert "D2" in main_titles


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


def test_display_dedup_preserves_within_family_members():
    """Additional fix #2: two members of the SAME storyline family are never
    deduped against each other — even if they're near-identical embeddings.

    The storyline resolver grouped them intentionally (different daily
    incidents of one conflict, different angles of one event). Collapsing
    them produced 1-member tabs (the 7/22 AndyBurnham incident: 3 family
    members collapsed to 1 by display dedup).
    """
    # Two near-identical summaries that WOULD merge if they were in regular.
    left = _storyline_summary("UK PM event A", 0.9, "burnham", role="core")
    right = _storyline_summary("UK PM event B", 0.85, "burnham")
    # Patch their embeddings to be near-identical so the dedup check would
    # trigger if it ran.
    left.cluster.articles[0].embedding = [1.0, 0.0, 0.0]
    right.cluster.articles[0].embedding = [0.99, 0.01, 0.0]
    family = {
        "macro_topic_key": "burnham",
        "storyline_key": "burnham",
        "summaries": [left, right],
    }
    hot, _f, regular, _p = resolve_display_duplicates([family], [], [], [])
    assert len(hot) == 1
    # Both members preserved — the family did not collapse to 1.
    assert hot[0]["member_count"] == 2
    assert regular == []


def test_display_dedup_preserves_same_storyline_members_in_main_lane():
    """2026-09-13: two clusters the resolver put in ONE storyline were merged
    at 0.78 because that family never claimed a hot-topic tab.

    ``resolve_display_duplicates`` only built its family index from hot_topic
    and focus families, so main-lane members of a storyline got no protection.
    """
    left = _storyline_summary("US Navy on ROK nuclear submarine", 0.77, "single-e8b64fc9", role="core")
    right = _storyline_summary("North Korea fires ballistic missiles", 0.60, "single-e8b64fc9")
    left.cluster.articles[0].embedding = [1.0, 0.0, 0.0]
    right.cluster.articles[0].embedding = [0.78, 0.6258, 0.0]

    _h, _f, regular, _p = resolve_display_duplicates([], [], [left, right], [])

    assert len(regular) == 2


def test_display_dedup_still_merges_same_event_across_storylines():
    """The family guard must not stop legitimate cross-family dedup."""
    left = _summary("Houthis seize Bab al-Mandeb island", 0.75)
    right = _summary("Yemen Houthis capture strategic island", 0.70)
    left.cluster.articles[0].embedding = [1.0, 0.0, 0.0]
    right.cluster.articles[0].embedding = [0.81, 0.5864, 0.0]

    _h, _f, regular, _p = resolve_display_duplicates([], [], [left, right], [])

    assert len(regular) == 1


def test_display_dedup_always_merges_shared_url_inside_storyline():
    shared = "https://wire.example/wildfire"
    left = _storyline_summary("France wildfire evacuation", 0.9, "wildfires", role="core")
    right = _storyline_summary("Spain and France wildfire update", 0.8, "wildfires")
    left.cluster.articles[0].url = shared
    right.cluster.articles[0].url = shared
    family = {
        "macro_topic_key": "wildfires",
        "storyline_key": "wildfires",
        "summaries": [left, right],
    }

    hot, _f, _r, _p = resolve_display_duplicates([family], [], [], [])

    assert hot[0]["member_count"] == 1


def test_display_dedup_merges_high_confidence_multisource_event_inside_storyline():
    left = _storyline_summary("France wildfire evacuation", 0.9, "wildfires", role="core")
    right = _storyline_summary("France wildfire evacuation update", 0.8, "wildfires")
    for summary, suffix in ((left, "left"), (right, "right")):
        summary.cluster.articles.extend([
            _article(f"BBC {suffix}", url=f"https://bbc.example/{suffix}", embedding=[1.0, 0.0, 0.0]),
            _article(f"Guardian {suffix}", url=f"https://guardian.example/{suffix}", embedding=[1.0, 0.0, 0.0]),
        ])
        summary.cluster.articles[-2].source_name = "BBC"
        summary.cluster.articles[-1].source_name = "Guardian"
        summary.cluster.sources = ["Reuters", "BBC", "Guardian"]
        summary.cluster.articles[0].embedding = [1.0, 0.0, 0.0]
    family = {
        "macro_topic_key": "wildfires",
        "storyline_key": "wildfires",
        "summaries": [left, right],
    }

    hot, _f, _r, _p = resolve_display_duplicates([family], [], [], [])

    assert hot[0]["member_count"] == 1


def test_perspectives_are_canonicalized_after_duplicate_summary_merge():
    shared = "https://wire.example/quake"
    left = _summary("Japan earthquake", 0.9, url=shared, embedding=[1.0, 0.0])
    right = _summary("Japan quake update", 0.8, url=shared, embedding=[1.0, 0.0])
    left.grouped_perspectives = [
        PerspectiveGroup(["Reuters"], "报道地震灾情及伤亡情况，关注救援进展和余震风险。")
    ]
    right.grouped_perspectives = [
        PerspectiveGroup(["BBC"], "报道地震伤亡及救援进展，关注余震风险。")
    ]

    _h, _f, regular, _p = resolve_display_duplicates([], [], [left, right], [])

    assert len(regular) == 1
    assert len(regular[0].grouped_perspectives) == 1
    assert regular[0].grouped_perspectives[0].sources == ["Reuters", "BBC"]


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


def test_display_dedup_merges_cpi_event_pair_below_old_075_bar():
    """08-13 regression: the US-CPI data-release cluster (zh/kr/ja) and the
    US-CPI market-reaction cluster (zh/kr) were the same event with centroid
    cosine 0.7477 — just below the old 0.75 bar — and both rendered in the
    main lane. cos≈0.748 must now merge under the 0.73 bar."""
    data_release = _summary("美7月 CPI 3.4% 放缓", 0.6, url="https://cn.example/cpi-data",
                            embedding=[0.748, 0.6636, 0.0])
    market_reaction = _summary("US CPI and Fed rate expectations", 0.6, url="https://en.example/cpi-market",
                               embedding=[1.0, 0.0, 0.0])
    _h, _f, regular, _p = resolve_display_duplicates([], [], [data_release, market_reaction], [])
    assert len(regular) == 1
    assert regular[0].cluster.topic_category == "美7月 CPI 3.4% 放缓"


def test_display_dedup_keeps_related_but_distinct_at_064():
    """A 0.64 centroid pair — the documented related-but-distinct storyline
    range — must stay separate under the lowered 0.73 bar."""
    left = _summary("event A", 0.8, url="https://a.com/1", embedding=[0.64, 0.768, 0.0])
    right = _summary("event B", 0.6, url="https://b.com/2", embedding=[1.0, 0.0, 0.0])
    _h, _f, regular, _p = resolve_display_duplicates([], [], [left, right], [])
    assert len(regular) == 2


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


def test_base_plan_keeps_full_storyline_name_for_body_headers():
    """Navigation tabs stay capped at tab_name_max_chars; the full-length
    family label is carried for in-body headers (overview + topic stage)."""
    cfg = _config(max_clusters=3)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "min_items_per_topic": 3,
        "tab_name_max_chars": 10,
    }
    long_name = "欧盟宣布扩大实施人工智能法透明度新规"
    summaries = [
        _storyline_summary("small group core", 0.95, "small-topic", role="core"),
        _storyline_summary("small group follow", 0.90, "small-topic"),
    ]
    for summary in summaries:
        summary.cluster.storyline_name = long_name
        summary.cluster.macro_topic_name = long_name

    plan = EditorialPlanner(cfg).base_plan(summaries)

    assert len(plan.hot_topics) == 1
    tab = plan.hot_topics[0]
    assert tab["macro_topic_name"] == "欧盟宣布扩大实施人工"  # capped at 10
    assert tab["macro_topic_name_full"] == long_name            # full for body
    assert tab["storyline_name_full"] == long_name


def test_select_report_clusters_caps_each_hot_topic_at_ten():
    cfg = _config()
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "max_items_per_topic": 10,
        "main_lane_target": 15,
        "tab_name_max_chars": 10,
    }
    clusters = []
    for i in range(12):
        cluster = _cluster(f"hot{i}", 0.95 - i * 0.01)
        cluster.storyline_key = "war"
        cluster.storyline_name = "战争专题"
        cluster.storyline_role = "core" if i == 0 else "spillover"
        clusters.append(cluster)

    hot, main = select_report_clusters(clusters, cfg)

    assert len(hot) == 10
    assert [c.topic_category for c in hot] == [f"hot{i}" for i in range(10)]
    assert main == []


def test_base_plan_caps_each_hot_topic_at_ten_after_freshness():
    cfg = _config(max_clusters=20)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "max_items_per_topic": 10,
        "main_lane_target": 15,
        "tab_name_max_chars": 10,
    }
    summaries = []
    for i in range(12):
        summaries.append(
            _storyline_summary(f"hot{i}", 0.95 - i * 0.01, "war", role="core" if i == 0 else "spillover")
        )

    plan = EditorialPlanner(cfg).base_plan(summaries)

    assert len(plan.hot_topics) == 1
    assert plan.hot_topics[0]["member_count"] == 10
    assert [s.cluster.topic_category for s in plan.hot_topics[0]["summaries"]] == [
        f"hot{i}" for i in range(10)
    ]
    assert plan.regular_summaries == []


def test_base_plan_limits_main_lane_to_target_15():
    cfg = _config(max_clusters=20)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "max_items_per_topic": 10,
        "main_lane_target": 15,
        "tab_name_max_chars": 10,
    }
    summaries = [_summary(f"main{i}", 0.9 - i * 0.01, category="国际时政") for i in range(20)]

    plan = EditorialPlanner(cfg).base_plan(summaries)

    assert len(plan.regular_summaries) == 15
    assert [s.cluster.topic_category for s in plan.regular_summaries] == [
        f"main{i}" for i in range(15)
    ]


def test_hot_topic_stories_do_not_consume_main_lane_budget():
    cfg = _config(max_clusters=20)
    cfg.output["hot_topics"] = {
        "enabled": True,
        "max_topic_tabs": 3,
        "max_items_per_topic": 10,
        "main_lane_target": 15,
        "tab_name_max_chars": 10,
    }
    summaries = [
        _storyline_summary("hot core", 0.99, "war", role="core"),
        _storyline_summary("hot follow 1", 0.98, "war"),
        _storyline_summary("hot follow 2", 0.97, "war"),
    ]
    summaries += [_summary(f"main{i}", 0.9 - i * 0.01, category="国际时政") for i in range(20)]

    plan = EditorialPlanner(cfg).base_plan(summaries)

    assert len(plan.hot_topics) == 1
    assert plan.hot_topics[0]["member_count"] == 3
    assert len(plan.regular_summaries) == 15
    assert [s.cluster.topic_category for s in plan.regular_summaries] == [
        f"main{i}" for i in range(15)
    ]


def _accounting_line(caplog) -> str:
    accounting = [message for message in caplog.messages if "Display dedup accounting:" in message]
    assert len(accounting) == 1, accounting
    return accounting[0]


def test_display_dedup_logs_funnel_accounting(caplog):
    left = _storyline_summary("event A", 0.9, "family-a", role="core")
    right = _storyline_summary("event B", 0.5, "family-a")
    left.cluster.articles[0].embedding = [1.0, 0.0, 0.0]
    right.cluster.articles[0].embedding = [0.99, 0.01, 0.0]
    family = {"macro_topic_key": "family-a", "storyline_key": "family-a", "summaries": [left, right]}

    with caplog.at_level(logging.INFO):
        resolve_display_duplicates([family], [], [], [])

    # The full breakdown is asserted, not just the totals: `kept` must equal the
    # sum of its components, or the line reports a phantom loss.
    assert _accounting_line(caplog) == (
        "Display dedup accounting: displayed=2 suppressed=0 kept=2 "
        "(hot_topic_members=2, focus_members=0, main=0, positive=0)"
    )


def test_display_dedup_accounting_counts_focus_members(caplog):
    """A focus-storyline member is displayed AND kept, so it must be counted.

    ``displayed`` is built from hot_topic *and* focus families, so any breakdown
    that omits focus members under-reports ``kept`` -- the exact phantom-loss
    signature this accounting exists to expose.
    """
    focus = _storyline_summary("focus A", 0.7, "focus-family", role="core")
    family = {"macro_topic_key": "focus-family", "storyline_key": "focus-family", "summaries": [focus]}

    with caplog.at_level(logging.INFO):
        _hot, focus_out, _regular, _positive = resolve_display_duplicates([], [family], [], [])

    assert _accounting_line(caplog) == (
        "Display dedup accounting: displayed=1 suppressed=0 kept=1 "
        "(hot_topic_members=0, focus_members=1, main=0, positive=0)"
    )
    assert sum(len(f["summaries"]) for f in focus_out) == 1


def test_display_dedup_accounting_counts_positive_lane(caplog):
    positive_story = _summary("good news", 0.6)

    with caplog.at_level(logging.INFO):
        resolve_display_duplicates([], [], [], [positive_story])

    assert _accounting_line(caplog) == (
        "Display dedup accounting: displayed=1 suppressed=0 kept=1 "
        "(hot_topic_members=0, focus_members=0, main=0, positive=1)"
    )


def test_display_dedup_accounting_counts_suppressions(caplog):
    """A suppressed duplicate must show up in both suppressed and kept."""
    shared = "https://wire.example/same-event"
    kept = _summary("event one", 0.9, url=shared)
    dropped = _summary("event one update", 0.8, url=shared)

    with caplog.at_level(logging.INFO):
        _hot, _focus, regular, _positive = resolve_display_duplicates([], [], [kept, dropped], [])

    assert _accounting_line(caplog) == (
        "Display dedup accounting: displayed=2 suppressed=1 kept=1 "
        "(hot_topic_members=0, focus_members=0, main=1, positive=0)"
    )
    assert regular == [kept]


def test_display_dedup_still_merges_same_event_across_different_storylines():
    """Production shape: the 2026-09-13 Bab al-Mandeb pair (0.81) must still merge.

    The storyline guard protects members of ONE family. Two clusters in
    DIFFERENT families are still ordinary display duplicates, and the unkeyed
    guard could not see that over-application risk -- this pins it with the real
    production keys.
    """
    left = _storyline_summary(
        "Houthis seize Bab al-Mandeb island", 0.75, "single-4a6e4f6b", role="none"
    )
    right = _storyline_summary(
        "Yemen Houthis capture strategic island", 0.70, "single-a1009515", role="none"
    )
    left.cluster.articles[0].embedding = [1.0, 0.0, 0.0]
    right.cluster.articles[0].embedding = [0.81, 0.5864, 0.0]

    _hot, _focus, regular, _positive = resolve_display_duplicates([], [], [left, right], [])

    assert len(regular) == 1
