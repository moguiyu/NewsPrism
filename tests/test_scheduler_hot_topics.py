from datetime import datetime, timedelta, timezone

from newsprism.config import Config, SourceConfig
from newsprism.runtime.scheduler import (
    positive_energy_classification_pool,
    resolve_display_duplicates,
    select_hot_topic_families,
    select_positive_energy_summaries,
    select_report_clusters,
    split_positive_energy_lane,
)
from newsprism.types import Article, ArticleCluster, ClusterSummary, PerspectiveGroup


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


def test_select_hot_topic_families_emits_generic_storyline_metadata():
    cfg = _config(main_limit=2)
    hot_clusters = [
        _cluster("Election dispute escalates", role="core", storyline_key="election-crisis", storyline_name="选举危机"),
        _cluster("Markets react to election dispute", role="spillover", storyline_key="election-crisis", storyline_name="选举危机"),
        _cluster("Cabinet talks delayed", role="spillover", storyline_key="election-crisis", storyline_name="选举危机"),
        _cluster("Transport closures expand", role="spillover", storyline_key="election-crisis", storyline_name="选举危机"),
        _cluster("Foreign governments respond", role="spillover", storyline_key="election-crisis", storyline_name="选举危机"),
    ]
    for cluster in hot_clusters:
        cluster.is_hot_topic = True
        cluster.macro_topic_member_count = 5
        cluster.macro_topic_icon_key = "globe"

    summaries = [_summary(cluster, cluster.articles[0].title) for cluster in hot_clusters]
    summaries.append(_summary(_cluster("Regular story"), "Regular story"))

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(summaries, cfg)

    assert len(hot_topics) == 1
    assert focus_storylines == []
    assert hot_topics[0]["storyline_key"] == "election-crisis"
    assert hot_topics[0]["storyline_name"] == "选举危机"
    assert hot_topics[0]["macro_topic_name"] == "选举危机"
    assert hot_topics[0]["member_count"] == 5
    assert len(hot_topics[0]["summaries"]) == 5
    assert len(main_summaries) == 1


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


def test_select_hot_topic_families_deduplicates_same_event_in_main_lane():
    cfg = _config(main_limit=5)

    articles_primary = [
        _article("Reuters", "Trump China trip delayed"),
        _article("BBC", "Trump China trip delayed"),
    ]
    articles_duplicate = [
        _article("Reuters", "Trump China trip delayed"),
        _article("BBC", "Trump China trip delayed"),
    ]
    primary_cluster = ArticleCluster(topic_category="World News", articles=articles_primary)
    duplicate_cluster = ArticleCluster(topic_category="World News", articles=articles_duplicate)

    primary_summary = ClusterSummary(
        cluster=primary_cluster,
        summary="**特朗普访华安排确认**\n\n会晤时间基本敲定。",
        perspectives={
            "Reuters": "聚焦访华时间表与白宫日程安排。",
            "BBC": "聚焦双边会晤对关系稳定的信号意义。",
        },
        grouped_perspectives=[
            PerspectiveGroup(sources=["Reuters"], perspective="聚焦访华时间表与白宫日程安排。"),
            PerspectiveGroup(sources=["BBC"], perspective="聚焦双边会晤对关系稳定的信号意义。"),
        ],
    )
    duplicate_summary = ClusterSummary(
        cluster=duplicate_cluster,
        summary="**美中首脑会晤延期后改期**\n\n白宫与各方同步新的访问时点。",
        perspectives={
            "Reuters": "聚焦访华时间表与白宫日程安排。",
            "BBC": "聚焦双边会晤对关系稳定的信号意义。",
        },
        grouped_perspectives=[
            PerspectiveGroup(sources=["Reuters"], perspective="聚焦访华时间表与白宫日程安排。"),
            PerspectiveGroup(sources=["BBC"], perspective="聚焦双边会晤对关系稳定的信号意义。"),
        ],
    )
    other_summary = _summary(_cluster("Unrelated market move"), "Unrelated market move")

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(
        [primary_summary, duplicate_summary, other_summary],
        cfg,
    )

    assert hot_topics == []
    assert focus_storylines == []
    assert [summary.summary for summary in main_summaries] == [
        primary_summary.summary,
        other_summary.summary,
    ]


def test_select_hot_topic_families_keeps_distinct_main_lane_stories_with_shared_anchor_label():
    cfg = _config(main_limit=5)

    cluster_a = _cluster(
        "Trump threatens Iran",
        role="none",
        storyline_key="iran-talks",
        storyline_name="特朗普再次就停战谈判",
        membership_status="none",
        anchor_labels=["特朗普再次就停战谈判"],
    )
    cluster_b = _cluster(
        "US poll on Iran action",
        role="none",
        storyline_key="iran-talks",
        storyline_name="特朗普再次就停战谈判",
        membership_status="none",
        anchor_labels=["特朗普再次就停战谈判"],
    )

    summary_a = ClusterSummary(
        cluster=cluster_a,
        summary="**特朗普威胁伊朗并坚称其渴望谈判**\n\n停战言论继续升级。",
        perspectives={
            "Reuters": "聚焦特朗普施压与停战谈判时限。",
            "BBC": "聚焦谈判窗口与地区局势风险。",
        },
        grouped_perspectives=[
            PerspectiveGroup(sources=["Reuters"], perspective="聚焦特朗普施压与停战谈判时限。"),
            PerspectiveGroup(sources=["BBC"], perspective="聚焦谈判窗口与地区局势风险。"),
        ],
        storyline_key=cluster_a.storyline_key,
        storyline_name=cluster_a.storyline_name,
        storyline_role=cluster_a.storyline_role,
        macro_topic_key=cluster_a.macro_topic_key,
        macro_topic_name=cluster_a.macro_topic_name,
    )
    summary_b = ClusterSummary(
        cluster=cluster_b,
        summary="**美民调显示近六成美国人认为对伊朗军事行动过头**\n\n国内政治压力正在上升。",
        perspectives={
            "Reuters": "聚焦民调本身与选民态度。",
            "BBC": "聚焦政治后果与汽油价格担忧。",
        },
        grouped_perspectives=[
            PerspectiveGroup(sources=["Reuters"], perspective="聚焦民调本身与选民态度。"),
            PerspectiveGroup(sources=["BBC"], perspective="聚焦政治后果与汽油价格担忧。"),
        ],
        storyline_key=cluster_b.storyline_key,
        storyline_name=cluster_b.storyline_name,
        storyline_role=cluster_b.storyline_role,
        macro_topic_key=cluster_b.macro_topic_key,
        macro_topic_name=cluster_b.macro_topic_name,
    )

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(
        [summary_a, summary_b],
        cfg,
    )

    assert hot_topics == []
    assert focus_storylines == []
    assert [summary.summary for summary in main_summaries] == [
        summary_a.summary,
        summary_b.summary,
    ]


def test_display_duplicate_resolver_merges_trump_iran_china_visit_split():
    war_cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            _article("Yonhap", "Trump wants Iran war ended before mid-May China visit"),
            _article("AP News", "Trump delays Iran strike deadline as talks continue"),
        ],
    )
    visit_cluster = ArticleCluster(
        topic_category="Geopolitics",
        articles=[
            _article("Reuters", "Trump plans May visit to China after Iran war delay"),
            _article("澎湃新闻", "特朗普将于5月中旬访华，中美领导人将举行会晤"),
        ],
    )
    war_summary = ClusterSummary(
        cluster=war_cluster,
        summary="**特朗普寻求数周内结束对伊战争并推迟霍尔木兹海峡打击期限**\n\n特朗普希望在5月中旬访华前结束与伊朗的战争，并推迟针对伊朗能源设施的打击期限。",
        perspectives={"Yonhap": "聚焦特朗普结束伊朗战争的时间表。"},
    )
    visit_summary = ClusterSummary(
        cluster=visit_cluster,
        summary="**特朗普将于5月中旬访华，中美领导人将举行会晤**\n\n白宫称特朗普计划在5月中旬访问中国，此前他推迟伊朗战争相关打击期限以争取谈判空间。",
        perspectives={
            "Reuters": "聚焦访华时间表与伊朗战争延后之间的外交关联。",
            "澎湃新闻": "聚焦中美会晤安排。",
        },
        grouped_perspectives=[
            PerspectiveGroup(sources=["Reuters"], perspective="聚焦访华时间表与伊朗战争延后之间的外交关联。"),
            PerspectiveGroup(sources=["澎湃新闻"], perspective="聚焦中美会晤安排。"),
        ],
    )

    hot_topics, focus_storylines, regular, positive = resolve_display_duplicates(
        [],
        [],
        [war_summary, visit_summary],
        [],
    )

    assert hot_topics == []
    assert focus_storylines == []
    assert positive == []
    assert regular == [visit_summary]
    assert visit_summary.duplicate_action == "merged"
    assert war_summary.duplicate_action == "suppressed"
    assert "Reuters" in visit_summary.cluster.sources
    assert "Yonhap" in visit_summary.cluster.sources
    assert visit_summary.event_signature["contexts"] == ["iran-war", "trump-china-visit"]


def test_display_duplicate_resolver_merges_cross_lane_duplicate():
    focus_summary = ClusterSummary(
        cluster=ArticleCluster(
            topic_category="Space",
            articles=[_article("The Verge", "NASA cancels lunar Gateway to fund moon base")],
        ),
        summary="**NASA 取消月球轨道站计划，转向建设200亿美元月球基地**\n\nNASA 将资金从月球轨道站转向月球基地建设。",
        perspectives={"The Verge": "聚焦项目调整。"},
    )
    regular_summary = ClusterSummary(
        cluster=ArticleCluster(
            topic_category="Space",
            articles=[_article("Reuters", "NASA pauses lunar space station and backs $20 billion moon base")],
        ),
        summary="**NASA 宣布暂停月球轨道空间站计划，转向投资200亿美元建设月球基地**\n\nNASA 暂停月球轨道空间站计划，转向投资月球基地。",
        perspectives={
            "Reuters": "聚焦预算转向。",
            "BBC": "聚焦登月计划影响。",
        },
        grouped_perspectives=[
            PerspectiveGroup(sources=["Reuters"], perspective="聚焦预算转向。"),
            PerspectiveGroup(sources=["BBC"], perspective="聚焦登月计划影响。"),
        ],
    )
    focus_storylines = [{"storyline_key": "nasa-moon", "member_count": 1, "summaries": [focus_summary]}]

    hot_topics, focus_storylines, regular, positive = resolve_display_duplicates(
        [],
        focus_storylines,
        [regular_summary],
        [],
    )

    assert hot_topics == []
    assert focus_storylines == []
    assert positive == []
    assert regular == [regular_summary]
    assert regular_summary.duplicate_action == "merged"
    assert focus_summary.duplicate_action == "suppressed"


def test_display_duplicate_resolver_keeps_same_company_different_actions():
    wwdc = ClusterSummary(
        cluster=ArticleCluster(
            topic_category="Smartphones & Electronics",
            articles=[_article("The Verge", "Apple announces WWDC26 dates")],
        ),
        summary="**苹果 WWDC26 定档 6 月 8 日，将聚焦 AI 进展与 iOS 27**\n\n苹果宣布开发者大会日期，并预告系统更新。",
        perspectives={"The Verge": "聚焦开发者大会。"},
    )
    maps = ClusterSummary(
        cluster=ArticleCluster(
            topic_category="Tech Companies - International",
            articles=[_article("TechCrunch", "Apple Maps will add search ads")],
        ),
        summary="**苹果地图将推出搜索广告并上线网页版以挑战谷歌**\n\n苹果计划扩大地图商业化。",
        perspectives={"TechCrunch": "聚焦广告业务。"},
    )

    _hot_topics, _focus_storylines, regular, _positive = resolve_display_duplicates([], [], [wwdc, maps], [])

    assert regular == [wwdc, maps]
    assert wwdc.duplicate_action == "kept"
    assert maps.duplicate_action == "kept"


def test_selection_score_prefers_distinct_perspectives_over_same_angle_pileup():
    cfg = _config(main_limit=2)
    pileup_cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            _article("Reuters", "Same fact confirmed"),
            _article("BBC", "Same fact confirmed"),
            _article("AP", "Same fact confirmed"),
            _article("Guardian", "Same fact confirmed"),
        ],
    )
    rich_cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            _article("Reuters", "Policy dispute deepens"),
            _article("BBC", "Policy dispute deepens"),
        ],
    )
    pileup = ClusterSummary(
        cluster=pileup_cluster,
        summary="**Same fact confirmed**\n\nBody.",
        perspectives={source: "Same angle." for source in pileup_cluster.sources},
        grouped_perspectives=[PerspectiveGroup(sources=pileup_cluster.sources, perspective="Same angle.")],
    )
    rich = ClusterSummary(
        cluster=rich_cluster,
        summary="**Policy dispute deepens**\n\nBody.",
        perspectives={"Reuters": "Official angle.", "BBC": "Public impact angle."},
        grouped_perspectives=[
            PerspectiveGroup(sources=["Reuters"], perspective="Official angle."),
            PerspectiveGroup(sources=["BBC"], perspective="Public impact angle."),
        ],
    )

    _hot_topics, _focus_storylines, main_summaries = select_hot_topic_families([pileup, rich], cfg)

    assert main_summaries[0] == rich
    assert rich.selection_score > pileup.selection_score


def test_selection_balance_reserves_slot_for_qualified_tech_story():
    cfg = _config(main_limit=5)
    geopolitical = []
    for index in range(5):
        cluster = ArticleCluster(
            topic_category="World News",
            articles=[
                _article("Reuters", f"World story {index} A"),
                _article("BBC", f"World story {index} B"),
            ],
        )
        geopolitical.append(
            ClusterSummary(
                cluster=cluster,
                summary=f"**World story {index}**\n\nBody.",
                perspectives={"Reuters": "Official angle.", "BBC": "Public angle."},
                grouped_perspectives=[
                    PerspectiveGroup(sources=["Reuters"], perspective="Official angle."),
                    PerspectiveGroup(sources=["BBC"], perspective="Public angle."),
                ],
            )
        )
    tech_cluster = ArticleCluster(
        topic_category="AI & LLM",
        articles=[_article("TechCrunch", "AI model launch")],
    )
    tech = ClusterSummary(
        cluster=tech_cluster,
        summary="**AI model launch**\n\nBody.",
        perspectives={"TechCrunch": "Product angle."},
        grouped_perspectives=[PerspectiveGroup(sources=["TechCrunch"], perspective="Product angle.")],
    )

    _hot_topics, _focus_storylines, main_summaries = select_hot_topic_families([*geopolitical, tech], cfg)

    assert tech in main_summaries
    assert len(main_summaries) == 5


def test_select_hot_topic_families_suppresses_incoherent_hotspot_family():
    cfg = _config(main_limit=6)
    clusters = [
        _cluster(
            "Iran strike update 1",
            role="core",
            membership_status="core",
            storyline_key="broad-war-family",
            storyline_name="伊朗以色列冲突升级",
            anchor_labels=["伊朗以色列冲突"],
        ),
        _cluster(
            "Iran strike update 2",
            role="core",
            membership_status="core",
            storyline_key="broad-war-family",
            storyline_name="伊朗以色列冲突升级",
            anchor_labels=["伊朗以色列冲突"],
        ),
        _cluster(
            "Trump China visit delay 1",
            role="core",
            membership_status="core",
            storyline_key="broad-war-family",
            storyline_name="伊朗以色列冲突升级",
            anchor_labels=["特朗普访华行程"],
        ),
        _cluster(
            "Trump China visit delay 2",
            role="core",
            membership_status="core",
            storyline_key="broad-war-family",
            storyline_name="伊朗以色列冲突升级",
            anchor_labels=["特朗普访华行程"],
        ),
        _cluster(
            "Ukraine security talks",
            role="core",
            membership_status="core",
            storyline_key="broad-war-family",
            storyline_name="伊朗以色列冲突升级",
            anchor_labels=["乌克兰安全谈判"],
        ),
    ]
    for cluster in clusters:
        cluster.is_hot_topic = True
        cluster.macro_topic_member_count = 5

    summaries = [_summary(cluster, cluster.articles[0].title) for cluster in clusters]

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(summaries, cfg)

    assert hot_topics == []
    assert len(focus_storylines) == 1
    assert focus_storylines[0]["member_count"] == 2
    assert len(main_summaries) == 2
    assert all(summary.is_hot_topic is False for summary in main_summaries)
    assert all(summary.cluster.is_hot_topic is False for summary in main_summaries)


def test_select_hot_topic_families_keeps_largest_coherent_component_and_renames_it():
    cfg = _config(main_limit=6)
    coherent_clusters = [
        _cluster(
            f"Iran strike update {index}",
            role="core" if index == 1 else "spillover",
            membership_status="core" if index == 1 else "spillover",
            storyline_key="broad-war-family",
            storyline_name="伊朗以色列冲突升级",
            anchor_labels=["以伊冲突主线"],
        )
        for index in range(1, 6)
    ]
    stray_cluster = _cluster(
        "Trump China visit delay",
        role="spillover",
        membership_status="spillover",
        storyline_key="broad-war-family",
        storyline_name="伊朗以色列冲突升级",
        anchor_labels=["特朗普访华行程"],
    )
    all_clusters = coherent_clusters + [stray_cluster]
    for cluster in all_clusters:
        cluster.is_hot_topic = True
        cluster.macro_topic_member_count = len(all_clusters)

    summaries = [_summary(cluster, cluster.articles[0].title) for cluster in all_clusters]

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(summaries, cfg)

    assert len(hot_topics) == 1
    assert focus_storylines == []
    assert hot_topics[0]["member_count"] == 5
    assert hot_topics[0]["macro_topic_name"] == "以伊冲突主线"
    assert [summary.cluster.articles[0].title for summary in hot_topics[0]["summaries"]] == [
        cluster.articles[0].title for cluster in coherent_clusters
    ]
    assert [summary.cluster.articles[0].title for summary in main_summaries] == ["Trump China visit delay"]
    assert main_summaries[0].is_hot_topic is False
    assert main_summaries[0].cluster.is_hot_topic is False


def test_select_hot_topic_families_builds_focus_storyline_for_subthreshold_family():
    cfg = _config(main_limit=6)
    focus_clusters = [
        _cluster(
            f"Trump China visit {index}",
            role="core" if index == 1 else "spillover",
            membership_status="core" if index == 1 else "spillover",
            storyline_key="trump-china-visit",
            storyline_name="特朗普访华主线",
            anchor_labels=["特朗普访华"],
        )
        for index in range(1, 4)
    ]
    other_cluster = _cluster("Other story")
    summaries = [_summary(cluster, cluster.articles[0].title) for cluster in focus_clusters + [other_cluster]]

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(summaries, cfg)

    assert hot_topics == []
    assert len(focus_storylines) == 1
    assert focus_storylines[0]["storyline_name"] == "特朗普访华"
    assert [summary.cluster.articles[0].title for summary in focus_storylines[0]["summaries"]] == [
        cluster.articles[0].title for cluster in focus_clusters
    ]
    assert [summary.cluster.articles[0].title for summary in main_summaries] == ["Other story"]


def test_select_hot_topic_families_prefers_short_topic_name_over_headline_like_family_label():
    cfg = _config(main_limit=6)
    clusters = [
        _cluster(
            f"Iran pressure update {index}",
            role="core" if index == 1 else "spillover",
            membership_status="core" if index == 1 else "spillover",
            storyline_key="iran-pressure",
            storyline_name="俄交通部：载有十万吨",
            anchor_labels=["俄交通部：载有十万吨"],
        )
        for index in range(1, 6)
    ]
    for cluster in clusters:
        cluster.is_hot_topic = True
        cluster.macro_topic_member_count = 5

    summaries = [_summary(cluster, cluster.articles[0].title) for cluster in clusters]
    for summary in summaries:
        summary.short_topic_name = "伊朗局势"

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(summaries, cfg)

    assert len(hot_topics) == 1
    assert focus_storylines == []
    assert main_summaries == []
    assert hot_topics[0]["macro_topic_name"] == "伊朗局势"
    assert hot_topics[0]["storyline_name"] == "伊朗局势"


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


def test_select_positive_energy_summaries_enforces_domain_diversity():
    cfg = _config(main_limit=6)
    cfg.output["positive_energy"] = {"enabled": True, "min_items": 3, "max_items": 3, "min_confidence": 0.55}
    summaries = [
        _positive_summary("A", "https://same.example/a", "A good story"),
        _positive_summary("B", "https://same.example/b", "B better story"),
        _positive_summary("C", "https://other.example/c", "C culture story"),
        _positive_summary("D", "https://third.example/d", "D science story"),
    ]
    classifications = [
        {"cluster_index": 1, "good_fit": True, "positive": True, "fun": False, "low_conflict": True, "confidence": 0.95, "reason": "好消息A"},
        {"cluster_index": 2, "good_fit": True, "positive": True, "fun": True, "low_conflict": True, "confidence": 0.9, "reason": "好消息B"},
        {"cluster_index": 3, "good_fit": True, "positive": True, "fun": False, "low_conflict": True, "confidence": 0.8, "reason": "文化轻松"},
        {"cluster_index": 4, "good_fit": True, "positive": False, "fun": True, "low_conflict": True, "confidence": 0.75, "reason": "科学有趣"},
    ]

    selected = select_positive_energy_summaries(summaries, classifications, cfg)

    assert [summary.cluster.articles[0].url for summary in selected] == [
        "https://same.example/b",
        "https://other.example/c",
        "https://third.example/d",
    ]
    assert selected[0].positive_energy_reason == "好消息B"

    regular = split_positive_energy_lane(summaries, selected)
    assert [summary.cluster.articles[0].url for summary in regular] == ["https://same.example/a"]


def test_select_positive_energy_summaries_omits_when_fewer_than_minimum_eligible():
    cfg = _config(main_limit=6)
    cfg.output["positive_energy"] = {"enabled": True, "min_items": 3, "max_items": 5, "min_confidence": 0.55}
    summaries = [
        _positive_summary("A", "https://a.example/1", "A good story"),
        _positive_summary("B", "https://b.example/1", "B conflict story"),
        _positive_summary("C", "https://c.example/1", "C market stress"),
    ]
    classifications = [
        {"cluster_index": 1, "good_fit": True, "positive": True, "fun": False, "low_conflict": True, "confidence": 0.92, "reason": "温暖"},
        {"cluster_index": 2, "good_fit": True, "positive": True, "fun": False, "low_conflict": False, "confidence": 0.92, "reason": "冲突"},
        {"cluster_index": 3, "good_fit": False, "positive": False, "fun": False, "low_conflict": True, "confidence": 0.92, "reason": "不轻松"},
    ]

    selected = select_positive_energy_summaries(summaries, classifications, cfg)
    assert selected == []
    assert split_positive_energy_lane(summaries, selected) == summaries


def test_select_positive_energy_summaries_rejects_weak_low_conflict_candidates():
    cfg = _config(main_limit=3)
    cfg.output["positive_energy"] = {"enabled": True, "min_items": 1, "max_items": 5, "min_confidence": 0.78}
    summaries = [
        _positive_summary("A", "https://a.example/1", "Neutral policy story"),
        _positive_summary("B", "https://b.example/1", "Whale calf delights watchers"),
    ]
    classifications = [
        {"cluster_index": 1, "good_fit": False, "positive": True, "fun": False, "low_conflict": True, "confidence": 0.95, "reason": "只是低冲突"},
        {"cluster_index": 2, "good_fit": True, "positive": True, "fun": True, "low_conflict": True, "confidence": 0.88, "reason": "可爱暖心"},
    ]

    selected = select_positive_energy_summaries(summaries, classifications, cfg)

    assert [summary.cluster.articles[0].url for summary in selected] == ["https://b.example/1"]


def test_select_positive_energy_summaries_blocks_procedural_rule_stories():
    cfg = _config(main_limit=3)
    cfg.output["positive_energy"] = {"enabled": True, "min_items": 1, "max_items": 5, "min_confidence": 0.78}
    summaries = [
        _positive_summary("A", "https://a.example/oscar", "AI-generated actors ineligible for Oscars"),
        _positive_summary("B", "https://b.example/whale", "Whale calf delights watchers"),
    ]
    classifications = [
        {"cluster_index": 1, "good_fit": True, "positive": True, "fun": False, "low_conflict": True, "confidence": 0.95, "reason": "保护创作"},
        {"cluster_index": 2, "good_fit": True, "positive": True, "fun": True, "low_conflict": True, "confidence": 0.88, "reason": "可爱暖心"},
    ]

    selected = select_positive_energy_summaries(summaries, classifications, cfg)

    assert [summary.cluster.articles[0].url for summary in selected] == ["https://b.example/whale"]


def test_select_positive_energy_summaries_allows_one_strong_story_by_default():
    cfg = _config(main_limit=3)
    cfg.output["positive_energy"] = {"enabled": True, "max_items": 5, "min_confidence": 0.78}
    summary = _positive_summary("A", "https://a.example/whale", "Whale calf swims with pod")
    classifications = [
        {"cluster_index": 1, "good_fit": True, "positive": True, "fun": True, "low_conflict": True, "confidence": 0.86, "reason": "小鲸可爱暖心"},
    ]

    selected = select_positive_energy_summaries([summary], classifications, cfg)

    assert selected == [summary]
    assert selected[0].positive_energy_reason == "小鲸可爱暖心"


def test_report_clusters_preserve_positive_energy_candidates_beyond_main_limit():
    cfg = _config(main_limit=1)
    cfg.filter["positive_energy_pre_filter"] = {"topic": "Positive Energy"}
    cfg.output["positive_energy"] = {"max_items": 5}
    regular = _cluster("Regular story")
    positive = _cluster("Whale calf story")
    positive.articles[0].topics = ["Positive Energy"]

    hot_clusters, main_clusters = select_report_clusters([regular, positive], cfg)

    assert hot_clusters == []
    assert main_clusters == [regular, positive]


def test_positive_energy_pool_preserves_candidate_clipped_by_main_lane_cap():
    cfg = _config(main_limit=2)
    cfg.filter["positive_energy_pre_filter"] = {"topic": "Positive Energy"}
    cfg.output["positive_energy"] = {"enabled": True, "max_items": 5, "min_confidence": 0.78}
    summaries = [
        _summary(_cluster("Regular story 1"), "Regular story 1"),
        _summary(_cluster("Regular story 2"), "Regular story 2"),
        _summary(_cluster("Whale calf delights watchers"), "Whale calf delights watchers"),
    ]
    summaries[2].cluster.articles[0].topics = ["Positive Energy"]

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(summaries, cfg)
    pool = positive_energy_classification_pool(summaries, main_summaries, hot_topics, focus_storylines, cfg)
    classifications = [
        {"cluster_index": 1, "good_fit": False, "positive": False, "fun": False, "low_conflict": True, "confidence": 0.1, "reason": "普通"},
        {"cluster_index": 2, "good_fit": False, "positive": False, "fun": False, "low_conflict": True, "confidence": 0.1, "reason": "普通"},
        {"cluster_index": 3, "good_fit": True, "positive": True, "fun": True, "low_conflict": True, "confidence": 0.88, "reason": "可爱暖心"},
    ]

    selected = select_positive_energy_summaries(pool, classifications, cfg)

    assert main_summaries == summaries[:2]
    assert pool == summaries
    assert selected == [summaries[2]]
    assert split_positive_energy_lane(main_summaries, selected) == main_summaries


def test_positive_energy_pool_includes_extra_after_hot_topic_and_regular_cap():
    cfg = _config(main_limit=20)
    cfg.filter["positive_energy_pre_filter"] = {"topic": "Positive Energy"}
    cfg.output["positive_energy"] = {"enabled": True, "max_items": 5, "min_confidence": 0.78}
    hot_clusters = [
        _cluster(f"Hot story {index}", role="core", storyline_key="hot-story", storyline_name="热点主线")
        for index in range(1, 6)
    ]
    for cluster in hot_clusters:
        cluster.is_hot_topic = True
        cluster.macro_topic_member_count = 5
    regular_summaries = [
        _summary(_cluster(f"Regular story {index}"), f"Regular story {index}")
        for index in range(1, 21)
    ]
    positive_extra = _summary(_cluster("Snooker comeback delights fans"), "Snooker comeback delights fans")
    positive_extra.cluster.topic_category = "Sports"
    positive_extra.cluster.articles[0].topics = ["Sports", "Positive Energy"]
    summaries = [_summary(cluster, cluster.articles[0].title) for cluster in hot_clusters]
    summaries.extend(regular_summaries)
    summaries.append(positive_extra)

    hot_topics, focus_storylines, main_summaries = select_hot_topic_families(summaries, cfg)
    pool = positive_energy_classification_pool(summaries, main_summaries, hot_topics, focus_storylines, cfg)

    assert len(hot_topics) == 1
    assert len(main_summaries) == 20
    assert positive_extra not in main_summaries
    assert pool == main_summaries + [positive_extra]
