from __future__ import annotations

from datetime import datetime, timezone

from newsprism.config import Config, SourceConfig
from newsprism.service.editorial_planner import EditorialPlanner, _body_only_text
from newsprism.types import Article, ArticleCluster, ClusterSummary


def _cfg() -> Config:
    return Config(
        raw={},
        sources=[
            SourceConfig("Reuters", "Reuters", "https://reuters.com", None, "rss", 1.0, "en", region="us"),
        ],
        topics={},
        schedule={},
        collection={},
        filter={"positive_energy_pre_filter": {"topic": "Positive Energy"}},
        clustering={"max_clusters_per_report": 10},
        dedup={},
        summarizer={},
        output={
            "hot_topics": {
                "enabled": True,
                "min_items_per_topic": 5,
                "max_topic_tabs": 3,
                "tab_name_max_chars": 10,
                "icon_allowlist": ["globe", "war", "trade", "chip", "ai", "energy"],
            },
            "positive_energy": {"enabled": True, "max_items": 5},
        },
        active_search={},
        topic_equivalence={},
    )


def _summary(title: str, key: str | None = None, role: str = "none") -> ClusterSummary:
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url=f"https://example.com/{title.replace(' ', '-')}",
                title=title,
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content=f"{title} body.",
            )
        ],
    )
    cluster.storyline_key = key
    cluster.macro_topic_key = key
    cluster.storyline_name = "伊朗局势" if key else None
    cluster.macro_topic_name = "伊朗局势" if key else None
    cluster.storyline_role = role
    cluster.storyline_membership_status = role if role in {"core", "spillover"} else "none"
    return ClusterSummary(cluster=cluster, summary=f"**{title}**\n\nBody.")


def test_editorial_planner_folds_two_story_family_out_of_main_feed():
    planner = EditorialPlanner(_cfg())
    first = _summary("Iran conflict update", key="iran", role="core")
    second = _summary("Hormuz shipping disruption", key="iran", role="spillover")
    standalone = _summary("SpaceX IPO update")

    plan = planner.plan([first, second, standalone], local_positive_summaries=[])

    assert len(plan.focus_storylines) == 1
    assert plan.focus_storylines[0]["storyline_key"] == "iran"
    assert plan.regular_summaries == [standalone]
    assert plan.hot_topics == []


def test_editorial_planner_finalizes_existing_base_plan():
    planner = EditorialPlanner(_cfg())
    first = _summary("Iran conflict update", key="iran", role="core")
    second = _summary("Hormuz shipping disruption", key="iran", role="spillover")
    standalone = _summary("SpaceX IPO update")

    base_plan = planner.base_plan([first, second, standalone])
    plan = planner.finalize(base_plan, positive_summaries=[])

    assert len(base_plan.focus_storylines) == 1
    assert base_plan.regular_summaries == [standalone]
    assert plan.focus_storylines == base_plan.focus_storylines
    assert plan.regular_summaries == [standalone]
    assert plan.positive_summaries == []


def test_body_only_text_strips_headline_and_perspective_bullets():
    text = """

**鲸鱼幼崽获救**

救援人员将鲸鱼幼崽带回深水区。
• 【Reuters】关注救援行动。
- 【BBC】关注当地志愿者。
* 【AP】关注海洋保护。

"""

    assert _body_only_text(text) == "救援人员将鲸鱼幼崽带回深水区。"
