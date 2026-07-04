"""Unit tests for the ownership gate (State Media Matrix cross-border 内政 filter)."""
from datetime import datetime, timezone

from newsprism.config import Config, SourceConfig
from newsprism.service.impact import ImpactAssessor, ImpactItem
from newsprism.types import Article, ArticleCluster


def _source(name: str, region: str, ownership: str, tier: str = "editorial") -> SourceConfig:
    return SourceConfig(
        name=name,
        name_en=name,
        url="https://example.com",
        rss_url=None,
        type="rss",
        weight=1.0,
        language="en",
        region=region,
        tier=tier,
        ownership=ownership,
        ownership_detail="",
    )


def _config(*sources: SourceConfig) -> Config:
    return Config(
        raw={},
        sources=list(sources),
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={"hot_topics": {"icon_allowlist": ["globe", "war", "ai"]}},
        active_search={},
        editorial_values={
            "impact": {
                "weights": {
                    "scope": 0.16, "severity": 0.16, "novelty": 0.12,
                    "actor_influence": 0.14, "decision_relevance": 0.18,
                    "feelgood": 0.0, "signal": 0.24,
                },
                "status": {
                    "suppress_floor": 0.18,
                    "review_floor": 0.34,
                    "single_source_severity_review": 6.0,
                },
            },
            "source_reliability": {
                "tier_scores": {
                    "editorial": 0.82,
                    "tech": 0.74,
                    "portal": 0.55,
                    "unknown": 0.45,
                }
            },
            "ownership": {
                "weight_multipliers": {
                    "private_constrained": 0.85,
                    "independent_private_low_evidence": 0.75,
                }
            },
        },
    )


def _article(source: str, title: str = "Test headline") -> Article:
    return Article(
        url=f"https://example.com/{source}/{title[:10]}",
        title=title,
        source_name=source,
        published_at=datetime.now(timezone.utc),
        content="Test content.",
    )


def _cluster(articles: list[Article]) -> ArticleCluster:
    return ArticleCluster(topic_category="Test", articles=articles)


def _impact_item(
    target_region: str | None = None,
    is_home_affairs: bool = False,
) -> ImpactItem:
    return ImpactItem(
        cluster_index=1,
        scope=7.0, severity=5.0, novelty=5.0,
        actor_influence=8.0, decision_relevance=7.0, feelgood=0.0,
        rationale="test",
        display_category="World",
        target_region=target_region,
        is_home_affairs=is_home_affairs,
    )


# ─── Core gate tests ─────────────────────────────────────────────────────


def test_state_controlled_block_foreign_naizheng_suppressed():
    """Sputnik (ru, state_controlled_block) on US elections -> suppressed."""
    assessor = ImpactAssessor(_config(
        _source("Sputnik", "ru", "state_controlled_block"),
    ))
    article = _article("Sputnik", "US midterm elections voter suppression claims")
    cluster = _cluster([article])
    item = _impact_item(target_region="us", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is True
    assert assessment.status == "suppress"  # all articles suppressed
    assert "ownership_suppressed_all" in assessment.flags
    assert assessment.gate_blocked == ["Sputnik"]
    assert assessment.gate_review == []


def test_state_controlled_block_own_naizheng_allowed():
    """Xinhua (cn, state_controlled_block) on China's 内政 -> allowed (own country)."""
    assessor = ImpactAssessor(_config(
        _source("Xinhua", "cn", "state_controlled_block"),
    ))
    article = _article("Xinhua", "China judicial reform progress")
    cluster = _cluster([article])
    item = _impact_item(target_region="cn", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "publishable"


def test_state_controlled_block_not_home_affairs_allowed():
    """Sputnik on US-China trade war -> not 内政 -> allowed."""
    assessor = ImpactAssessor(_config(
        _source("Sputnik", "ru", "state_controlled_block"),
    ))
    article = _article("Sputnik", "US imposes new tariffs on Chinese EVs")
    cluster = _cluster([article])
    item = _impact_item(target_region="us", is_home_affairs=False)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "publishable"


def test_independent_public_foreign_naizheng_allowed():
    """BBC (gb, independent_public) on US 内政 -> allowed."""
    assessor = ImpactAssessor(_config(
        _source("BBC", "gb", "independent_public"),
    ))
    article = _article("BBC", "US Congress passes budget deal")
    cluster = _cluster([article])
    item = _impact_item(target_region="us", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "publishable"
    assert assessment.gate_blocked == []
    assert assessment.gate_review == []


def test_private_constrained_foreign_naizheng_needs_review():
    """IT之家 (cn, private_constrained) on JP 内政 -> needs_review with weight penalty."""
    assessor = ImpactAssessor(_config(
        _source("ITHome", "cn", "private_constrained"),
    ))
    article = _article("ITHome", "Japan PM reshuffles cabinet")
    cluster = _cluster([article])
    item = _impact_item(target_region="jp", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    original_composite = assessment.composite
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "needs_review"
    assert assessment.composite == original_composite * 0.85
    assert assessment.gate_review == ["ITHome"]
    assert assessment.gate_blocked == []


def test_cluster_mixed_sources_suppress_only_blocked():
    """Mixed cluster: BBC (allowed) + Sputnik (blocked) on US 内政.
    Sputnik suppressed; BBC survives; cluster stays publishable."""
    assessor = ImpactAssessor(_config(
        _source("BBC", "gb", "independent_public"),
        _source("Sputnik", "ru", "state_controlled_block"),
    ))
    bbc = _article("BBC", "US elections analysis")
    sputnik = _article("Sputnik", "US elections chaos claims")
    cluster = _cluster([bbc, sputnik])
    item = _impact_item(target_region="us", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert bbc.ownership_suppressed is False
    assert sputnik.ownership_suppressed is True
    assert assessment.status == "publishable"  # BBC survives


def test_cluster_all_blocked_dropped():
    """All articles state_controlled on foreign 内政 -> entire cluster suppressed."""
    assessor = ImpactAssessor(_config(
        _source("Sputnik", "ru", "state_controlled_block"),
        _source("VOA", "us", "state_controlled_block"),
    ))
    a1 = _article("Sputnik", "US protests escalate")
    a2 = _article("VOA", "China crackdown on dissent")
    cluster = _cluster([a1, a2])
    # target is "us" — Sputnik (ru->us) suppressed; VOA (us->us) is own country -> allowed
    # Actually VOA-on-CN would be cross-border. Let's make both cross-border.
    item = _impact_item(target_region="fr", is_home_affairs=True)  # French 内政

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert a1.ownership_suppressed is True  # ru->fr, cross-border
    assert a2.ownership_suppressed is True  # us->fr, cross-border
    assert assessment.status == "suppress"


def test_missing_ownership_defaults_to_review():
    """Source with no ownership stamp -> defaults to state_influenced_review -> needs_review."""
    assessor = ImpactAssessor(_config(
        _source("UnknownBlog", "us", "state_influenced_review"),
    ))
    article = _article("UnknownBlog", "France pension reform protests")
    cluster = _cluster([article])
    item = _impact_item(target_region="fr", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "needs_review"


def test_llm_failure_gate_inactive():
    """When LLM fails (item=None), target_region is None -> gate skipped."""
    assessor = ImpactAssessor(_config(
        _source("Sputnik", "ru", "state_controlled_block"),
    ))
    article = _article("Sputnik", "Some title")
    cluster = _cluster([article])

    assessment = assessor._build_assessment(cluster, None, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "publishable"  # signal-only fallback, no gate


def test_target_region_null_gate_skipped():
    """LLM returns target_region=null -> gate inactive (ambiguous target)."""
    assessor = ImpactAssessor(_config(
        _source("Sputnik", "ru", "state_controlled_block"),
    ))
    article = _article("Sputnik", "Some story")
    cluster = _cluster([article])
    item = _impact_item(target_region=None, is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False


def test_independent_nonprofit_allowed():
    """AP (nonprofit) on foreign 内政 -> allowed."""
    assessor = ImpactAssessor(_config(
        _source("AP", "us", "independent_nonprofit"),
    ))
    article = _article("AP", "UK election results")
    cluster = _cluster([article])
    item = _impact_item(target_region="gb", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "publishable"


def test_independent_private_low_evidence_weight_penalty():
    """TechnoEdge (low_evidence) on KR 内政 -> needs_review with 0.75 multiplier."""
    assessor = ImpactAssessor(_config(
        _source("TechnoEdge", "jp", "independent_private_low_evidence"),
    ))
    article = _article("TechnoEdge", "Korea election results")
    cluster = _cluster([article])
    item = _impact_item(target_region="kr", is_home_affairs=True)

    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    original = assessment.composite
    assessor._gate_cluster(cluster, assessment)

    assert article.ownership_suppressed is False
    assert assessment.status == "needs_review"
    assert assessment.composite == original * 0.75
