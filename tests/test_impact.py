"""Unit tests for the impact evaluation core (local math + parsing; LLM mocked)."""
from datetime import datetime, timezone
from types import SimpleNamespace

import litellm

from newsprism.config import Config, SourceConfig
from newsprism.service.impact import (
    DIMENSIONS,
    ImpactAssessor,
    ImpactItem,
    cluster_key,
)
from newsprism.types import Article, ArticleCluster


def _source(name: str, region: str, tier: str = "editorial") -> SourceConfig:
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
    )


def _config() -> Config:
    return Config(
        raw={},
        sources=[
            _source("Alpha", "us"),
            _source("Beta", "cn"),
            _source("Gamma", "jp"),
            _source("PortalX", "us", tier="portal"),
        ],
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
                    "scope": 0.16,
                    "severity": 0.16,
                    "novelty": 0.12,
                    "actor_influence": 0.14,
                    "decision_relevance": 0.18,
                    "feelgood": 0.0,
                    "signal": 0.24,
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
                    "portal": 0.55,
                    "unknown": 0.45,
                }
            },
        },
    )


def _article(source: str, title: str = "Test headline", official: bool = False) -> Article:
    return Article(
        url=f"https://example.com/{source}/{title[:10]}",
        title=title,
        source_name=source,
        published_at=datetime.now(timezone.utc),
        content="body " * 30,
        is_official_source=official,
    )


def _cluster(*sources: str) -> ArticleCluster:
    return ArticleCluster(
        topic_category="Test Event",
        articles=[_article(source) for source in sources],
    )


def _assessor(weights_loader=lambda: {}, policy_loader=lambda: None) -> ImpactAssessor:
    return ImpactAssessor(_config(), weights_loader=weights_loader, policy_loader=policy_loader)


def test_weights_normalized_and_calibration_overrides_seeds():
    assessor = _assessor(weights_loader=lambda: {"severity": 0.32, "ignored_dim": 9.0})
    weights = assessor.weights()
    assert abs(sum(weights.values()) - 1.0) < 1e-9
    # severity override is normalized but still dominates its seed ratio
    assert weights["severity"] > weights["scope"]
    assert "ignored_dim" not in weights


def test_signal_rewards_sources_and_regions():
    assessor = _assessor()
    multi, multi_flags = assessor._signal(_cluster("Alpha", "Beta", "Gamma"))
    single, single_flags = assessor._signal(_cluster("Alpha"))
    assert multi > single
    assert "single_source" in single_flags
    assert "single_source" not in multi_flags


def test_signal_flags_official_only():
    cluster = ArticleCluster(
        topic_category="Official",
        articles=[_article("Alpha", official=True), _article("Beta", official=True)],
    )
    assessor = _assessor()
    _, flags = assessor._signal(cluster)
    assert "official_only" in flags


def test_composite_blends_dims_and_signal():
    assessor = _assessor()
    weights = assessor.weights()
    dims = {dim: 10.0 for dim in DIMENSIONS}
    assert assessor._composite(dims, 1.0, weights) == 1.0
    zero = {dim: 0.0 for dim in DIMENSIONS}
    assert abs(assessor._composite(zero, 1.0, weights) - weights["signal"]) < 1e-9


def test_status_rules():
    assessor = _assessor()
    high = {dim: 8.0 for dim in DIMENSIONS}
    status, _ = assessor._status(high, 0.7, [], evaluated_by_llm=True)
    assert status == "publishable"
    status, _ = assessor._status(high, 0.05, [], evaluated_by_llm=True)
    assert status == "suppress"
    # signal-only fallback must never suppress
    status, _ = assessor._status({d: 0.0 for d in DIMENSIONS}, 0.05, [], evaluated_by_llm=False)
    assert status == "publishable"
    status, _ = assessor._status({**high, "severity": 7.0}, 0.7, ["single_source"], evaluated_by_llm=True)
    assert status == "seek_more_evidence"
    status, constraints = assessor._status(high, 0.7, ["official_only"], evaluated_by_llm=True)
    assert status == "needs_review"
    assert any("官方" in c for c in constraints)


def test_build_assessment_fallback_uses_signal_only():
    assessor = _assessor()
    cluster = _cluster("Alpha", "Beta")
    assessment = assessor._build_assessment(cluster, None, assessor.weights())
    assert assessment.evaluated_by_llm is False
    assert assessment.composite == assessment.signal
    assert assessment.display_category == "国际时政"
    assert assessment.status != "suppress"


def test_build_assessment_validates_category_and_icon():
    assessor = _assessor()
    cluster = _cluster("Alpha", "Beta")
    item = ImpactItem(
        cluster_index=1,
        scope=12.0,        # clamped to 10
        severity=5.0,
        novelty=5.0,
        actor_influence=5.0,
        decision_relevance=5.0,
        feelgood=0.0,
        rationale="  多空格   理由  ",
        display_category="不存在的栏目",
        topic_icon_key="not-an-icon",
    )
    assessment = assessor._build_assessment(cluster, item, assessor.weights())
    assert assessment.dim("scope") == 10.0
    assert assessment.display_category == "国际时政"
    assert assessment.topic_icon_key == "globe"
    assert "  " not in assessment.rationale


def test_salvage_items_from_malformed_output():
    assessor = _assessor()
    content = (
        'garbage before {"items": [ {"cluster_index": 1, "scope": 7, "severity": 6, '
        '"novelty": 5, "actor_influence": 8, "decision_relevance": 7, "feelgood": 0, '
        '"rationale": "重大地缘事件", "display_category": "国际时政", '
        '"short_topic_name": "美伊谈判", "topic_icon_key": "war"}, {"cluster_index": 2, '
        '"scope": 2, "severity": 1, BROKEN'
    )
    salvaged = assessor._salvage_items(content, 2)
    assert len(salvaged) == 2
    assert salvaged[0].scope == 7
    assert salvaged[0].display_category == "国际时政"
    assert salvaged[1].cluster_index == 2


def test_assess_clusters_with_mocked_llm(monkeypatch):
    assessor = _assessor()
    clusters = [_cluster("Alpha", "Beta"), _cluster("Gamma")]

    payload = (
        '{"items": ['
        '{"cluster_index": 1, "scope": 8, "severity": 7, "novelty": 6, "actor_influence": 9, '
        '"decision_relevance": 8, "feelgood": 0, "rationale": "大国冲突升级", '
        '"display_category": "国际时政", "short_topic_name": "冲突升级", "topic_icon_key": "war"},'
        '{"cluster_index": 2, "scope": 1, "severity": 0, "novelty": 3, "actor_influence": 1, '
        '"decision_relevance": 1, "feelgood": 9, "rationale": "治愈动物故事", '
        '"display_category": "文化艺术", "short_topic_name": "动物趣闻", "topic_icon_key": "globe"}'
        "]}"
    )

    def fake_completion(**kwargs):
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=payload))]
        )

    monkeypatch.setattr(litellm, "completion", fake_completion)
    assessments = assessor.assess_clusters(clusters)

    assert len(assessments) == 2
    assert clusters[0].impact is assessments[0]
    assert clusters[0].display_category == "国际时政"
    assert assessments[0].composite > assessments[1].composite
    assert assessments[1].dim("feelgood") == 9.0
    assert assessments[0].evaluated_by_llm and assessments[1].evaluated_by_llm
    assert cluster_key(clusters[0]) == assessments[0].cluster_key


def test_assess_clusters_llm_failure_falls_back_to_signal(monkeypatch):
    assessor = _assessor()
    clusters = [_cluster("Alpha", "Beta", "Gamma"), _cluster("Alpha")]

    def boom(**kwargs):
        raise RuntimeError("provider down")

    monkeypatch.setattr(litellm, "completion", boom)
    assessments = assessor.assess_clusters(clusters)

    assert all(not a.evaluated_by_llm for a in assessments)
    # deterministic degradation: multi-source outranks single-source
    assert assessments[0].composite > assessments[1].composite
    assert all(a.status != "suppress" for a in assessments)
