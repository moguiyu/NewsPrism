"""Tests for the merged history module: freshness, validation, storyline grouping."""
from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.service.history import (
    EventClusterValidator,
    FreshnessEvaluator,
    StorylineResolver,
    StorylineStateMachine,
)
from newsprism.types import Article, ArticleCluster, Cluster, ImpactAssessment


def _config(hot_enabled: bool = True) -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={"window_days": 3},
        summarizer={},
        output={
            "hot_topics": {
                "enabled": hot_enabled,
                "tab_name_max_chars": 10,
                "edge_confidence_threshold": 0.56,
                "admission_similarity": 0.62,
                "history_similarity_threshold": 0.40,
                "icon_allowlist": ["globe", "war", "trade", "chip", "ai", "energy"],
            }
        },
        active_search={},
    )


def _article(title: str, embedding: list[float]) -> Article:
    return Article(
        url=f"https://example.com/{title}",
        title=title,
        source_name="Reuters",
        published_at=datetime.now(tz=timezone.utc),
        content=f"{title} body",
        embedding=embedding,
    )


class _StubSummarizer:
    def __init__(self, results):
        self._results = results

    def classify_storyline_relations(self, _pairs):
        return list(self._results)

    def name_storyline(self, _anchor_clusters):
        return "中东战事"


# ─── EventClusterValidator ────────────────────────────────────────────────────

def test_validator_splits_incoherent_two_article_cluster():
    validator = EventClusterValidator(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            _article("Hong Kong money laundering case", [1.0, 0.0, 0.0]),
            _article("Hospital patient forgotten in MRI room", [0.0, 1.0, 0.0]),
        ],
    )
    validated = validator.validate([cluster])
    assert len(validated) == 2


def test_validator_keeps_coherent_cluster_together():
    validator = EventClusterValidator(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            _article("US strikes Iran targets", [1.0, 0.0, 0.0]),
            _article("US launches strikes on Iran", [0.97, 0.03, 0.0]),
        ],
    )
    validated = validator.validate([cluster])
    assert len(validated) == 1


def test_validator_noop_when_hot_topics_disabled():
    validator = EventClusterValidator(_config(hot_enabled=False))
    cluster = ArticleCluster(
        topic_category="x",
        articles=[_article("a", [1.0, 0.0]), _article("b", [0.0, 1.0])],
    )
    assert validator.validate([cluster]) == [cluster]


# ─── StorylineResolver ────────────────────────────────────────────────────────

def test_resolver_unions_core_and_spillover_from_edges():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_direct_spillover_storyline", "confidence": 0.82}]
        ),
        similarity_fn=lambda _t, _h: 0.0,
    )
    clusters = [
        ArticleCluster(topic_category="World News", articles=[_article("US announces tariff hike", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Business", articles=[_article("Markets drop after tariff hike", [0.9, 0.1, 0.0])]),
        ArticleCluster(topic_category="Sports", articles=[_article("Team wins season final", [0.0, 1.0, 0.0])]),
    ]
    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    assert resolved[0].storyline_key == resolved[1].storyline_key
    assert resolved[0].storyline_role == "core"
    assert resolved[1].storyline_role == "spillover"
    assert resolved[2].storyline_role == "none"
    assert resolved[2].storyline_key != resolved[0].storyline_key


def test_resolver_ignores_subthreshold_edges():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.40}]
        ),
        similarity_fn=lambda _t, _h: 0.0,
    )
    clusters = [
        ArticleCluster(topic_category="World News", articles=[_article("US strikes Iran", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="World News", articles=[_article("Iran responds to strike", [0.95, 0.05, 0.0])]),
    ]
    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    assert resolved[0].storyline_key != resolved[1].storyline_key


def test_resolver_reuses_historical_identity():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.84}]
        ),
        similarity_fn=lambda text, historical: 0.66 if "tariff" in text.lower() and "tariff" in historical.summary.lower() else 0.0,
    )
    clusters = [
        ArticleCluster(topic_category="World News", articles=[_article("US announces tariff hike", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Business", articles=[_article("China responds to tariff move", [0.95, 0.05, 0.0])]),
    ]
    historical = [
        Cluster(
            id=1,
            topic_category="World News",
            article_ids=[1],
            summary="Tariff war expands after latest policy move",
            perspectives={},
            report_date="2026-03-14",
            storyline_key="tariff-war",
            storyline_name="关税战",
            storyline_role="core",
            storyline_confidence=0.81,
        )
    ]
    resolved = resolver.resolve(clusters, historical, datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    assert {cluster.storyline_key for cluster in resolved} == {"tariff-war"}
    assert {cluster.storyline_name for cluster in resolved} == {"关税战"}


def test_resolver_uses_llm_name_for_new_family():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.88}]
        ),
        similarity_fn=lambda _t, _h: 0.0,
    )
    clusters = [
        ArticleCluster(topic_category="World News", articles=[_article("US strikes Iran targets", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="World News", articles=[_article("Iran vows retaliation", [0.95, 0.05, 0.0])]),
    ]
    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    assert resolved[0].storyline_key == resolved[1].storyline_key
    assert all(cluster.storyline_name == "中东战事" for cluster in resolved)


# ─── FreshnessEvaluator ───────────────────────────────────────────────────────

def _historical_cluster(summary: str, sources: list[str]) -> Cluster:
    return Cluster(
        id=1,
        topic_category="World News",
        article_ids=[1],
        summary=summary,
        perspectives={source: "" for source in sources},
        report_date="2026-03-14",
    )


def test_freshness_new_when_no_history():
    evaluator = FreshnessEvaluator(_config())
    cluster = ArticleCluster(topic_category="x", articles=[_article("Brand new event", [1.0, 0.0])])
    result = evaluator.evaluate(cluster, "Brand new event summary", [])
    assert result.state == "new"


def test_freshness_developing_when_new_sources(monkeypatch):
    evaluator = FreshnessEvaluator(_config())
    monkeypatch.setattr(evaluator, "_compute_embedding", lambda text: __import__("numpy").array([1.0, 0.0]))
    cluster = ArticleCluster(topic_category="x", articles=[_article("t", [1.0, 0.0])])
    cluster.sources = ["Reuters", "BBC News"]
    historical = [_historical_cluster("same story", ["Reuters"])]
    result = evaluator.evaluate(cluster, "same story", historical)
    assert result.state == "developing"
    assert "BBC News" in (result.new_sources or [])


def test_freshness_stale_when_no_new_sources(monkeypatch):
    evaluator = FreshnessEvaluator(_config())
    monkeypatch.setattr(evaluator, "_compute_embedding", lambda text: __import__("numpy").array([1.0, 0.0]))
    cluster = ArticleCluster(topic_category="x", articles=[_article("t", [1.0, 0.0])])
    cluster.sources = ["Reuters"]
    historical = [_historical_cluster("same story", ["Reuters"])]
    result = evaluator.evaluate(cluster, "same story", historical)
    assert result.state == "stale"


# ─── StorylineStateMachine ────────────────────────────────────────────────────

def test_state_machine_emerging_without_history():
    machine = StorylineStateMachine()
    cluster = ArticleCluster(topic_category="x", articles=[_article("t", [1.0, 0.0])])
    cluster.storyline_key = "new-key"
    assert machine.resolve_state(cluster, []) == "emerging"


def test_state_machine_developing_with_history():
    machine = StorylineStateMachine()
    cluster = ArticleCluster(topic_category="x", articles=[_article("t", [1.0, 0.0])])
    cluster.storyline_key = "k"
    historical = [_historical_cluster("prior", ["Reuters"])]
    historical[0].storyline_key = "k"
    assert machine.resolve_state(cluster, historical) == "developing"


def test_state_machine_stabilized_with_strong_impact():
    machine = StorylineStateMachine()
    cluster = ArticleCluster(
        topic_category="x",
        articles=[
            Article(url="a", title="t", source_name="Reuters", published_at=datetime.now(timezone.utc), content="c", origin_region="us"),
            Article(url="b", title="t", source_name="BBC News", published_at=datetime.now(timezone.utc), content="c", origin_region="gb"),
        ],
    )
    cluster.storyline_key = "k"
    cluster.impact = ImpactAssessment(cluster_key="k", composite=0.7)
    historical = [_historical_cluster("prior", ["Reuters"])]
    historical[0].storyline_key = "k"
    assert machine.resolve_state(cluster, historical) == "stabilized"
