from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.service.storyline import EventClusterValidator, StorylineResolver
from newsprism.types import Article, ArticleCluster, Cluster


def _config() -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={
            "hot_topics": {
                "enabled": True,
                "tab_name_max_chars": 10,
                "max_pair_candidates": 20,
                "storyline_blocker_similarity": 0.55,
                "storyline_blocker_title_overlap": 0.05,
                "storyline_edge_confidence_threshold": 0.56,
                "storyline_history_similarity_threshold": 0.40,
                "icon_allowlist": ["globe", "war", "trade", "chip", "ai", "energy"],
                "cluster_validation_pair_similarity": 0.50,
                "cluster_validation_pair_title_overlap": 0.03,
                "cluster_validation_outlier_similarity": 0.58,
                "cluster_validation_outlier_title_overlap": 0.05,
            }
        },
        active_search={},
        topic_equivalence={},
    )


def _article(title: str, embedding: list[float], topic: str = "World News") -> Article:
    return Article(
        url=f"https://example.com/{title}",
        title=title,
        source_name="Reuters",
        published_at=datetime.now(tz=timezone.utc),
        content=f"{title} body",
        embedding=embedding,
        topics=[topic],
    )


class _StubSummarizer:
    def __init__(self, results: list[dict[str, object]]) -> None:
        self._results = results

    def classify_storyline_relations(self, _pair_candidates):
        return list(self._results)

    def name_storyline(self, _anchor_clusters):
        return "中东战事"


class _HeadlineNameStubSummarizer(_StubSummarizer):
    def name_storyline(self, _anchor_clusters):
        return "俄交通部：载有十万吨"


def test_event_cluster_validator_splits_incoherent_two_article_cluster():
    validator = EventClusterValidator(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            _article("Hong Kong money laundering case", [1.0, 0.0, 0.0]),
            _article("Hospital patient forgotten in MRI room", [0.0, 1.0, 0.0], topic="Society"),
        ],
    )

    validated = validator.validate([cluster])

    assert len(validated) == 2
    assert validated[0].articles[0].title != validated[1].articles[0].title


def test_storyline_resolver_groups_core_and_direct_spillover_generically():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_direct_spillover_storyline", "confidence": 0.82}
            ]
        ),
        similarity_fn=lambda _text, _historical: 0.0,
    )
    clusters = [
        ArticleCluster(
            topic_category="World News",
            articles=[_article("US announces tariff hike", [1.0, 0.0, 0.0], topic="Trade")],
        ),
        ArticleCluster(
            topic_category="Business",
            articles=[_article("Markets drop after tariff hike", [0.9, 0.1, 0.0], topic="Trade")],
        ),
        ArticleCluster(
            topic_category="Sports",
            articles=[_article("Team wins season final", [0.0, 1.0, 0.0], topic="Sports")],
        ),
    ]

    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())

    assert resolved[0].storyline_key == resolved[1].storyline_key
    assert resolved[0].storyline_role == "core"
    assert resolved[1].storyline_role == "spillover"
    assert resolved[2].storyline_role == "none"
    assert resolved[2].storyline_key != resolved[0].storyline_key


def test_storyline_resolver_reuses_historical_storyline_identity():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.84}
            ]
        ),
        similarity_fn=lambda text, historical: 0.66 if "tariff" in text.lower() and "tariff" in historical.summary.lower() else 0.0,
    )
    clusters = [
        ArticleCluster(
            topic_category="World News",
            articles=[_article("US announces tariff hike", [1.0, 0.0, 0.0], topic="Trade")],
        ),
        ArticleCluster(
            topic_category="Business",
            articles=[_article("China responds to tariff move", [0.95, 0.05, 0.0], topic="Trade")],
        ),
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


def test_storyline_resolver_reuses_history_when_anchor_has_no_match():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.84}
            ]
        ),
        similarity_fn=lambda text, historical: 0.66 if "tariff" in text.lower() and "tariff" in historical.summary.lower() else 0.0,
    )
    clusters = [
        ArticleCluster(
            topic_category="World News",
            articles=[_article("US says talks continue after tariff hike", [1.0, 0.0, 0.0], topic="Trade")],
        ),
        ArticleCluster(
            topic_category="Business",
            articles=[_article("China responds to tariff move", [0.95, 0.05, 0.0], topic="Trade")],
        ),
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


def test_storyline_resolver_excludes_broad_policy_story_from_conflict_family():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.86},
                {"left_index": 0, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.68},
            ]
        ),
        similarity_fn=lambda _text, _historical: 0.0,
    )
    clusters = [
        ArticleCluster(
            topic_category="World News",
            articles=[_article("US strikes Iranian military targets", [1.0, 0.0, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("Iran vows retaliation after strikes", [0.95, 0.05, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("China responds to proposed US arms sale to Taiwan", [0.55, 0.45, 0.0], topic="Geopolitics")],
        ),
    ]

    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())

    assert resolved[0].storyline_key == resolved[1].storyline_key
    assert resolved[2].storyline_membership_status == "excluded_to_main"
    assert resolved[2].storyline_key != resolved[0].storyline_key
    assert resolved[0].storyline_name == "中东战事"


def test_storyline_resolver_admits_direct_conflict_and_direct_spillover_members():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.88},
                {"left_index": 0, "right_index": 2, "relation": "same_direct_spillover_storyline", "confidence": 0.81},
            ]
        ),
        similarity_fn=lambda _text, _historical: 0.0,
    )
    clusters = [
        ArticleCluster(
            topic_category="World News",
            articles=[_article("US plans strikes on Iran next week", [1.0, 0.0, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("US defense secretary says Iranian leader was injured", [0.91, 0.09, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="Business",
            articles=[_article("Oil shipping through Hormuz faces severe disruption", [0.85, 0.15, 0.0], topic="Energy & Climate (English)")],
        ),
    ]

    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())

    assert {cluster.storyline_key for cluster in resolved} == {resolved[0].storyline_key}
    assert resolved[0].storyline_role == "core"
    assert resolved[1].storyline_role == "core"
    assert resolved[2].storyline_role == "spillover"
    assert all(cluster.storyline_membership_status in {"core", "spillover"} for cluster in resolved)
    assert all(cluster.storyline_name == "中东战事" for cluster in resolved)


def test_storyline_resolver_replaces_headline_like_storyline_name_with_synthesized_label():
    resolver = StorylineResolver(
        _config(),
        summarizer=_HeadlineNameStubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.88},
            ]
        ),
        similarity_fn=lambda _text, _historical: 0.0,
    )
    clusters = [
        ArticleCluster(
            topic_category="World News",
            articles=[_article("特朗普警告伊朗能源设施", [1.0, 0.0, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("伊朗称将回应美方新制裁", [0.95, 0.05, 0.0], topic="Geopolitics")],
        ),
    ]

    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())

    assert {cluster.storyline_key for cluster in resolved} == {resolved[0].storyline_key}
    assert all(cluster.storyline_name == "伊朗局势" for cluster in resolved)


def test_storyline_resolver_splits_disjoint_core_groups_before_hotspot_selection():
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.88},
                {"left_index": 2, "right_index": 3, "relation": "same_core_storyline", "confidence": 0.86},
                {"left_index": 0, "right_index": 4, "relation": "same_direct_spillover_storyline", "confidence": 0.78},
            ]
        ),
        similarity_fn=lambda _text, _historical: 0.0,
    )
    clusters = [
        ArticleCluster(
            topic_category="World News",
            articles=[_article("Trump threatens Iran over ceasefire talks", [1.0, 0.0, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("US poll says Iran strikes went too far", [0.95, 0.05, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("Trump to visit China in mid-May", [0.0, 1.0, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("White House confirms Trump China itinerary", [0.05, 0.95, 0.0], topic="Geopolitics")],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("Zelensky says US tied security guarantees to Donbas", [0.25, 0.15, 0.6], topic="Geopolitics")],
        ),
    ]

    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())

    assert resolved[0].storyline_key == resolved[1].storyline_key
    assert resolved[2].storyline_key == resolved[3].storyline_key
    assert resolved[0].storyline_key != resolved[2].storyline_key
    assert resolved[4].storyline_role == "none"
    assert resolved[4].storyline_key not in {resolved[0].storyline_key, resolved[2].storyline_key}
