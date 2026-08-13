"""Tests for the merged history module: freshness, validation, storyline grouping."""
from datetime import datetime, timezone

from newsprism.config import Config, load_config
from newsprism.service.history import (
    EventClusterValidator,
    FreshnessEvaluator,
    StorylineResolver,
    StorylineStateMachine,
    _content_hash,
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


def test_resolver_uses_namespaced_values_from_real_config():
    cfg = load_config("config/config.yaml")
    cfg.output["hot_topics"].update(
        {
            "edge_confidence_threshold": 0.01,
            "admission_similarity": 0.02,
            "history_similarity_threshold": 0.03,
        }
    )
    resolver = StorylineResolver(cfg, _StubSummarizer([]), lambda *_args: 0.0)

    assert resolver.edge_confidence_threshold == 0.50
    assert resolver.candidate_similarity == 0.64
    assert resolver.history_similarity_threshold == 0.55
    assert resolver.conflict_history_similarity_threshold == 0.40


def test_conflict_relation_requires_both_stories_to_name_same_conflict_pair():
    iran = ArticleCluster(
        topic_category="US Israel Iran conflict",
        articles=[_article("US and Israel strike Iran", [1.0, 0.0])],
    )
    ukraine = ArticleCluster(
        topic_category="Russia Ukraine war",
        articles=[_article("Russia launches drones at Ukraine", [0.9, 0.1])],
    )
    resolver = StorylineResolver(
        _config(),
        _StubSummarizer([
            {
                "left_index": 0,
                "right_index": 1,
                "relation": "same_conflict_different_event",
                "confidence": 0.92,
            }
        ]),
        lambda *_args: 0.0,
    )
    candidate = {
        "left_index": 0,
        "right_index": 1,
        "left_cluster": iran,
        "right_cluster": ukraine,
        "similarity": 0.7,
        "title_overlap": 0.1,
        "signal_overlap": 1,
    }

    result = resolver._classify_pairs([candidate])[0]

    assert result["relation"] == "not_related"


def test_conflict_relation_keeps_distinct_events_in_same_conflict():
    left = ArticleCluster(
        topic_category="Russia Ukraine war",
        articles=[_article("Russia attacks Ukraine port", [1.0, 0.0])],
    )
    right = ArticleCluster(
        topic_category="Russia Ukraine war",
        articles=[_article("Ukraine strikes Russian depot", [0.8, 0.2])],
    )
    resolver = StorylineResolver(
        _config(),
        _StubSummarizer([
            {
                "left_index": 0,
                "right_index": 1,
                "relation": "same_conflict_different_event",
                "confidence": 0.88,
            }
        ]),
        lambda *_args: 0.0,
    )
    candidate = {
        "left_index": 0,
        "right_index": 1,
        "left_cluster": left,
        "right_cluster": right,
        "similarity": 0.7,
        "title_overlap": 0.1,
        "signal_overlap": 1,
    }

    result = resolver._classify_pairs([candidate])[0]

    assert result["relation"] == "same_conflict_different_event"


def test_conflict_relation_treats_iran_us_and_iran_israel_as_same_crisis():
    left = ArticleCluster(
        topic_category="美伊局势",
        articles=[_article("美国宣布对伊朗采取行动", [1.0, 0.0])],
    )
    right = ArticleCluster(
        topic_category="以伊冲突",
        articles=[_article("以色列与伊朗交换空袭", [0.8, 0.2])],
    )
    resolver = StorylineResolver(
        _config(),
        _StubSummarizer([
            {
                "left_index": 0,
                "right_index": 1,
                "relation": "same_conflict_different_event",
                "confidence": 0.88,
            }
        ]),
        lambda *_args: 0.0,
    )
    candidate = {
        "left_index": 0,
        "right_index": 1,
        "left_cluster": left,
        "right_cluster": right,
        "similarity": 0.7,
        "title_overlap": 0.1,
        "signal_overlap": 1,
    }

    result = resolver._classify_pairs([candidate])[0]

    assert result["relation"] == "same_conflict_different_event"

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


def test_resolver_splits_incoherent_chain_into_coherent_families():
    """Transitively-chained edges must not merge unrelated clusters into one
    family. A Middle East trio chained to an unrelated AI pair should split into
    two internally-coherent families, not one family of five."""
    from collections import Counter

    # Two genuinely-distinct stories (a tight A-pair and a tight B-pair). The
    # shared title token makes every pair a candidate; the LLM stub asserts a
    # bogus A→B bridge edge (1↔2) that union-find would chain into one family.
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.80},
                {"left_index": 1, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.78},
                {"left_index": 2, "right_index": 3, "relation": "same_core_storyline", "confidence": 0.80},
            ]
        ),
        similarity_fn=lambda _t, _h: 0.0,
    )
    clusters = [
        ArticleCluster(topic_category="World", articles=[_article("美伊协议签署 全球快讯", [1.0, 0.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="World", articles=[_article("伊朗回应美方 全球快讯", [1.0, 0.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Tech", articles=[_article("英伟达发布新芯片 全球快讯", [0.0, 1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Tech", articles=[_article("台积电扩产芯片 全球快讯", [0.0, 1.0, 0.0, 0.0])]),
    ]
    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    keys = [c.storyline_key for c in resolved]
    # The A-pair and B-pair each cohere, but must not be chained together.
    assert keys[0] == keys[1]
    assert keys[2] == keys[3]
    assert keys[0] != keys[2]
    assert max(Counter(keys).values()) == 2


def test_resolver_detaches_history_magnet_singletons():
    """A stale historical storyline must not glue unrelated singletons together.

    Three clusters each independently match the same historical key, but only two
    are mutually coherent; the third (orthogonal embedding) must detach."""
    from collections import Counter

    class _HistSim:
        """Matches every cluster to the same historical storyline above threshold."""

        def __call__(self, _text, _hist):
            return 0.9

    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer([]),  # no LLM edges → all singletons pre-assignment
        similarity_fn=_HistSim(),
    )
    historical = [
        Cluster(
            id=1,
            topic_category="World",
            article_ids=[99],
            summary="",
            perspectives={},
            report_date="2026-03-14",
            storyline_key="mideast",
            storyline_name="中东局势",
            storyline_role="core",
        )
    ]
    clusters = [
        ArticleCluster(topic_category="World", articles=[_article("US Iran deal nears", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="World", articles=[_article("Iran responds to US", [0.98, 0.05, 0.0])]),
        ArticleCluster(topic_category="Sports", articles=[_article("Team wins the final", [0.0, 1.0, 0.0])]),
    ]
    resolved = resolver.resolve(clusters, historical, datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    keys = [c.storyline_key for c in resolved]
    # The coherent Iran pair keeps the storyline; the sports cluster detaches.
    assert keys[0] == keys[1] == "mideast"
    assert keys[2] != "mideast"
    assert max(Counter(keys).values()) == 2


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


def test_resolver_conflict_relation_glues_different_events_as_spillover():
    """Issue #2 rec #3: same_conflict_different_event edges glue members into
    one family with role=spillover (never core).

    Simulates three distinct daily incidents of the Russia-Ukraine war that
    have low pairwise centroid similarity (different locations, different
    casualties). Under the OLD precision-first policy, the LLM marked them
    not_related and they spilled into the main lane. Under the new policy,
    the LLM mark of same_conflict_different_event glues them into one tab.
    """
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                {"left_index": 0, "right_index": 1, "relation": "same_conflict_different_event", "confidence": 0.70},
                {"left_index": 1, "right_index": 2, "relation": "same_conflict_different_event", "confidence": 0.65},
            ]
        ),
        similarity_fn=lambda _t, _h: 0.0,
    )
    # Three semantically distinct Russia-Ukraine events (orthogonal embeddings
    # would be rejected by the 0.60 coherence bar; the conflict relaxation
    # lowers it to 0.40 for components glued by same_conflict_different_event).
    # Titles share "Russia" / "Ukraine" tokens so they enter the pair-candidate
    # pool via title_overlap; the LLM stub then returns the conflict relation.
    clusters = [
        ArticleCluster(topic_category="World News", articles=[_article("Russia Ukraine Odesa attack kills", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="World News", articles=[_article("Russia Ukraine merchant ships Black Sea", [0.0, 1.0, 0.0])]),
        ArticleCluster(topic_category="World News", articles=[_article("Russia Ukraine troop losses 2022", [0.0, 0.0, 1.0])]),
    ]
    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    # All three glued into one family.
    keys = {c.storyline_key for c in resolved}
    assert len(keys) == 1, f"expected single family, got keys={keys}"
    # Roles: exactly one core (highest composite anchor), the rest spillover.
    roles = [c.storyline_role for c in resolved]
    assert roles.count("core") == 1
    assert roles.count("spillover") == 2


def test_resolver_conflict_relation_does_not_affect_non_conflict_components():
    """The coherence relaxation applies ONLY to components with at least one
    same_conflict_different_event edge — ordinary same_core/spillover components
    keep the strict 0.60 coherence bar (don't re-introduce the over-merging bug
    that motivated precision-first)."""
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [
                # Two unrelated events that happen to chain via a bogus same_core edge.
                {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.80},
                {"left_index": 1, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.78},
            ]
        ),
        similarity_fn=lambda _t, _h: 0.0,
    )
    # Three semantically orthogonal events with NO conflict keywords.
    clusters = [
        ArticleCluster(topic_category="Tech", articles=[_article("Apple launches new phone", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Sports", articles=[_article("World cup final preview", [0.0, 1.0, 0.0])]),
        ArticleCluster(topic_category="Culture", articles=[_article("Museum opens new wing", [0.0, 0.0, 1.0])]),
    ]
    resolved = resolver.resolve(clusters, [], datetime(2026, 3, 15, tzinfo=timezone.utc).date())
    # No conflict edges → strict 0.60 bar → split into separate families.
    keys = {c.storyline_key for c in resolved}
    assert len(keys) >= 2, f"expected split families, got keys={keys}"


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


def test_resolver_regenerates_name_when_history_reuse_is_stale():
    """When a historical storyline_key is reused via a weak cosine match for a
    DIFFERENT topic, the historical name is dropped and a fresh name is
    generated from today's content. The KEY is preserved for cross-day
    continuity.

    Reproduces the 7/22 incident where storyline-3 (historically
    "特朗普再提选举舞弊") was attached to Philippines reef content via a weak
    match — the user saw a tab named "特朗普再提选举舞弊" containing stories
    about 中方/菲方/仁爱礁. Uses Chinese content (as production does per the
    style guide) so the same-script token-overlap check can detect the drift.
    """
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.80}]
        ),
        similarity_fn=lambda _t, _h: 0.6,  # weak but above threshold → triggers reuse
    )
    clusters = [
        ArticleCluster(topic_category="World News", articles=[_article("中方就仁爱礁问题警告菲方", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="World News", articles=[_article("海警在争议礁石附近对峙", [0.95, 0.05, 0.0])]),
    ]
    # 5-day-old history with an UNRELATED storyline name → name should regenerate.
    historical = [
        Cluster(
            id=1,
            topic_category="Politics",
            article_ids=[1],
            summary="China Philippines reef dispute",
            perspectives={},
            report_date="2026-07-17",  # 5 days before 7/22
            storyline_key="storyline-3",
            storyline_name="特朗普再提选举舞弊",
            storyline_role="core",
            storyline_confidence=0.6,
        )
    ]
    resolved = resolver.resolve(clusters, historical, datetime(2026, 7, 22, tzinfo=timezone.utc).date())
    names = {cluster.storyline_name for cluster in resolved}
    # Key is preserved (cross-day continuity)...
    keys = {cluster.storyline_key for cluster in resolved}
    assert "storyline-3" in keys
    # ...but the unrelated historical name is NOT used.
    assert "特朗普再提选举舞弊" not in names


def test_resolver_keeps_historical_name_when_adjacent():
    """Sanity: for yesterday's storylines (≤2 days old), the historical name
    is always preserved — the coherence check is only applied to older history
    where drift is plausible. This guards the common case of a multi-day
    storyline whose name is in the source language (e.g. Chinese) while
    today's headlines are in another language.
    """
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.80}]
        ),
        similarity_fn=lambda _t, _h: 0.6,
    )
    clusters = [
        ArticleCluster(topic_category="Business", articles=[_article("US announces tariff hike", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Business", articles=[_article("China responds to tariff move", [0.95, 0.05, 0.0])]),
    ]
    historical = [
        Cluster(
            id=1,
            topic_category="Business",
            article_ids=[1],
            summary="Tariff war expands",
            perspectives={},
            report_date="2026-07-21",  # 1 day before 7/22 — adjacent
            storyline_key="tariff-war",
            storyline_name="关税战",  # Chinese name, English content
            storyline_role="core",
            storyline_confidence=0.81,
        )
    ]
    resolved = resolver.resolve(clusters, historical, datetime(2026, 7, 22, tzinfo=timezone.utc).date())
    # Adjacent history → name preserved even with cross-language mismatch.
    assert "关税战" in {cluster.storyline_name for cluster in resolved}


def test_resolver_does_not_reuse_ru_ua_history_for_unrelated_korean_missile_launch():
    """A conflict-family key cannot turn a generic missile event into Kyiv coverage.

    This reproduces the Aug. 6–8 contamination: a real Russia-Ukraine
    storyline had a North-Korean missile spillover, after which an unrelated
    Korean launch reused the Kyiv key through a permissive cross-script match.
    """
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer([]),
        similarity_fn=lambda _text, _historical: 0.80,
    )
    clusters = [
        ArticleCluster(
            topic_category="North Korea launch",
            articles=[_article("North Korea fires ballistic missile into Sea of Japan", [1.0, 0.0])],
        ),
        ArticleCluster(
            topic_category="Kyiv missile attack",
            articles=[_article("Russia missile attack on Kyiv Ukraine", [0.0, 1.0])],
        ),
    ]
    historical = [
        Cluster(
            id=1,
            topic_category="Russia Ukraine war",
            article_ids=[1],
            summary="Russia launches missiles at Kyiv in Ukraine",
            perspectives={},
            report_date="2026-08-07",
            storyline_key="kyiv-missiles",
            storyline_name="基辅导弹袭击",
            storyline_role="core",
            storyline_confidence=0.80,
        )
    ]

    resolved = resolver.resolve(clusters, historical, datetime(2026, 8, 8, tzinfo=timezone.utc).date())

    assert resolved[0].storyline_key != "kyiv-missiles"
    assert resolved[1].storyline_key == "kyiv-missiles"


def test_resolver_conflict_signature_admits_same_conflict_cluster_below_generic_bar():
    """08-13 Iran/Hormuz regression: same-conflict clusters scored between the
    relaxed conflict bar (0.40) and the generic history bar (0.55) and were
    stranded as single-* while the family shrank from 4 core members (08-09) to
    1. A cluster whose conflict signature intersects the historical family's
    signature must be admitted at the relaxed bar."""
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer([]),
        # The Iran cluster scores in the relaxed-conflict band (0.40–0.55);
        # the unrelated cluster scores far below any admission bar.
        similarity_fn=lambda text, _h: 0.47 if "Trump demands compensation" in text else 0.05,
    )
    clusters = [
        ArticleCluster(
            topic_category="US Iran conflict",
            articles=[_article("Trump demands compensation from Iran", [1.0, 0.0])],
        ),
        ArticleCluster(
            topic_category="World News",
            articles=[_article("Europe reacts to Iran US tensions", [0.0, 1.0])],
        ),
    ]
    historical = [
        Cluster(
            id=1,
            topic_category="World News",
            article_ids=[1],
            summary="US Iran tensions and Hormuz attacks",
            perspectives={},
            report_date="2026-08-12",
            storyline_key="storyline-ed88a9db",
            storyline_name="霍尔木兹海峡局势",
            storyline_role="core",
            storyline_confidence=0.80,
        )
    ]

    resolved = resolver.resolve(clusters, historical, datetime(2026, 8, 13, tzinfo=timezone.utc).date())

    # The same-conflict cluster reuses the historical family key even though
    # 0.47 sits below the generic 0.55 history bar.
    assert "storyline-ed88a9db" in {cluster.storyline_key for cluster in resolved}


def test_resolver_generic_bar_still_applies_to_non_conflict_clusters():
    """The relaxed conflict bar must NOT admit ordinary (non-conflict) clusters
    below the generic history threshold — same-script drift guard stays intact."""
    cfg = _config()
    cfg.output["hot_topics"]["history_similarity_threshold"] = 0.55
    resolver = StorylineResolver(
        cfg,
        summarizer=_StubSummarizer([]),
        similarity_fn=lambda _t, _h: 0.47,
    )
    clusters = [
        ArticleCluster(
            topic_category="Economy",
            articles=[_article("Fed holds rates amid inflation data", [1.0, 0.0])],
        ),
    ]
    historical = [
        Cluster(
            id=1,
            topic_category="Economy",
            article_ids=[1],
            summary="US central bank policy and markets",
            perspectives={},
            report_date="2026-08-12",
            storyline_key="storyline-fed-rates",
            storyline_name="美联储利率",
            storyline_role="core",
            storyline_confidence=0.80,
        )
    ]

    resolved = resolver.resolve(clusters, historical, datetime(2026, 8, 13, tzinfo=timezone.utc).date())

    assert resolved[0].storyline_key != "storyline-fed-rates"


def test_resolver_regenerates_name_on_adjacent_same_script_drift():
    """Regression for the 2026-07-31 incident: a storyline named "美以伊局势"
    (US-Israel-Iran) was reused for Russia-Ukraine content the very next day.
    The name and content are both Chinese (same script), so the cross-script
    guard does NOT apply — zero token overlap correctly detects the drift and
    the name is regenerated even though the history is only 1 day old.
    """
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.80}]
        ),
        similarity_fn=lambda _t, _h: 0.55,
    )
    clusters = [
        ArticleCluster(topic_category="World News", articles=[_article("波兰总理称俄导弹落入波兰境内", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="World News", articles=[_article("俄军大规模空袭乌克兰致多人伤亡", [0.95, 0.05, 0.0])]),
    ]
    historical = [
        Cluster(
            id=1,
            topic_category="World News",
            article_ids=[1],
            summary="US Israel Iran military escalation",
            perspectives={},
            report_date="2026-07-30",  # 1 day before 7/31 — adjacent
            storyline_key="storyline-8aa4fcb4",
            storyline_name="美以伊局势",
            storyline_role="core",
            storyline_confidence=0.81,
        )
    ]
    resolved = resolver.resolve(clusters, historical, datetime(2026, 7, 31, tzinfo=timezone.utc).date())
    names = {cluster.storyline_name for cluster in resolved}
    # Same-script, zero-overlap → name regenerated despite adjacent reuse.
    assert "美以伊局势" not in names


def test_resolver_regenerates_name_on_same_domain_different_entity():
    """Regression for the 2026-07-31 Tab 3 issue: a storyline named
    "美联储宣布维持利率不" (Fed holds rates) was reused for Bank of England and
    Bank of Japan rate-decision content. The entity-prefix guard catches this:
    none of {美, 联, 储} appear in BoE/BoJ content, so the name is regenerated
    even though generic vocabulary (维持, 利率, 不) gives 45% token overlap.
    """
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer(
            [{"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.80}]
        ),
        similarity_fn=lambda _t, _h: 0.55,
    )
    clusters = [
        ArticleCluster(topic_category="Business", articles=[_article("英国央行维持利率3.75%不变", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Business", articles=[_article("日本央行维持利率1%不变", [0.95, 0.05, 0.0])]),
    ]
    historical = [
        Cluster(
            id=1,
            topic_category="Business",
            article_ids=[1],
            summary="Federal Reserve holds interest rate",
            perspectives={},
            report_date="2026-07-30",
            storyline_key="single-ad5f91a3",
            storyline_name="美联储宣布维持利率不",
            storyline_role="core",
            storyline_confidence=0.81,
        )
    ]
    resolved = resolver.resolve(clusters, historical, datetime(2026, 7, 31, tzinfo=timezone.utc).date())
    names = {cluster.storyline_name for cluster in resolved}
    assert "美联储宣布维持利率不" not in names


def test_resolver_does_not_reuse_mismatched_singleton_history():
    title = "英国央行维持利率3.75%不变"
    resolver = StorylineResolver(
        _config(),
        summarizer=_StubSummarizer([]),
        similarity_fn=lambda _text, _historical: 0.66,
    )
    cluster = ArticleCluster(
        topic_category="Business",
        articles=[_article(title, [1.0, 0.0, 0.0])],
    )
    historical = [
        Cluster(
            id=1,
            topic_category="Business",
            article_ids=[1],
            summary="Federal Reserve holds interest rate",
            perspectives={},
            report_date="2026-07-30",
            storyline_key="single-fed-rate",
            storyline_name="美联储宣布维持利率不",
            storyline_role="none",
        )
    ]

    resolved = resolver.resolve(
        [cluster],
        historical,
        datetime(2026, 7, 31, tzinfo=timezone.utc).date(),
    )

    assert resolved[0].storyline_key == f"single-{_content_hash(title)}"
    assert resolved[0].storyline_key != "single-fed-rate"
    assert resolved[0].storyline_name != "美联储宣布维持利率不"


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


def test_state_machine_does_not_attach_mismatched_history_timeline():
    cluster = ArticleCluster(
        topic_category="Business",
        articles=[_article("英国央行维持利率3.75%不变", [1.0, 0.0])],
    )
    cluster.storyline_key = "shared-rate-key"
    cluster.storyline_name = "美联储宣布维持利率不"
    historical = [
        Cluster(
            id=1,
            topic_category="Business",
            article_ids=[1],
            summary="Federal Reserve holds interest rate",
            perspectives={},
            report_date="2026-07-30",
            storyline_key="shared-rate-key",
            storyline_name="美联储宣布维持利率不",
        )
    ]

    timeline = StorylineStateMachine().timeline_for_cluster(
        cluster,
        historical,
        datetime(2026, 7, 31, tzinfo=timezone.utc).date(),
    )

    assert [event.event_type for event in timeline] == ["current"]
    assert all(event.cluster_id is None for event in timeline)


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
