"""Storyline key stability (Issue #4).

Per-run counter keys (``storyline-1``, ``single-26``) collided across days:
the same key meant 8 different topics over two weeks. The history matcher
reused stale keys for unrelated clusters via weak ~0.48 cosine matches.

Fix: keys are now content-derived hashes. Same anchor set → same key across
runs; different topic → different key. This test guards the property.
"""
from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.service.history import StorylineResolver, _content_hash
from newsprism.types import Article, ArticleCluster


def _config() -> Config:
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
                "enabled": True,
                "tab_name_max_chars": 10,
                "edge_confidence_threshold": 0.56,
                "admission_similarity": 0.62,
                "history_similarity_threshold": 0.55,
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


# ── _content_hash properties ────────────────────────────────────────────────


def test_content_hash_deterministic_for_same_inputs():
    assert _content_hash("a", "b", "c") == _content_hash("a", "b", "c")


def test_content_hash_order_invariant():
    """Sorted internally so caller order doesn't matter."""
    assert _content_hash("a", "b") == _content_hash("b", "a")


def test_content_hash_differs_for_different_inputs():
    assert _content_hash("apple") != _content_hash("banana")


def test_content_hash_ignores_empty_parts():
    assert _content_hash("apple", "", None) == _content_hash("apple")


# ── Same anchor set → same key across runs ──────────────────────────────────


def test_same_anchors_produce_same_storyline_key_across_runs():
    """Two independent resolver runs over the same anchor titles produce the
    same storyline_key — even though the old per-run counter would have
    produced storyline-1 then storyline-1 again only by coincidence.
    """
    clusters_a = [
        ArticleCluster(topic_category="World News", articles=[_article("US tariff hike announced", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Business", articles=[_article("Markets drop after tariff hike", [0.95, 0.05, 0.0])]),
    ]
    clusters_b = [
        ArticleCluster(topic_category="World News", articles=[_article("US tariff hike announced", [1.0, 0.0, 0.0])]),
        ArticleCluster(topic_category="Business", articles=[_article("Markets drop after tariff hike", [0.95, 0.05, 0.0])]),
    ]
    edges = [
        {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.85}
    ]

    resolved_a = StorylineResolver(
        _config(), summarizer=_StubSummarizer(edges), similarity_fn=lambda _t, _h: 0.0
    ).resolve(clusters_a, [], datetime(2026, 7, 21, tzinfo=timezone.utc).date())
    resolved_b = StorylineResolver(
        _config(), summarizer=_StubSummarizer(edges), similarity_fn=lambda _t, _h: 0.0
    ).resolve(clusters_b, [], datetime(2026, 7, 22, tzinfo=timezone.utc).date())

    key_a = {c.storyline_key for c in resolved_a if c.storyline_role == "core"}
    key_b = {c.storyline_key for c in resolved_b if c.storyline_role == "core"}
    assert key_a == key_b, f"expected same key across runs, got {key_a} vs {key_b}"


# ── Different topic → different key ─────────────────────────────────────────


def test_different_topics_get_different_storyline_keys():
    """Two distinct storylines in the same run get distinct keys — old
    counter-based scheme would produce storyline-1 and storyline-2 (also
    distinct), but more importantly: two runs of DIFFERENT topics would
    collide at storyline-1. Content hashing makes that impossible.
    """
    clusters_run1 = [
        ArticleCluster(topic_category="World News", articles=[_article("US tariff hike topic A", [1.0, 0.0])]),
        ArticleCluster(topic_category="Business", articles=[_article("Markets react to tariff A", [0.95, 0.05])]),
    ]
    clusters_run2 = [
        ArticleCluster(topic_category="Tech", articles=[_article("AI startup launches product B", [0.0, 1.0])]),
        ArticleCluster(topic_category="Tech", articles=[_article("AI startup raises round B", [0.05, 0.95])]),
    ]
    edges = [
        {"left_index": 0, "right_index": 1, "relation": "same_core_storyline", "confidence": 0.85}
    ]

    resolved_run1 = StorylineResolver(
        _config(), summarizer=_StubSummarizer(edges), similarity_fn=lambda _t, _h: 0.0
    ).resolve(clusters_run1, [], datetime(2026, 7, 21, tzinfo=timezone.utc).date())
    resolved_run2 = StorylineResolver(
        _config(), summarizer=_StubSummarizer(edges), similarity_fn=lambda _t, _h: 0.0
    ).resolve(clusters_run2, [], datetime(2026, 7, 22, tzinfo=timezone.utc).date())

    key_run1 = next(c.storyline_key for c in resolved_run1 if c.storyline_role == "core")
    key_run2 = next(c.storyline_key for c in resolved_run2 if c.storyline_role == "core")
    assert key_run1 != key_run2, f"different topics must not collide: {key_run1}"


# ── Singleton keys are content-stable ───────────────────────────────────────


def test_singleton_key_is_content_stable():
    """A standalone cluster with the same lead title produces the same
    ``single-{hash}`` key across days. Old per-run ``single-{N}`` collided
    (single-8 appeared on 4 different days for 4 different topics).
    """
    cluster_a = [
        ArticleCluster(topic_category="Sports", articles=[_article("Spain wins World Cup final", [1.0, 0.0])]),
    ]
    cluster_b = [
        ArticleCluster(topic_category="Sports", articles=[_article("Spain wins World Cup final", [1.0, 0.0])]),
    ]

    resolved_a = StorylineResolver(
        _config(), summarizer=_StubSummarizer([]), similarity_fn=lambda _t, _h: 0.0
    ).resolve(cluster_a, [], datetime(2026, 7, 21, tzinfo=timezone.utc).date())
    resolved_b = StorylineResolver(
        _config(), summarizer=_StubSummarizer([]), similarity_fn=lambda _t, _h: 0.0
    ).resolve(cluster_b, [], datetime(2026, 7, 22, tzinfo=timezone.utc).date())

    key_a = resolved_a[0].storyline_key
    key_b = resolved_b[0].storyline_key
    assert key_a == key_b, f"same title → same single-{{hash}} key, got {key_a} vs {key_b}"
    assert key_a.startswith("single-")
    # Hex hash suffix, not a per-run integer.
    suffix = key_a.removeprefix("single-")
    assert all(c in "0123456789abcdef" for c in suffix) and len(suffix) == 8


def test_singleton_keys_differ_for_different_titles():
    cluster_a = [
        ArticleCluster(topic_category="Tech", articles=[_article("Apple launches iPhone", [1.0, 0.0])]),
    ]
    cluster_b = [
        ArticleCluster(topic_category="World", articles=[_article("EU signs climate deal", [0.0, 1.0])]),
    ]

    resolved_a = StorylineResolver(
        _config(), summarizer=_StubSummarizer([]), similarity_fn=lambda _t, _h: 0.0
    ).resolve(cluster_a, [], datetime(2026, 7, 21, tzinfo=timezone.utc).date())
    resolved_b = StorylineResolver(
        _config(), summarizer=_StubSummarizer([]), similarity_fn=lambda _t, _h: 0.0
    ).resolve(cluster_b, [], datetime(2026, 7, 22, tzinfo=timezone.utc).date())

    assert resolved_a[0].storyline_key != resolved_b[0].storyline_key
