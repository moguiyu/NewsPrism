"""Pre-LLM same-source compaction and post-cluster re-expansion."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from newsprism.config import Config
from newsprism.service.clustering_input import (
    compact_same_source_near_duplicates,
    expand_clusters_with_collapsed_articles,
)
from newsprism.types import Article, ArticleCluster


def _config(enabled: bool = True) -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={
            "compaction_enabled": enabled,
            "compaction_semantic_similarity": 0.92,
            "compaction_title_ratio": 90,
        },
        dedup={},
        summarizer={},
        output={},
        active_search={},
    )


def _article(index: int, source: str = "Reuters", title: str = "Same event update") -> Article:
    return Article(
        url=f"https://example.com/{source}/{index}",
        title=title,
        source_name=source,
        published_at=datetime.now(timezone.utc) + timedelta(minutes=index),
        content=f"Shared event body {index}",
        embedding=[1.0, 0.0, 0.0],
    )


def test_same_source_near_duplicates_are_compacted_and_reattached():
    newer = _article(2)
    older = _article(1)
    cfg = _config()

    representatives, collapsed = compact_same_source_near_duplicates([older, newer], cfg)
    assert len(representatives) == 1
    assert representatives[0] is newer
    assert collapsed[id(newer)] == [older]

    cluster = ArticleCluster(topic_category="Event", articles=[newer])
    expanded = expand_clusters_with_collapsed_articles([cluster], collapsed)
    assert expanded[0].articles == [newer, older]


def test_different_same_source_events_are_not_compacted():
    cfg = _config()
    articles = [
        _article(1, title="Central bank raises rates"),
        _article(2, title="Parliament passes budget bill"),
    ]
    representatives, collapsed = compact_same_source_near_duplicates(articles, cfg)
    assert len(representatives) == 2
    assert collapsed == {}


def test_compaction_is_off_by_default():
    cfg = _config(enabled=False)
    articles = [_article(1), _article(2)]
    representatives, collapsed = compact_same_source_near_duplicates(articles, cfg)
    assert representatives == articles
    assert collapsed == {}


# ─── Corroboration prefilter ────────────────────────────────────────────────


def _cfg_prefilter(enabled: bool = True, threshold: float = 0.68) -> Config:
    cfg = _config(enabled=False)
    cfg.clustering["corroboration_prefilter_enabled"] = enabled
    cfg.clustering["corroboration_threshold"] = threshold
    return cfg


def _vec(x: float, y: float) -> list[float]:
    return [x, y, 0.0]


def _art(index: int, source: str, embedding: list[float]) -> Article:
    return _article(index, source).model_copy(update={"embedding": embedding}) if hasattr(
        Article, "model_copy"
    ) else Article(
        url=f"https://example.com/{source}/{index}",
        title=f"Story {index}",
        source_name=source,
        published_at=datetime.now(timezone.utc),
        content="body",
        embedding=embedding,
    )


def _partition(articles, cfg, tiers):
    from newsprism.service.clustering_input import partition_by_corroboration

    return partition_by_corroboration(articles, cfg, tiers)


def test_corroborated_articles_go_to_the_llm():
    """Two outlets on the same vector are corroborated and reach the clusterer."""
    cfg = _cfg_prefilter()
    arts = [_art(1, "Reuters", _vec(1, 0)), _art(2, "NPR", _vec(0.99, 0.05))]
    corroborated, solo, discarded = _partition(arts, cfg, {"Reuters": "editorial", "NPR": "editorial"})
    assert len(corroborated) == 2
    assert solo == [] and discarded == []


def test_solo_editorial_article_skips_the_llm_but_is_kept():
    """A single-source story from an editorial outlet must not be discarded.

    This is the product requirement behind the split: an actor can flood the
    feed with pickups that carry two source names, and a story only one outlet
    reported would be filtered away by a pure corroboration test.
    """
    cfg = _cfg_prefilter()
    arts = [_art(1, "Ukrinform", _vec(1, 0)), _art(2, "TechCrunch", _vec(0, 1))]
    corroborated, solo, discarded = _partition(
        arts, cfg, {"Ukrinform": "editorial", "TechCrunch": "tech"}
    )
    assert [a.source_name for a in corroborated] == []
    assert [a.source_name for a in solo] == ["Ukrinform"]
    assert [a.source_name for a in discarded] == ["TechCrunch"]


def test_same_source_neighbours_do_not_count_as_corroboration():
    """Two articles from one outlet are not independent corroboration."""
    cfg = _cfg_prefilter()
    arts = [_art(1, "Reuters", _vec(1, 0)), _art(2, "Reuters", _vec(1, 0))]
    corroborated, solo, discarded = _partition(arts, cfg, {"Reuters": "editorial"})
    assert corroborated == []
    assert len(solo) == 2


def test_prefilter_is_disabled_by_config():
    """Disabling the prefilter restores the previous behaviour exactly."""
    cfg = _cfg_prefilter(enabled=False)
    arts = [_art(1, "Ukrinform", _vec(1, 0)), _art(2, "TechCrunch", _vec(0, 1))]
    corroborated, solo, discarded = _partition(
        arts, cfg, {"Ukrinform": "editorial", "TechCrunch": "tech"}
    )
    assert len(corroborated) == 2
    assert solo == [] and discarded == []


def test_articles_without_embeddings_are_never_discarded():
    """A missing embedding must fail open — never silently drop the article."""
    cfg = _cfg_prefilter()
    arts = [_art(1, "TechCrunch", _vec(0, 1))]
    arts[0].embedding = None
    corroborated, solo, discarded = _partition(arts, cfg, {"TechCrunch": "tech"})
    assert len(corroborated) == 1
    assert solo == [] and discarded == []


def test_solo_editorial_articles_become_single_source_clusters():
    """The solo tier must still produce candidate clusters for the window."""
    from newsprism.service.clustering_input import solo_clusters

    cfg = _cfg_prefilter()
    solo = [_art(1, "Ukrinform", _vec(1, 0)), _art(2, "The Guardian", _vec(0, 1))]
    clusters = solo_clusters(solo)
    assert len(clusters) == 2
    assert all(len(c.articles) == 1 for c in clusters)
    # The label reaches the summarizer prompt, so it carries the headline.
    assert {c.topic_category for c in clusters} == {"Story 1", "Story 2"}


def test_clusterer_sends_only_corroborated_articles_to_the_llm(monkeypatch):
    """End-to-end: the LLM sees corroborated articles; solos become local clusters."""
    from newsprism.service import llm_clusterer as module

    cfg = _cfg_prefilter()
    cfg.sources = [
        type("S", (), {"name": "Reuters", "tier": "editorial", "region": "us"})(),
        type("S", (), {"name": "NPR", "tier": "editorial", "region": "us"})(),
        type("S", (), {"name": "Ukrinform", "tier": "editorial", "region": "ua"})(),
    ]
    clusterer = module.LLMClusterer(cfg)

    seen: list[int] = []

    def fake_chunked(articles, report_date=None):
        seen.extend(len(articles) for _ in [0])
        seen.clear()
        seen.append(len(articles))
        return [ArticleCluster(topic_category="Event", articles=list(articles))]

    monkeypatch.setattr(clusterer, "_cluster_chunked", fake_chunked)

    arts = [
        _art(1, "Reuters", _vec(1, 0)),
        _art(2, "NPR", _vec(0.99, 0.01)),
        _art(3, "Ukrinform", _vec(0, 1)),
    ]
    clusters = clusterer.cluster(arts)

    assert seen == [2], "the LLM must receive only the two corroborated articles"
    assert len(clusters) == 2, "the solo article still yields a candidate cluster"
    assert any(len(c.articles) == 1 for c in clusters)


def test_clusterer_falls_back_to_full_pool_when_nothing_is_corroborated(monkeypatch):
    """If the prefilter empties the LLM input, clustering must not silently stop."""
    from newsprism.service import llm_clusterer as module

    cfg = _cfg_prefilter()
    cfg.sources = [type("S", (), {"name": "TechCrunch", "tier": "tech", "region": "us"})()]
    clusterer = module.LLMClusterer(cfg)

    seen: list[int] = []

    def fake_chunked(articles, report_date=None):
        seen.append(len(articles))
        return [ArticleCluster(topic_category="Event", articles=list(articles))]

    monkeypatch.setattr(clusterer, "_cluster_chunked", fake_chunked)
    arts = [_art(1, "TechCrunch", _vec(1, 0)), _art(2, "TechCrunch", _vec(0, 1))]
    clusterer.cluster(arts)

    assert seen == [2], "the full pool is retried so news is never lost to a filter"


# ─── Beta-only aggressive outlet-alias normalisation ────────────────────────


def test_alias_normalisation_merges_domain_and_brand_names():
    """One outlet must count once, whatever name its article arrived under.

    Measured on 95 rendered cards (2026-10-24..28): 2 inflated their source
    count this way — 8568 counted Медуза and meduza.io as two outlets, 8483 did
    the same with Ars Technica and arstechnica.com.
    """
    from newsprism.service.evidence import normalise_source_name

    aliases = {
        "Медуза": "meduza.io",
        "meduza.io": "meduza.io",
        "Ars Technica": "arstechnica.com",
        "arstechnica.com": "arstechnica.com",
        "Kyiv Independent": "kyivindependent.com",
        "kyivindependent.com": "kyivindependent.com",
    }
    assert normalise_source_name("Медуза", aliases) == normalise_source_name(
        "meduza.io", aliases
    )
    assert normalise_source_name("Ars Technica", aliases) == normalise_source_name(
        "arstechnica.com", aliases
    )
    # an unknown outlet is returned unchanged, not collapsed
    assert normalise_source_name("NPR", aliases) == "NPR"


def test_alias_normalisation_counts_one_source_for_a_dual_name_card():
    """The evidence line must not count one outlet twice."""
    from newsprism.service.evidence import evidence_counts
    from newsprism.types import Article, ArticleCluster
    from datetime import datetime, timezone

    def art(url, name):
        return Article(
            url=url, title="t", source_name=name,
            published_at=datetime.now(tz=timezone.utc), content="b",
        )

    cluster = ArticleCluster(
        topic_category="Event",
        articles=[art("https://meduza.io/1", "meduza.io"), art("https://meduza.io/2", "Медуза")],
    )
    aliases = {"Медуза": "meduza.io", "meduza.io": "meduza.io"}
    sources, _regions = evidence_counts(cluster, source_aliases=aliases)
    assert sources == 1, "one outlet, two names, one source"

    unnormalised, _ = evidence_counts(cluster)
    assert unnormalised == 2, "the bug this guards: counted as two outlets"
