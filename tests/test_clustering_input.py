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
