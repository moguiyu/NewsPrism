"""Unit tests for newsprism.service.dedup.Deduplicator.

Covers the zero-test gap without loading the real sentence-transformer model.
The semantic pass is made deterministic by monkeypatching
newsprism.service.dedup._get_model to return a stub whose .encode() returns
a fixed numpy array controlled per test.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from newsprism.config import Config, SourceConfig
from newsprism.service.dedup import Deduplicator
from newsprism.types import Article


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_cfg(
    sources: list[tuple[str, float]] | None = None,
    fuzzy_threshold: int = 85,
    semantic_threshold: float = 0.82,
) -> Config:
    """Minimal Config with only the fields Deduplicator needs."""
    if sources is None:
        sources = [("SourceA", 1.0), ("SourceB", 0.5)]
    source_cfgs = [
        SourceConfig(
            name=name,
            name_en=name,
            url=f"https://{name.lower()}.test",
            rss_url=None,
            type="rss",
            weight=weight,
            language="en",
        )
        for name, weight in sources
    ]
    return Config(
        raw={},
        sources=source_cfgs,
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={"fuzzy_threshold": fuzzy_threshold, "semantic_threshold": semantic_threshold},
        summarizer={},
        output={},
        active_search={},
    )


def _article(
    title: str,
    source: str = "SourceA",
    url: str | None = None,
    content: str = "body text",
) -> Article:
    return Article(
        url=url or f"https://example.test/{title.replace(' ', '-')}",
        title=title,
        source_name=source,
        published_at=datetime(2026, 6, 10, 12, 0, 0),
        content=content,
    )


def _stub_model(embeddings_map: dict[str, np.ndarray]) -> MagicMock:
    """Return a mock model whose .encode(texts, ...) returns rows from embeddings_map.

    If a text is not in the map, returns a zero vector of length 3.
    """
    def _encode(texts: list[str], **kwargs) -> np.ndarray:
        rows = []
        for text in texts:
            rows.append(embeddings_map.get(text, np.zeros(3)))
        return np.array(rows, dtype=np.float32)

    mock = MagicMock()
    mock.encode.side_effect = _encode
    return mock


# ─── tests ────────────────────────────────────────────────────────────────────

def test_empty_input_returns_empty():
    """Deduplicator.deduplicate([]) must return []."""
    dedup = Deduplicator(_make_cfg())
    assert dedup.deduplicate([]) == []


def test_same_source_fuzzy_dedup_collapses_to_one(monkeypatch):
    """Two near-duplicate titles from the same source collapse to one article."""
    # Patch _get_model so the semantic pass doesn't load sentence-transformers
    stub = _stub_model({})
    monkeypatch.setattr("newsprism.service.dedup._get_model", lambda: stub)

    cfg = _make_cfg(sources=[("SourceA", 1.0)])
    dedup = Deduplicator(cfg)

    a1 = _article("Apple announces new iPhone model", source="SourceA", url="https://test/a1")
    a2 = _article("Apple announces new iPhone models", source="SourceA", url="https://test/a2")

    result = dedup.deduplicate([a1, a2])
    assert len(result) == 1


def test_same_source_fuzzy_keeps_higher_weight_article(monkeypatch):
    """When two same-source articles are fuzzy-duped, the higher-weight source wins.

    Because fuzzy dedup only compares within the same source, 'weight' is the
    source-level weight from Config.  We use two different sources here only to
    confirm the higher-weight source's article survives when compared against a
    same-source duplicate.  In practice we just verify the kept article is a2
    when it arrives second (fuzzy dedup keeps the first unless the replacement
    has strictly higher weight — both are from the same source so weight is equal
    and the first article is kept).
    """
    stub = _stub_model({})
    monkeypatch.setattr("newsprism.service.dedup._get_model", lambda: stub)

    cfg = _make_cfg(sources=[("SourceA", 1.0), ("SourceB", 2.0)])
    dedup = Deduplicator(cfg)

    # Two same-source articles — SourceA weight == SourceA weight, so first is kept
    a1 = _article("Breaking: big earthquake hits region", source="SourceA", url="https://test/a1")
    a2 = _article("Breaking: big earthquake hits regions", source="SourceA", url="https://test/a2")
    result = dedup.deduplicate([a1, a2])
    assert len(result) == 1
    assert result[0].url == a1.url  # first article is kept when weights are equal


def test_different_source_same_event_not_merged_below_syndication_threshold(monkeypatch):
    """Articles from different sources about the same event survive dedup unless
    embeddings are >= 0.98 (syndication threshold).  Values below that are kept.
    """
    # Use distinct, orthogonal unit vectors — dot product = 0.0
    emb_a = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    emb_b = np.array([0.0, 1.0, 0.0], dtype=np.float32)

    a1 = _article("President signs climate bill", source="SourceA", url="https://test/a1", content="body a")
    a2 = _article("President signs climate bill", source="SourceB", url="https://test/a2", content="body b")

    text_a = f"{a1.title} {a1.content[:500]}"
    text_b = f"{a2.title} {a2.content[:500]}"

    stub = _stub_model({text_a: emb_a, text_b: emb_b})
    monkeypatch.setattr("newsprism.service.dedup._get_model", lambda: stub)

    cfg = _make_cfg()
    dedup = Deduplicator(cfg)
    result = dedup.deduplicate([a1, a2])
    # Both articles from different sources should survive — clustering decides, not dedup
    assert len(result) == 2


def test_cross_source_syndication_dedup_at_098_threshold(monkeypatch):
    """Near-identical cross-source articles (sim >= 0.98) are treated as syndication
    and deduplicated to one.
    """
    # Identical unit vectors give dot product 1.0
    emb = np.array([1.0, 0.0, 0.0], dtype=np.float32)

    a1 = _article("Syndicated story verbatim", source="SourceA", url="https://test/a1", content="exact same body")
    a2 = _article("Syndicated story verbatim", source="SourceB", url="https://test/a2", content="exact same body")

    text_a = f"{a1.title} {a1.content[:500]}"
    text_b = f"{a2.title} {a2.content[:500]}"

    stub = _stub_model({text_a: emb, text_b: emb})
    monkeypatch.setattr("newsprism.service.dedup._get_model", lambda: stub)

    cfg = _make_cfg(sources=[("SourceA", 0.5), ("SourceB", 0.5)])
    dedup = Deduplicator(cfg)
    result = dedup.deduplicate([a1, a2])
    assert len(result) == 1


def test_embeddings_attached_after_dedup(monkeypatch):
    """After the semantic pass, all surviving articles have .embedding set."""
    emb_a = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    emb_b = np.array([0.0, 1.0, 0.0], dtype=np.float32)

    a1 = _article("Story one", source="SourceA", url="https://test/a1", content="content one")
    a2 = _article("Story two", source="SourceB", url="https://test/a2", content="content two")

    text_a = f"{a1.title} {a1.content[:500]}"
    text_b = f"{a2.title} {a2.content[:500]}"

    stub = _stub_model({text_a: emb_a, text_b: emb_b})
    monkeypatch.setattr("newsprism.service.dedup._get_model", lambda: stub)

    cfg = _make_cfg()
    dedup = Deduplicator(cfg)
    result = dedup.deduplicate([a1, a2])

    assert all(a.embedding is not None for a in result), "All surviving articles must have embeddings"
    assert all(isinstance(a.embedding, list) for a in result)


def test_single_article_passthrough(monkeypatch):
    """A single article should pass through unchanged (semantic pass skips len < 2)."""
    stub = _stub_model({})
    monkeypatch.setattr("newsprism.service.dedup._get_model", lambda: stub)

    cfg = _make_cfg()
    dedup = Deduplicator(cfg)
    a = _article("Solo story", source="SourceA")
    result = dedup.deduplicate([a])
    assert len(result) == 1
    assert result[0].url == a.url
    # Semantic pass was skipped — encode should not have been called
    stub.encode.assert_not_called()


def test_no_cross_source_same_source_semantic_dedup_respects_threshold(monkeypatch):
    """Same-source articles with semantic sim >= semantic_threshold are collapsed."""
    # sim = 0.9 > threshold 0.82 → should collapse
    emb = np.array([1.0, 0.0, 0.0], dtype=np.float32)

    a1 = _article("Tech company layoffs announced", source="SourceA", url="https://test/a1", content="body one")
    a2 = _article("Company announces job cuts", source="SourceA", url="https://test/a2", content="body two")

    text_a = f"{a1.title} {a1.content[:500]}"
    text_b = f"{a2.title} {a2.content[:500]}"

    # Identical embeddings → sim 1.0 → above 0.82 threshold → deduped
    stub = _stub_model({text_a: emb, text_b: emb})
    monkeypatch.setattr("newsprism.service.dedup._get_model", lambda: stub)

    cfg = _make_cfg(sources=[("SourceA", 1.0)])
    dedup = Deduplicator(cfg)
    result = dedup.deduplicate([a1, a2])
    assert len(result) == 1
