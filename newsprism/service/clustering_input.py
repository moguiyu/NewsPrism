"""Pre-LLM clustering input compaction.

Collapses only very high-confidence same-source near-duplicates so the LLM
clustering pass sees representatives. Collapsed articles are re-expanded into
the cluster of their representative after clustering, so the final article set
is unchanged. Disabled by default in code; enabled through
``clustering.compaction_enabled`` in config.yaml.

Layer: service (imports types and config; never imports runtime).
"""
from __future__ import annotations

import logging

import numpy as np
from rapidfuzz import fuzz

from newsprism.config import Config
from newsprism.types import Article, ArticleCluster, is_real_article

logger = logging.getLogger(__name__)


def _cosine(left: list[float] | None, right: list[float] | None) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    a = np.asarray(left, dtype=float)
    b = np.asarray(right, dtype=float)
    norm_a = float(np.linalg.norm(a))
    norm_b = float(np.linalg.norm(b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def compact_same_source_near_duplicates(
    articles: list[Article],
    cfg: Config,
) -> tuple[list[Article], dict[int, list[Article]]]:
    """Return (representative_articles, representative_id -> collapsed_articles).

    The input order is preserved for representatives. A collapsed article is
    keyed by ``id(representative)``; callers must reattach it only when that
    representative object is present in a cluster.
    """
    enabled = bool(cfg.clustering.get("compaction_enabled", False))
    if not enabled or len(articles) < 2:
        return list(articles), {}

    semantic_floor = float(cfg.clustering.get("compaction_semantic_similarity", 0.92))
    title_floor = float(cfg.clustering.get("compaction_title_ratio", 90))

    representatives: list[Article] = []
    collapsed: dict[int, list[Article]] = {}
    kept_by_source: dict[str, list[Article]] = {}

    for article in sorted(articles, key=lambda a: a.published_at, reverse=True):
        if not is_real_article(article):
            representatives.append(article)
            continue
        source_kept = kept_by_source.setdefault(article.source_name, [])
        representative: Article | None = None
        for existing in source_kept:
            similarity = _cosine(article.embedding, existing.embedding)
            if similarity < semantic_floor:
                continue
            title_ratio = fuzz.token_set_ratio(article.title or "", existing.title or "")
            if title_ratio < title_floor:
                continue
            representative = existing
            break

        if representative is None:
            representatives.append(article)
            source_kept.append(article)
        else:
            collapsed.setdefault(id(representative), []).append(article)

    if collapsed:
        logger.info(
            "Clustering compaction: %d articles -> %d representatives (%d collapsed same-source updates)",
            len(articles),
            len(representatives),
            sum(len(items) for items in collapsed.values()),
        )
    return representatives, collapsed


def expand_clusters_with_collapsed_articles(
    clusters: list[ArticleCluster],
    collapsed: dict[int, list[Article]],
) -> list[ArticleCluster]:
    """Reattach collapsed same-source articles to their representative cluster."""
    if not collapsed:
        return clusters
    for cluster in clusters:
        extras: list[Article] = []
        for article in list(cluster.articles):
            extras.extend(collapsed.pop(id(article), []))
        if extras:
            cluster.articles.extend(extras)
    return clusters
