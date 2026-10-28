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


# ─── Corroboration prefilter ────────────────────────────────────────────────
#
# Clustering is 60-75% of LLM spend and the LLM merges articles into stories.
# It only earns that cost where there is something to merge: an article with no
# cross-source neighbour nearby cannot join a multi-source story, so paying to
# have the LLM confirm "nothing to merge" is wasted.
#
# Measured over five production days (13,234 articles, 2026-10-24..28):
#   corroborated (cross-source neighbour cos >= 0.68)  41.8%
#   solo, from an editorial-tier outlet                39.8%
#   neither                                            18.3%
# Sending only the first tier to the LLM cuts its input 58.2% while keeping
# 74/75 multi-source cards intact and losing 0/80 rendered cards.
#
# The editorial tier matters: a pure corroboration test would discard exactly
# the stories an actor can bury by flooding the feed with two-source pickups.
# Those articles skip the LLM but still become candidate clusters.

SOLO_TOPIC_CATEGORY = "单来源报道"


def _unit(embedding: list[float] | None) -> np.ndarray | None:
    if not embedding:
        return None
    vector = np.asarray(embedding, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm == 0.0:
        return None
    return vector / norm


def partition_by_corroboration(
    articles: list[Article],
    cfg: Config,
    source_tiers: dict[str, str] | None = None,
) -> tuple[list[Article], list[Article], list[Article]]:
    """Split articles into (corroborated, solo_editorial, discarded).

    ``corroborated`` articles have a neighbour from a *different* source at or
    above ``clustering.corroboration_threshold`` and go to the LLM clusterer.
    ``solo_editorial`` articles have no such neighbour but come from an
    editorial-tier outlet; they skip clustering and become single-source
    candidate clusters. The rest are discarded.

    Anything that cannot be judged — no embedding, or the prefilter disabled —
    is returned as corroborated, so a missing vector can never silently drop a
    story.
    """
    threshold = float(cfg.clustering.get("corroboration_threshold", 0.68))
    enabled = bool(cfg.clustering.get("corroboration_prefilter_enabled", True))
    tiers = source_tiers or {}

    if not enabled or len(articles) < 2:
        return list(articles), [], []

    vectors = [_unit(article.embedding) for article in articles]
    corroborated: list[Article] = []
    solo_editorial: list[Article] = []
    discarded: list[Article] = []

    for index, article in enumerate(articles):
        vector = vectors[index]
        if vector is None:
            corroborated.append(article)
            continue
        has_neighbour = False
        for other_index, other in enumerate(articles):
            if other_index == index:
                continue
            other_vector = vectors[other_index]
            if other_vector is None:
                continue
            if (other.source_name or "") == (article.source_name or ""):
                continue
            if float(np.dot(vector, other_vector)) >= threshold:
                has_neighbour = True
                break
        if has_neighbour:
            corroborated.append(article)
        elif tiers.get(article.source_name or "") == "editorial":
            solo_editorial.append(article)
        else:
            discarded.append(article)

    logger.info(
        "Corroboration prefilter (threshold=%.2f): %d input -> %d corroborated (LLM), "
        "%d solo-editorial (no LLM), %d discarded",
        threshold,
        len(articles),
        len(corroborated),
        len(solo_editorial),
        len(discarded),
    )
    return corroborated, solo_editorial, discarded


def solo_clusters(articles: list[Article]) -> list[ArticleCluster]:
    """Wrap solo-editorial articles as single-source candidate clusters.

    The LLM would have produced the same shape (a cluster of one) while paying
    for the call; constructing it locally is free and keeps the story in the
    candidate pool for the impact window.

    ``topic_category`` is the label the summarizer sees in its prompt, so it
    carries the article's own headline instead of a placeholder: LLM clusters
    arrive with a real event label and a solo entry should not look like a
    different kind of object downstream.
    """
    return [
        ArticleCluster(
            topic_category=(article.title or "").strip()[:60] or SOLO_TOPIC_CATEGORY,
            articles=[article],
        )
        for article in articles
    ]
