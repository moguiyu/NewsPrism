"""Freshness evaluation — classifies clusters as new, developing, or stale.

Compares candidate clusters against historical clusters to determine:
- NEW: First coverage, no similar prior cluster
- DEVELOPING: Continuation with new sources/information
- STALE: Same information already covered, skip from report

Layer: service (imports types, config; never imports repo or runtime)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
from sentence_transformers import SentenceTransformer

from newsprism.config import Config
from newsprism.types import ArticleCluster, Cluster

logger = logging.getLogger(__name__)

_MODEL: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")
    return _MODEL


@dataclass
class FreshnessResult:
    """Result of freshness evaluation for a cluster."""
    state: str  # "new" | "developing" | "stale"
    continues_cluster_id: int | None = None
    similarity_score: float = 0.0
    new_sources: list[str] | None = None


class FreshnessEvaluator:
    """Evaluates cluster freshness against historical clusters."""

    def __init__(self, cfg: Config) -> None:
        self.similarity_threshold = 0.85  # High threshold for story match
        self.window_days = cfg.dedup.get("window_days", 3)
        self._embedding_cache: dict[int, np.ndarray] = {}
        self._text_embedding_cache: dict[str, np.ndarray] = {}

    def evaluate(
        self,
        cluster: ArticleCluster,
        summary: str,
        historical_clusters: list[Cluster],
    ) -> FreshnessResult:
        """Evaluate freshness of a candidate cluster against history.

        Args:
            cluster: The candidate cluster being evaluated
            summary: The LLM-generated summary for this cluster
            historical_clusters: Clusters from previous days to compare against

        Returns:
            FreshnessResult with state, continuation info, and similarity score
        """
        if not historical_clusters or self.window_days <= 0:
            return FreshnessResult(state="new")

        # Compute embedding for the new cluster's summary
        new_embedding = self._get_text_embedding(summary)

        # Find the most similar historical cluster
        best_match: Cluster | None = None
        best_similarity = 0.0

        for hist in historical_clusters:
            if not hist.summary:
                continue
            hist_embedding = self._get_cached_embedding(hist)
            similarity = float(np.dot(new_embedding, hist_embedding))

            if similarity > best_similarity:
                best_similarity = similarity
                best_match = hist

        # No similar cluster found -> new story
        if best_similarity < self.similarity_threshold or best_match is None:
            logger.debug(
                "Freshness: NEW story '%s' (best similarity %.2f)",
                cluster.articles[0].title[:50] if cluster.articles else "empty",
                best_similarity,
            )
            return FreshnessResult(state="new", similarity_score=best_similarity)

        # Found similar cluster - check if it has new sources
        new_sources = self._find_new_sources(cluster, best_match)

        if new_sources:
            # Has new sources -> developing story
            logger.debug(
                "Freshness: DEVELOPING story '%s' (similarity %.2f, %d new sources: %s)",
                cluster.articles[0].title[:50] if cluster.articles else "empty",
                best_similarity,
                len(new_sources),
                ", ".join(new_sources[:3]),
            )
            return FreshnessResult(
                state="developing",
                continues_cluster_id=best_match.id,
                similarity_score=best_similarity,
                new_sources=new_sources,
            )
        else:
            # No new sources -> stale repetition
            logger.debug(
                "Freshness: STALE story '%s' (similarity %.2f, no new sources)",
                cluster.articles[0].title[:50] if cluster.articles else "empty",
                best_similarity,
            )
            return FreshnessResult(
                state="stale",
                continues_cluster_id=best_match.id,
                similarity_score=best_similarity,
            )

    def _compute_embedding(self, text: str) -> np.ndarray:
        """Compute normalized embedding for a text string."""
        model = _get_model()
        embedding = model.encode([text], normalize_embeddings=True, show_progress_bar=False)
        return embedding[0]

    def score_text_to_historical_cluster(self, text: str, historical_cluster: Cluster) -> float:
        """Score similarity between free text and a historical cluster summary."""
        if not historical_cluster.summary:
            return 0.0
        new_embedding = self._get_text_embedding(text)
        hist_embedding = self._get_cached_embedding(historical_cluster)
        return float(np.dot(new_embedding, hist_embedding))

    def _get_cached_embedding(self, cluster: Cluster) -> np.ndarray:
        """Get embedding for a historical cluster, using cache if available."""
        if cluster.id in self._embedding_cache:
            return self._embedding_cache[cluster.id]

        embedding = self._compute_embedding(cluster.summary)
        if cluster.id is not None:
            self._embedding_cache[cluster.id] = embedding
        return embedding

    def _get_text_embedding(self, text: str) -> np.ndarray:
        """Get embedding for free text, using cache if available."""
        cached = self._text_embedding_cache.get(text)
        if cached is not None:
            return cached

        embedding = self._compute_embedding(text)
        self._text_embedding_cache[text] = embedding
        return embedding

    def _find_new_sources(
        self,
        new_cluster: ArticleCluster,
        old_cluster: Cluster,
    ) -> list[str]:
        """Find sources in new cluster that weren't in the old cluster."""
        # Get sources from old cluster's article IDs (need to look up)
        # For now, we check if the new cluster has more unique sources
        # than what we can infer from the old cluster's perspectives
        old_sources = set(old_cluster.perspectives.keys())
        new_sources = set(new_cluster.sources)

        # Sources that are new compared to the old cluster
        added = new_sources - old_sources
        return list(added)

    def classify_all(
        self,
        cluster_summaries: list[tuple[ArticleCluster, str]],
        historical_clusters: list[Cluster],
    ) -> list[tuple[ArticleCluster, str, FreshnessResult]]:
        """Classify multiple clusters and return results.

        Args:
            cluster_summaries: List of (cluster, summary) tuples
            historical_clusters: Clusters from previous days

        Returns:
            List of (cluster, summary, FreshnessResult) tuples
        """
        results = []
        for cluster, summary in cluster_summaries:
            freshness = self.evaluate(cluster, summary, historical_clusters)
            results.append((cluster, summary, freshness))
        return results
