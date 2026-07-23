"""Topic-centric clusterer — the heart of NewsPrism.

Groups articles about the same story from different sources into clusters.
Each cluster represents one "topic" for the daily report, and surfaces
multiple perspectives (source viewpoints) on that topic.

Algorithm:
1. Use pre-computed embeddings from dedup pass (or compute them now).
2. Group articles with cosine similarity above threshold that were published
   within the time window and pass a title-ngram coherence check.
3. Within each cluster, articles from the same source are further deduped
   (keep the most representative one).
4. Clusters with only one source are kept as single-perspective items.
5. Sort clusters by number of sources (multi-perspective first).

Layer: service (imports types, config; never imports repo or runtime)
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np

from newsprism.config import Config
from newsprism.service.embeddings import get_model as _get_model
from newsprism.types import Article, ArticleCluster

logger = logging.getLogger(__name__)

# Sentinel for sorting when published_at is None (searched articles with no
# recoverable date). Puts them last in descending-time ordering.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class Clusterer:
    def __init__(self, cfg: Config) -> None:
        self.sem_threshold = cfg.clustering.get("semantic_threshold", 0.72)
        self.strong_threshold = cfg.clustering.get("strong_similarity_threshold", max(self.sem_threshold + 0.1, 0.82))
        self.title_ngram_threshold = cfg.clustering.get("coherence_title_ngram_threshold", 0.12)
        self.time_window_h = cfg.clustering.get("time_window_hours", 48)
        # Store source regions for diversity ranking
        self.source_regions = {s.name: s.region for s in cfg.sources}

    def cluster(self, articles: list[Article]) -> list[ArticleCluster]:
        """Group articles into event clusters using graph connectivity."""
        if not articles:
            return []

        self._ensure_embeddings(articles)

        sorted_articles = sorted(articles, key=lambda a: a.published_at, reverse=True)
        adjacency: dict[int, set[int]] = defaultdict(set)

        for i, article in enumerate(sorted_articles):
            for j in range(i + 1, len(sorted_articles)):
                other = sorted_articles[j]
                dt = abs((article.published_at - other.published_at).total_seconds())
                if dt > self.time_window_h * 3600:
                    continue

                sim = self._cosine_sim(article, other)
                if sim < self.sem_threshold:
                    continue

                if not self._passes_coherence(article, other, sim):
                    continue

                adjacency[i].add(j)
                adjacency[j].add(i)

        clusters: list[ArticleCluster] = []
        visited: set[int] = set()
        for idx in range(len(sorted_articles)):
            if idx in visited:
                continue

            component = self._collect_component(idx, adjacency)
            visited |= component
            cluster_articles = self._prune_same_source(component, sorted_articles, adjacency)
            label = cluster_articles[0].title[:60] if cluster_articles else "Event"
            clusters.append(ArticleCluster(topic_category=label, articles=cluster_articles))

        # Sort primarily by regional diversity (number of unique countries/regions)
        # then by number of sources, then by number of articles.
        def _cluster_sort_key(c: ArticleCluster):
            regions = {self.source_regions.get(s, "") for s in c.sources}
            regions.discard("") # Remove empty strings just in case
            return (len(regions), len(c.sources), len(c.articles))

        clusters.sort(key=_cluster_sort_key, reverse=True)

        multi = sum(1 for c in clusters if c.is_multi_source)
        logger.info(
            "Clustering: %d articles → %d clusters (%d multi-source)",
            len(articles), len(clusters), multi,
        )
        return clusters

    def _passes_coherence(self, article: Article, other: Article, sim: float) -> bool:
        if sim >= self.strong_threshold:
            return True
        return self._title_ngram_overlap(article.title, other.title) >= self.title_ngram_threshold

    def _collect_component(self, start: int, adjacency: dict[int, set[int]]) -> set[int]:
        stack = [start]
        component: set[int] = set()
        while stack:
            idx = stack.pop()
            if idx in component:
                continue
            component.add(idx)
            stack.extend(sorted(adjacency.get(idx, ()), reverse=True))
        return component

    def _prune_same_source(
        self,
        component: set[int],
        articles: list[Article],
        adjacency: dict[int, set[int]],
    ) -> list[Article]:
        component_set = set(component)
        by_source: dict[str, list[int]] = defaultdict(list)
        for idx in component:
            by_source[articles[idx].source_name].append(idx)

        kept_indices: list[int] = []
        for source_indices in by_source.values():
            best_idx = max(
                source_indices,
                key=lambda idx: (
                    sum(1 for neighbor in adjacency.get(idx, ()) if neighbor in component_set),
                    len(articles[idx].content),
                    (articles[idx].published_at.timestamp() if articles[idx].published_at else 0.0),
                ),
            )
            kept_indices.append(best_idx)

        kept_indices.sort(
            key=lambda idx: articles[idx].published_at or _EPOCH,
            reverse=True,
        )
        return [articles[idx] for idx in kept_indices]

    def _embedding_text(self, article: Article) -> str:
        """Return sanitized embedding text, falling back to title-only on bad content."""
        content = article.content or ""
        fallback_reason: str | None = None
        if content:
            # Detect mojibake: Latin-1 surrogate range chars dominate what should be CJK text
            sample = content[:200]
            latin1_count = sum(1 for c in sample if 0x80 <= ord(c) <= 0xFF)
            if latin1_count > len(sample) * 0.25:
                fallback_reason = "mojibake"
                content = ""
            # Detect boilerplate: very short with no sentence-ending punctuation
            elif len(content) < 120 and not any(c in content for c in '。？！.?!\n'):
                fallback_reason = "boilerplate"
                content = ""
        if fallback_reason is not None:
            logger.debug(
                "Embedding fallback to title-only for article '%s' (content: %s)",
                article.title[:40],
                fallback_reason,
            )
        return f"{article.title} {content[:500]}".strip() if content else article.title

    def _ensure_embeddings(self, articles: list[Article]) -> None:
        needs_embedding = [a for a in articles if a.embedding is None]
        if not needs_embedding:
            return
        model = _get_model()
        texts = [self._embedding_text(a)[:600] for a in needs_embedding]
        embs = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
        for article, emb in zip(needs_embedding, embs):
            article.embedding = emb.tolist()

    def _cosine_sim(self, a: Article, b: Article) -> float:
        if a.embedding is None or b.embedding is None:
            return 0.0
        va = np.array(a.embedding)
        vb = np.array(b.embedding)
        return float(np.dot(va, vb))  # already normalized

    def _title_ngram_overlap(self, left: str, right: str) -> float:
        left_ngrams = self._char_ngrams(left)
        right_ngrams = self._char_ngrams(right)
        if not left_ngrams or not right_ngrams:
            return 0.0
        union = left_ngrams | right_ngrams
        if not union:
            return 0.0
        return len(left_ngrams & right_ngrams) / len(union)

    def _char_ngrams(self, text: str, n: int = 3) -> set[str]:
        compact = re.sub(r"[^\w\u4e00-\u9fff]+", "", text.lower())
        if not compact:
            return set()
        if len(compact) <= n:
            return {compact}
        return {compact[i : i + n] for i in range(len(compact) - n + 1)}
