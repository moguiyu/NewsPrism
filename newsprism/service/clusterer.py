"""Topic-centric clusterer — the heart of NewsPrism.

Groups articles about the same story from different sources into clusters.
Each cluster represents one "topic" for the daily report, and surfaces
multiple perspectives (source viewpoints) on that topic.

Algorithm:
1. Use pre-computed embeddings from dedup pass (or compute them now).
2. Group articles that share ≥1 topic category AND have cosine similarity
   above threshold AND were published within the time window.
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

import numpy as np
from sentence_transformers import SentenceTransformer

from newsprism.config import Config
from newsprism.types import Article, ArticleCluster

logger = logging.getLogger(__name__)

_MODEL: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")
    return _MODEL


class Clusterer:
    def __init__(self, cfg: Config) -> None:
        self.sem_threshold = cfg.clustering.get("semantic_threshold", 0.72)
        self.strong_threshold = cfg.clustering.get("strong_similarity_threshold", max(self.sem_threshold + 0.1, 0.82))
        self.title_ngram_threshold = cfg.clustering.get("coherence_title_ngram_threshold", 0.12)
        self.time_window_h = cfg.clustering.get("time_window_hours", 48)
        # Store source regions for diversity ranking
        self.source_regions = {s.name: s.region for s in cfg.sources}
        # Topic equivalence mapping
        self.topic_equivalence = cfg.topic_equivalence
        # Pre-compute expanded topic sets for efficiency
        self._topic_cache: dict[str, set[str]] = {}

    def _expand_topics(self, topics: list[str]) -> set[str]:
        """Expand topic list to include all equivalent topics.

        Bidirectional and transitive: if A≡B and B≡C, then A≡C.
        Results are cached for efficiency.
        """
        result = set(topics)
        for topic in topics:
            if topic in self._topic_cache:
                result |= self._topic_cache[topic]
                continue

            # Compute expanded set for this topic
            expanded = {topic}
            to_process = [topic]
            while to_process:
                current = to_process.pop()
                # Check if current topic has equivalents
                for canonical, equivalents in self.topic_equivalence.items():
                    if current == canonical or current in equivalents:
                        for eq in [canonical] + equivalents:
                            if eq not in expanded:
                                expanded.add(eq)
                                to_process.append(eq)

            self._topic_cache[topic] = expanded
            result |= expanded

        return result

    def cluster(self, articles: list[Article]) -> list[ArticleCluster]:
        """Group articles into event clusters using graph connectivity."""
        if not articles:
            return []

        self._ensure_embeddings(articles)

        sorted_articles = sorted(articles, key=lambda a: a.published_at, reverse=True)
        expanded_topics = [self._expand_topics(article.topics) for article in sorted_articles]
        adjacency: dict[int, set[int]] = defaultdict(set)
        equivalence_matches = 0

        for i, article in enumerate(sorted_articles):
            for j in range(i + 1, len(sorted_articles)):
                other = sorted_articles[j]
                if not expanded_topics[i] & expanded_topics[j]:
                    continue

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

                direct_overlap = bool(set(article.topics) & set(other.topics))
                if not direct_overlap:
                    equivalence_matches += 1
                    logger.debug(
                        "Topic equivalence enabled clustering: '%s' (%s) ↔ '%s' (%s)",
                        article.title[:50], article.topics,
                        other.title[:50], other.topics,
                    )

        clusters: list[ArticleCluster] = []
        visited: set[int] = set()
        for idx in range(len(sorted_articles)):
            if idx in visited:
                continue

            component = self._collect_component(idx, adjacency)
            visited |= component
            cluster_articles = self._prune_same_source(component, sorted_articles, adjacency)
            primary = self._primary_topic(cluster_articles)
            clusters.append(ArticleCluster(topic_category=primary, articles=cluster_articles))

        # Sort primarily by regional diversity (number of unique countries/regions)
        # then by number of sources, then by number of articles.
        def _cluster_sort_key(c: ArticleCluster):
            regions = {self.source_regions.get(s, "") for s in c.sources}
            regions.discard("") # Remove empty strings just in case
            return (len(regions), len(c.sources), len(c.articles))

        clusters.sort(key=_cluster_sort_key, reverse=True)

        multi = sum(1 for c in clusters if c.is_multi_source)
        logger.info(
            "Clustering: %d articles → %d clusters (%d multi-source, %d via topic equivalence)",
            len(articles), len(clusters), multi, equivalence_matches,
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
                    articles[idx].published_at.timestamp(),
                ),
            )
            kept_indices.append(best_idx)

        kept_indices.sort(key=lambda idx: articles[idx].published_at, reverse=True)
        return [articles[idx] for idx in kept_indices]

    def _ensure_embeddings(self, articles: list[Article]) -> None:
        needs_embedding = [a for a in articles if a.embedding is None]
        if not needs_embedding:
            return
        model = _get_model()
        texts = [f"{a.title} {a.content[:500]}" for a in needs_embedding]
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

    def _primary_topic(self, articles: list[Article]) -> str:
        counts: dict[str, int] = defaultdict(int)
        for a in articles:
            for t in a.topics:
                counts[t] += 1
        if not counts:
            return "Other"
        return max(counts, key=lambda t: counts[t])
