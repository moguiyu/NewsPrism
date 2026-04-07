"""Deduplication — removes near-duplicate articles before clustering.

Two passes:
1. Fuzzy title match (rapidfuzz) — catches rephrased headlines from same story
2. Semantic similarity (sentence-transformers) — catches paraphrased content

We keep one article per near-duplicate group (highest source weight wins).

Layer: service (imports types, config; never imports repo or runtime)
"""
from __future__ import annotations

import logging

import numpy as np
from rapidfuzz import fuzz
from sentence_transformers import SentenceTransformer

from newsprism.config import Config
from newsprism.types import Article

logger = logging.getLogger(__name__)

_MODEL: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        # paraphrase-multilingual-mpnet works well for mixed CJK+EN text
        _MODEL = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")
    return _MODEL


class Deduplicator:
    def __init__(self, cfg: Config) -> None:
        self.fuzzy_threshold = cfg.dedup.get("fuzzy_threshold", 85)
        self.semantic_threshold = cfg.dedup.get("semantic_threshold", 0.82)
        self._weights = {s.name: s.weight for s in cfg.sources}

    def deduplicate(self, articles: list[Article]) -> list[Article]:
        if not articles:
            return []

        after_fuzzy = self._fuzzy_dedup(articles)
        logger.info("Dedup fuzzy: %d → %d", len(articles), len(after_fuzzy))

        after_sem = self._semantic_dedup(after_fuzzy)
        logger.info("Dedup semantic: %d → %d", len(after_fuzzy), len(after_sem))

        return after_sem

    def _fuzzy_dedup(self, articles: list[Article]) -> list[Article]:
        kept: list[Article] = []
        for article in articles:
            is_dup = False
            for existing in kept:
                # ONLY fuzzy deduplicate if it's from the same source
                if article.source_name != existing.source_name:
                    continue
                score = fuzz.ratio(article.title, existing.title)
                if score >= self.fuzzy_threshold:
                    if self._weight(article) > self._weight(existing):
                        kept.remove(existing)
                        kept.append(article)
                    is_dup = True
                    break
            if not is_dup:
                kept.append(article)
        return kept

    def _semantic_dedup(self, articles: list[Article]) -> list[Article]:
        if len(articles) < 2:
            return articles

        model = _get_model()
        texts = [f"{a.title} {a.content[:500]}" for a in articles]
        embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)

        # Store embeddings on articles for later reuse in clustering
        for article, emb in zip(articles, embeddings):
            article.embedding = emb.tolist()

        kept_indices: list[int] = []
        dropped = set()

        for i in range(len(articles)):
            if i in dropped:
                continue
            kept_indices.append(i)
            for j in range(i + 1, len(articles)):
                if j in dropped:
                    continue
                # ONLY semantic deduplicate if it's from the exact same source
                if articles[i].source_name != articles[j].source_name:
                    # Unless it's a near-exact syndicated copy (> 0.98)
                    sim = float(np.dot(embeddings[i], embeddings[j]))
                    if sim >= 0.98:
                        if self._weight(articles[j]) > self._weight(articles[i]):
                            kept_indices[-1] = j
                        dropped.add(j)
                    continue

                sim = float(np.dot(embeddings[i], embeddings[j]))
                if sim >= self.semantic_threshold:
                    if self._weight(articles[j]) > self._weight(articles[i]):
                        kept_indices[-1] = j
                    dropped.add(j)

        return [articles[i] for i in kept_indices]

    def _weight(self, article: Article) -> float:
        return self._weights.get(article.source_name, 0.5)
