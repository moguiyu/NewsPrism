"""LLM-driven clusterer using batched API calls to group articles by event.

The LLM groups articles by real-world event identity. Large pools are split
into time-ordered chunks (clustering.llm_max_articles_per_call each); a
same-event split across a chunk boundary is tolerated — downstream display
dedup merges it. Falls back to the embedding Clusterer if any LLM call fails
or returns too few clusters.

Layer: service (imports types, config, service/llm_compat; never imports runtime)
"""
from __future__ import annotations

import json
import logging
import math

import litellm

from newsprism.config import Config
from newsprism.service.clusterer import Clusterer
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.types import Article, ArticleCluster

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a senior news editor grouping wire stories by real-world event.\n"
    "Output ONLY valid JSON. No prose, no markdown, no explanation."
)


def _keep_one_per_source(articles: list[Article]) -> list[Article]:
    """Keep the most recent article per source (simple dedup for LLM-selected groups)."""
    seen: dict[str, Article] = {}
    for article in sorted(articles, key=lambda a: a.published_at, reverse=True):
        if article.source_name not in seen:
            seen[article.source_name] = article
    # Preserve recency order
    return sorted(seen.values(), key=lambda a: a.published_at, reverse=True)


class LLMClusterer:
    """Groups articles by real-world event using batched LLM calls.

    Falls back to the embedding-based Clusterer if an LLM call fails or
    the combined result has fewer clusters than ``min_clusters_fallback``.
    """

    def __init__(self, cfg: Config) -> None:
        self.model = cfg.litellm_model
        self.api_key = cfg.litellm_api_key
        self.base_url = cfg.litellm_base_url
        self.source_regions = {s.name: s.region for s in cfg.sources}
        self.min_clusters_fallback = cfg.clustering.get("llm_min_clusters_fallback", 3)
        self.max_articles_per_call = max(
            20, int(cfg.clustering.get("llm_max_articles_per_call", 300))
        )
        self._compat_kwargs = completion_compat_kwargs(cfg.litellm_model, cfg.litellm_base_url)
        self._fallback = Clusterer(cfg)

    def cluster(self, articles: list[Article]) -> list[ArticleCluster]:
        if not articles:
            return []
        try:
            clusters = self._cluster_chunked(articles)
            if len(clusters) < self.min_clusters_fallback:
                logger.warning(
                    "LLM clustering returned %d clusters (< %d) — falling back to embedding clusterer",
                    len(clusters),
                    self.min_clusters_fallback,
                )
                return self._fallback.cluster(articles)
            return clusters
        except Exception as exc:
            logger.error(
                "LLM clustering failed (%s) — falling back to embedding clusterer", exc
            )
            return self._fallback.cluster(articles)

    def _cluster_chunked(self, articles: list[Article]) -> list[ArticleCluster]:
        if len(articles) <= self.max_articles_per_call:
            chunks = [articles]
        else:
            # Time-ordered chunks of roughly equal size; same-event splits across
            # a boundary are merged later by display dedup.
            ordered = sorted(articles, key=lambda a: a.published_at, reverse=True)
            chunk_count = math.ceil(len(ordered) / self.max_articles_per_call)
            size = math.ceil(len(ordered) / chunk_count)
            chunks = [ordered[i:i + size] for i in range(0, len(ordered), size)]
            logger.info(
                "LLM clustering volume guard: %d articles split into %d chunks of <=%d",
                len(articles),
                len(chunks),
                size,
            )

        clusters: list[ArticleCluster] = []
        for chunk in chunks:
            clusters.extend(self._llm_cluster(chunk))

        # Sort: most diverse (regions, sources, articles) first
        clusters.sort(
            key=lambda c: (
                len({self.source_regions.get(a.source_name, "intl") for a in c.articles}),
                len(c.sources),
                len(c.articles),
            ),
            reverse=True,
        )
        logger.info(
            "LLM clusterer: %d clusters from %d articles (%d chunks)",
            len(clusters),
            len(articles),
            len(chunks),
        )
        return clusters

    def _llm_cluster(self, articles: list[Article]) -> list[ArticleCluster]:
        payload = [
            {
                "id": i,
                "source": a.source_name,
                "title": a.title,
                "snippet": (a.content or "")[:240],
            }
            for i, a in enumerate(articles)
        ]

        user_prompt = (
            f"Group the following {len(articles)} news articles into clusters.\n\n"
            "Rules:\n"
            "- Group ONLY articles that cover the exact same real-world event or development.\n"
            "- Articles in DIFFERENT LANGUAGES covering the same event MUST be grouped together.\n"
            "- Tightly coupled developments of one event within the window (a strike and the "
            "same day's response to it) belong in one cluster.\n"
            "- Do NOT group merely topically similar articles "
            "(e.g. two different earthquakes, two unrelated political speeches).\n"
            "- Each cluster should have a concise English event label (≤8 words).\n"
            '- Articles that do not fit any cluster go in "unclustered".\n\n'
            "Return exactly this JSON structure:\n"
            '{{"clusters": [{{"label": "...", "ids": [0, 3, 7]}}], "unclustered": [1, 2, 4]}}\n\n'
            f"Articles:\n{json.dumps(payload, ensure_ascii=False)}"
        )

        response = litellm.completion(
            model=self.model,
            api_key=self.api_key,
            api_base=self.base_url,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=8000,
            temperature=0.1,
            response_format={"type": "json_object"},
            **self._compat_kwargs,
        )

        raw_content = response.choices[0].message.content or ""
        try:
            text = (raw_content or "").strip()
            if text.startswith("```"):
                # Strip opening fence line and closing fence
                text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"LLM returned non-JSON content: {raw_content[:200]!r}") from exc

        llm_clusters = parsed.get("clusters", [])
        if not isinstance(llm_clusters, list):
            raise ValueError(f"LLM response 'clusters' is not a list: {type(llm_clusters)}")

        result: list[ArticleCluster] = []
        n = len(articles)

        for entry in llm_clusters:
            ids = entry.get("ids", [])
            if not ids:
                continue
            # Filter out-of-range indices defensively
            valid_indices = [idx for idx in ids if isinstance(idx, int) and 0 <= idx < n]
            if not valid_indices:
                continue

            cluster_articles = [articles[idx] for idx in valid_indices]
            cluster_articles = _keep_one_per_source(cluster_articles)
            if not cluster_articles:
                continue

            label = str(entry.get("label") or "").strip() or cluster_articles[0].title[:60]
            result.append(
                ArticleCluster(
                    topic_category=label,
                    articles=cluster_articles,
                )
            )

        return result
