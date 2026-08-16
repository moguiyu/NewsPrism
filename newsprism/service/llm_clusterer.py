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
from typing import Any

from newsprism.config import Config
from newsprism.service.clusterer import Clusterer
from newsprism.service.clustering_input import (
    compact_same_source_near_duplicates,
    expand_clusters_with_collapsed_articles,
)
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.service.llm_telemetry import tracked_completion
from newsprism.types import Article, ArticleCluster

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a senior news editor grouping wire stories by real-world event.\n"
    "Output ONLY valid JSON. No prose, no markdown, no explanation."
)


class _ClusterParseError(ValueError):
    """Raised when the LLM returned unparseable cluster JSON."""

    def __init__(self, message: str, raw_content: str) -> None:
        super().__init__(message)
        self.raw_content = raw_content


def _keep_one_per_source(articles: list[Article]) -> list[Article]:
    """Keep the most recent article per source (simple dedup for LLM-selected groups)."""
    seen: dict[str, Article] = {}
    for article in sorted(articles, key=lambda a: a.published_at, reverse=True):
        if article.source_name not in seen:
            seen[article.source_name] = article
    # Preserve recency order
    return sorted(seen.values(), key=lambda a: a.published_at, reverse=True)


def _strip_code_fence(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return text


def _parse_cluster_entries(raw_content: str) -> list[dict[str, Any]]:
    """Extract every complete ``{"label": ..., "ids": [...]}`` object.

    Works on truncated JSON arrays by repeatedly raw-decoding objects found at
    ``{`` positions. Incomplete trailing objects are skipped.
    """
    text = _strip_code_fence(raw_content)
    if not text:
        return []
    entries: list[dict[str, Any]] = []
    decoder = json.JSONDecoder()
    pos = 0
    while pos < len(text):
        start = text.find("{", pos)
        if start == -1:
            break
        try:
            obj, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            pos = start + 1
            continue
        if isinstance(obj, dict) and isinstance(obj.get("ids"), list):
            entries.append(obj)
        pos = start + max(1, end)
    return entries


def _build_clusters(entries: list[dict[str, Any]], articles: list[Article]) -> list[ArticleCluster]:
    result: list[ArticleCluster] = []
    n = len(articles)
    for entry in entries:
        ids = entry.get("ids", [])
        if not ids:
            continue
        valid_indices = [idx for idx in ids if isinstance(idx, int) and 0 <= idx < n]
        if not valid_indices:
            continue
        cluster_articles = _keep_one_per_source([articles[idx] for idx in valid_indices])
        if not cluster_articles:
            continue
        label = str(entry.get("label") or "").strip() or cluster_articles[0].title[:60]
        result.append(ArticleCluster(topic_category=label, articles=cluster_articles))
    return result


class LLMClusterer:
    """Groups articles by real-world event using batched LLM calls.

    Falls back to the embedding-based Clusterer if an LLM call fails or
    the combined result has fewer clusters than ``min_clusters_fallback``.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.model = cfg.litellm_model
        self.api_key = cfg.litellm_api_key
        self.base_url = cfg.litellm_base_url
        self.source_regions = {s.name: s.region for s in cfg.sources}
        self.telemetry_enabled = getattr(cfg, "llm_telemetry_enabled", False)
        self.min_clusters_fallback = cfg.clustering.get("llm_min_clusters_fallback", 3)
        self.max_articles_per_call = max(
            20, int(cfg.clustering.get("llm_max_articles_per_call", 120))
        )
        self._compat_kwargs = completion_compat_kwargs(cfg.litellm_model, cfg.litellm_base_url)
        self._fallback = Clusterer(cfg)

    def cluster(
        self,
        articles: list[Article],
        report_date: str | None = None,
    ) -> list[ArticleCluster]:
        if not articles:
            return []
        working_articles, collapsed = compact_same_source_near_duplicates(articles, self.cfg)
        clusters = self._cluster_chunked(working_articles, report_date=report_date)
        if len(clusters) < self.min_clusters_fallback:
            logger.warning(
                "LLM clustering returned %d clusters (< %d) — falling back to embedding clusterer",
                len(clusters),
                self.min_clusters_fallback,
            )
            clusters = self._fallback.cluster(working_articles)
        return expand_clusters_with_collapsed_articles(clusters, collapsed)

    def _cluster_chunked(
        self,
        articles: list[Article],
        report_date: str | None = None,
    ) -> list[ArticleCluster]:
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
            clusters.extend(self._cluster_chunk_with_recovery(chunk, report_date=report_date))

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

    def _cluster_chunk_with_recovery(
        self,
        articles: list[Article],
        report_date: str | None = None,
    ) -> list[ArticleCluster]:
        """Prefer LLM clustering, degrading only the failed input chunk.

        A malformed response previously forced the whole chunk to be retried
        as two half chunks. Complete cluster objects are now salvaged from the
        malformed output first, and only the uncovered articles are re-sent.
        """
        try:
            return self._llm_cluster(articles, report_date=report_date)
        except _ClusterParseError as exc:
            salvaged = self._salvage_failed_chunk(articles, exc.raw_content, report_date)
            if salvaged is not None:
                return salvaged
            return self._retry_as_halves(articles, report_date)
        except Exception as exc:
            logger.warning(
                "LLM clustering chunk failed (articles=%d error=%s)",
                len(articles),
                exc,
            )
            return self._retry_as_halves(articles, report_date)

    def _salvage_failed_chunk(
        self,
        articles: list[Article],
        raw_content: str,
        report_date: str | None,
    ) -> list[ArticleCluster] | None:
        """Recover complete clusters from a malformed response when possible."""
        entries = _parse_cluster_entries(raw_content)
        clusters = _build_clusters(entries, articles)
        if not clusters:
            return None

        covered = {
            idx
            for entry in entries
            for idx in entry.get("ids", [])
            if isinstance(idx, int) and 0 <= idx < len(articles)
        }
        uncovered = [article for i, article in enumerate(articles) if i not in covered]
        if not uncovered:
            logger.warning(
                "Salvaged %d complete clusters from malformed chunk; no uncovered articles",
                len(clusters),
            )
            return clusters
        if len(uncovered) > self.max_articles_per_call:
            logger.warning(
                "Salvaged %d clusters but %d articles remain uncovered; using half-split retry",
                len(clusters),
                len(uncovered),
            )
            return None

        logger.warning(
            "Salvaged %d clusters from malformed chunk; following up with %d uncovered articles",
            len(clusters),
            len(uncovered),
        )
        try:
            recovered = self._llm_cluster_followup(
                uncovered,
                prior_clusters=clusters,
                report_date=report_date,
            )
            return clusters + recovered
        except Exception as followup_exc:
            logger.error(
                "Salvaged-cluster follow-up failed (uncovered=%d error=%s); using embedding fallback",
                len(uncovered),
                followup_exc,
            )
            recovered = self._fallback.cluster(uncovered)
            return clusters + recovered

    def _retry_as_halves(
        self,
        articles: list[Article],
        report_date: str | None,
    ) -> list[ArticleCluster]:
        if len(articles) < 40:
            logger.error(
                "LLM clustering chunk failed (articles=%d); using embedding fallback",
                len(articles),
            )
            return self._fallback.cluster(articles)

        midpoint = len(articles) // 2
        logger.warning(
            "LLM clustering chunk failed (articles=%d); retrying as %d+%d",
            len(articles),
            midpoint,
            len(articles) - midpoint,
        )
        recovered: list[ArticleCluster] = []
        for subchunk in (articles[:midpoint], articles[midpoint:]):
            try:
                recovered.extend(self._llm_cluster(subchunk, report_date=report_date, attempt=2))
            except Exception as retry_exc:
                logger.error(
                    "LLM clustering sub-chunk failed (articles=%d error=%s); using embedding fallback",
                    len(subchunk),
                    retry_exc,
                )
                recovered.extend(self._fallback.cluster(subchunk))
        return recovered

    def _article_payload(self, articles: list[Article]) -> list[dict[str, Any]]:
        return [
            {
                "id": i,
                "source": a.source_name,
                "title": a.title,
                "snippet": (a.content or "")[:240],
            }
            for i, a in enumerate(articles)
        ]

    def _llm_cluster(
        self,
        articles: list[Article],
        report_date: str | None = None,
        attempt: int = 1,
    ) -> list[ArticleCluster]:
        payload = self._article_payload(articles)

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
            "- Include at most one article per source in each cluster.\n"
            "- Articles that do not fit any cluster must be omitted entirely.\n\n"
            "Return exactly this JSON structure:\n"
            '{"clusters": [{"label": "...", "ids": [0, 3, 7]}]}\n\n'
            f"Articles:\n{json.dumps(payload, ensure_ascii=False)}"
        )

        tracked = tracked_completion(
            stage="clustering",
            enabled=self.telemetry_enabled,
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
            report_date=report_date,
            item_count=len(articles),
            attempt=attempt,
            **self._compat_kwargs,
        )

        raw_content = tracked.choices[0].message.content or ""
        try:
            text = _strip_code_fence(raw_content)
            parsed = json.loads(text)
            llm_clusters = parsed.get("clusters", [])
            if not isinstance(llm_clusters, list):
                raise ValueError(f"LLM response 'clusters' is not a list: {type(llm_clusters)}")
            return _build_clusters(llm_clusters, articles)
        except Exception as exc:
            tracked.mark("malformed_json")
            if isinstance(exc, json.JSONDecodeError):
                raise _ClusterParseError(
                    f"LLM returned non-JSON content: {raw_content[:200]!r}",
                    raw_content,
                ) from exc
            raise

    def _llm_cluster_followup(
        self,
        uncovered: list[Article],
        prior_clusters: list[ArticleCluster],
        report_date: str | None,
    ) -> list[ArticleCluster]:
        """Cluster only the articles omitted by a salvaged partial response."""
        payload = self._article_payload(uncovered)
        prior_lines = []
        for index, cluster in enumerate(prior_clusters, 1):
            lead = cluster.articles[0] if cluster.articles else None
            title = (lead.title if lead else "")[:160]
            prior_lines.append(f"- recovered_{index}: label={cluster.topic_category!r} lead_title={title!r}")

        user_prompt = (
            f"Group the following {len(uncovered)} remaining news articles.\n\n"
            "Some articles from the same batch were already recovered into these clusters:\n"
            + "\n".join(prior_lines)
            + "\n\n"
            "Return clusters for the NEW articles only. If a new article belongs to one of the "
            "recovered clusters, reuse that cluster's label exactly. Otherwise create a concise "
            "English event label (≤8 words). Omit articles that fit no cluster.\n\n"
            "Return exactly this JSON structure:\n"
            '{"clusters": [{"label": "...", "ids": [0, 1, 4]}]}\n\n'
            f"Articles:\n{json.dumps(payload, ensure_ascii=False)}"
        )

        tracked = tracked_completion(
            stage="clustering_retry",
            enabled=self.telemetry_enabled,
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
            report_date=report_date,
            item_count=len(uncovered),
            attempt=2,
            **self._compat_kwargs,
        )

        raw_content = tracked.choices[0].message.content or ""
        try:
            parsed = json.loads(_strip_code_fence(raw_content))
            entries = parsed.get("clusters", [])
            if not isinstance(entries, list):
                raise ValueError(f"LLM response 'clusters' is not a list: {type(entries)}")
        except Exception as exc:
            tracked.mark("malformed_json")
            if isinstance(exc, json.JSONDecodeError):
                raise _ClusterParseError(
                    f"LLM follow-up returned non-JSON content: {raw_content[:200]!r}",
                    raw_content,
                ) from exc
            raise

        new_clusters = _build_clusters(entries, uncovered)
        prior_by_label = {
            str(cluster.topic_category or "").strip().casefold(): cluster
            for cluster in prior_clusters
        }
        merged: list[ArticleCluster] = []
        for cluster in new_clusters:
            prior = prior_by_label.get(str(cluster.topic_category or "").strip().casefold())
            if prior is not None:
                prior.articles.extend(cluster.articles)
                prior.articles = _keep_one_per_source(prior.articles)
            else:
                merged.append(cluster)
        return merged
