"""Active Perspective Seeker — fetches missing regional perspectives via Tavily.

Triggered only where the impact evaluation says search money is worth spending
(seek_more_evidence status, or any high-composite cluster — main feed or hot
topic). One small
evaluator LLM call per enriched cluster picks the event-relevant regions that
are missing from the cluster and an English search keyword; non-English regions
get one keyword-localization call. Candidates must pass region, freshness, and
embedding event-match gates — the seeker prefers no injection over a bad one.

Layer: service (imports types, config, repo for telemetry)
"""
from __future__ import annotations

import json
import logging
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any

import httpx
import litellm
import numpy as np
from rapidfuzz import fuzz

from newsprism.config import Config
from newsprism.repo import DB_PATH, insert_search_request_event
from newsprism.service.embeddings import get_model
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.types import Article, ArticleCluster, SearchRequestEvent

logger = logging.getLogger(__name__)

litellm.set_verbose = False

_REGION_NAMES: dict[str, str] = {
    "us": "United States",
    "gb": "United Kingdom",
    "cn": "China",
    "jp": "Japan",
    "kr": "South Korea",
    "ru": "Russia",
    "de": "Germany",
    "fr": "France",
    "in": "India",
    "ua": "Ukraine",
    "il": "Israel",
    "sa": "Saudi Arabia",
}

_LANGUAGE_NAMES: dict[str, str] = {
    "en": "English",
    "zh": "Chinese",
    "ja": "Japanese",
    "ko": "Korean",
    "ru": "Russian",
    "de": "German",
    "fr": "French",
    "uk": "Ukrainian",
    "he": "Hebrew",
    "ar": "Arabic",
}

_TLD_REGIONS: dict[str, str] = {
    "jp": "jp", "kr": "kr", "ru": "ru", "de": "de", "fr": "fr",
    "in": "in", "ua": "ua", "il": "il", "sa": "sa", "cn": "cn",
    "uk": "gb",
}


@dataclass
class RegionConfig:
    """Search configuration for one major region."""
    language: str = "en"
    trusted_domains: list[str] = field(default_factory=list)


class ActiveSeeker:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        # Tavily key rotation: build a de-duplicated list from TAVILY_API_KEYS (CSV)
        # plus the legacy TAVILY_API_KEY (singular) for backward compat.
        keys: list[str] = list(cfg.tavily_api_keys) if cfg.tavily_api_keys else []
        if cfg.tavily_api_key and cfg.tavily_api_key not in keys:
            keys.append(cfg.tavily_api_key)
        self.tavily_api_keys = [k for k in keys if k]
        # Backward-compat: single-key view of the list.
        self.tavily_api_key = self.tavily_api_keys[0] if self.tavily_api_keys else ""
        # Index of the key that last succeeded; rotation starts here on the next call.
        self._active_key_idx = 0
        # Track auth-exhausted keys within a single enhance_clusters() run so we
        # don't retry a known-bad key on every region.
        self._exhausted_keys: set[int] = set()

        self.evaluator_model = cfg.evaluator_model
        self.api_key = cfg.litellm_api_key
        self.base_url = cfg.litellm_base_url
        self.completion_compat_kwargs = completion_compat_kwargs(self.evaluator_model, self.base_url)

        search_cfg = cfg.active_search if isinstance(cfg.active_search, dict) else {}
        self.telemetry_enabled = bool(search_cfg.get("telemetry_enabled", True))
        self.telemetry_db_path = DB_PATH
        self.result_max_age_hours = int(search_cfg.get("result_max_age_hours", 72))
        self.min_content_chars = int(search_cfg.get("min_content_chars", 150))
        self.max_results_per_region = int(search_cfg.get("max_results_per_region", 1))
        self.max_regions_per_cluster = int(search_cfg.get("max_regions_per_cluster", 2))
        self.min_organic_sources_to_skip = int(search_cfg.get("min_organic_sources_to_skip", 8))
        self.max_existing_title_overlap = float(search_cfg.get("max_existing_title_overlap", 0.82))
        self.min_semantic_event_match = float(search_cfg.get("min_semantic_event_match", 0.45))
        self.hot_composite_trigger = float(search_cfg.get("hot_composite_trigger", 0.55))

        profiles = search_cfg.get("search_profiles", {}) if isinstance(search_cfg, dict) else {}
        self.region_config = self._build_region_config(profiles)
        self.source_regions = {source.name: source.region for source in cfg.sources}
        self._search_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

    # ─── PUBLIC API ──────────────────────────────────────────────────────────

    def enhance_clusters(self, clusters: list[ArticleCluster]) -> list[ArticleCluster]:
        if not self.tavily_api_keys:
            logger.info("Active seeker disabled: no TAVILY_API_KEYS configured")
            return clusters
        if not self.region_config:
            return clusters

        # Reset per-run state: keys that auth-failed earlier may have been reset by the provider.
        self._exhausted_keys = set()
        self._search_cache.clear()
        enriched = 0
        for cluster in clusters:
            if not self._should_enrich(cluster):
                continue
            try:
                if self._enrich_cluster(cluster):
                    enriched += 1
            except Exception as exc:
                logger.warning("Seeker enrichment failed for '%s': %s", cluster.topic_category, exc)
        if enriched:
            logger.info("Active seeker enriched %d/%d clusters", enriched, len(clusters))
        if len(self._exhausted_keys) == len(self.tavily_api_keys) and self.tavily_api_keys:
            logger.warning(
                "Active seeker: all %d Tavily keys are auth-exhausted; check TAVILY_API_KEYS",
                len(self.tavily_api_keys),
            )
        return clusters

        self._search_cache.clear()
        enriched = 0
        for cluster in clusters:
            if not self._should_enrich(cluster):
                continue
            try:
                if self._enrich_cluster(cluster):
                    enriched += 1
            except Exception as exc:
                logger.warning("Seeker enrichment failed for '%s': %s", cluster.topic_category, exc)
        if enriched:
            logger.info("Active seeker enriched %d/%d clusters", enriched, len(clusters))
        return clusters

    # ─── TRIGGER / TARGETING ─────────────────────────────────────────────────

    def _should_enrich(self, cluster: ArticleCluster) -> bool:
        organic_sources = {
            article.source_name for article in cluster.articles if not article.is_searched
        }
        if len(organic_sources) >= self.min_organic_sources_to_skip:
            return False
        impact = cluster.impact
        if impact is None:
            return False
        if impact.status == "seek_more_evidence":
            return True
        # No longer gated on is_hot_topic — high-composite main-feed clusters
        # earn the same search budget as hot-topic ones.
        return bool(impact.composite >= self.hot_composite_trigger)

    def _cluster_regions(self, cluster: ArticleCluster) -> set[str]:
        return {
            article.origin_region or self.source_regions.get(article.source_name)
            for article in cluster.articles
        } - {None}

    def _analyze_search_targets(self, cluster: ArticleCluster) -> tuple[str, list[str]]:
        """One evaluator call: an English search keyword + event-relevant regions."""
        present = self._cluster_regions(cluster)
        missing = [region for region in self.region_config if region not in present]
        if not missing:
            return "", []
        titles = "\n".join(f"- {article.title}" for article in cluster.articles[:5])
        prompt = (
            "You are targeting a news search to add missing regional perspectives to one event.\n"
            f"Event headlines:\n{titles}\n\n"
            f"Candidate regions (ISO codes): {', '.join(missing)}\n\n"
            "Return compact JSON only:\n"
            '{"keyword": "<concise English search query for this exact event, 3-8 words>", '
            '"regions": ["<up to ' + str(self.max_regions_per_cluster) + " region codes from the candidate list whose "
            'perspective genuinely matters for THIS event; [] if none>"]}'
        )
        try:
            response = litellm.completion(
                model=self.evaluator_model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=120,
                response_format={"type": "json_object"},
                **self.completion_compat_kwargs,
            )
            content = (response.choices[0].message.content or "").strip()
            parsed = json.loads(content[content.find("{"): content.rfind("}") + 1])
            keyword = str(parsed.get("keyword") or "").strip()
            regions = [
                str(region).strip().lower()
                for region in parsed.get("regions", [])
                if str(region).strip().lower() in self.region_config
                and str(region).strip().lower() in missing
            ]
            return keyword, regions[: self.max_regions_per_cluster]
        except Exception as exc:
            logger.debug("Search target analysis failed for '%s': %s", cluster.topic_category, exc)
            return "", []

    # ─── ENRICHMENT ──────────────────────────────────────────────────────────

    def _enrich_cluster(self, cluster: ArticleCluster) -> bool:
        keyword, regions = self._analyze_search_targets(cluster)
        if not keyword or not regions:
            return False

        centroid = self._cluster_centroid(cluster)
        added = False
        for region in regions:
            article, fail_reason = self._search_region(cluster, region, keyword, centroid)
            if article is not None:
                cluster.articles.append(article)
                if article.source_name not in cluster.sources:
                    cluster.sources.append(article.source_name)
                logger.info(
                    "Seeker added %s perspective to '%s': %s (%s)",
                    region,
                    cluster.topic_category,
                    article.title[:60],
                    article.source_name,
                )
                added = True
                continue
            # Synthesize a flat inline placeholder so the reader sees that this
            # region's perspective was targeted but unavailable — never silent.
            placeholder = self._placeholder_article(cluster, region, fail_reason or "unknown")
            cluster.articles.append(placeholder)
            # Do NOT add the placeholder source_name to cluster.sources — it must
            # not count toward is_multi_source or appear in perspective grouping.
            logger.info(
                "Seeker placeholder for %s on '%s': reason=%s",
                region,
                cluster.topic_category,
                fail_reason,
            )
        return added

    def _search_region(
        self,
        cluster: ArticleCluster,
        region: str,
        keyword: str,
        centroid: np.ndarray | None,
    ) -> tuple[Article | None, str | None]:
        """Return (accepted_article, failure_reason).

        failure_reason is set only when no article was accepted, so the caller
        can synthesize an inline placeholder that surfaces the cause to readers.
        """
        last_reason: str | None = None
        for query in self._build_search_queries(cluster, region, keyword):
            results, search_fail = self._search_tavily(region, query)
            if search_fail:
                # Provider-level failure (HTTP 401/403/network). Don't try more
                # queries for this region; the same key/network is used.
                return None, search_fail
            accepted, rejections = self._accept_results(cluster, region, results, keyword, centroid)
            # Always record acceptance telemetry (not only when rejections exist)
            # so we have positive signal that the gate ran.
            self._record_search_event(
                provider="tavily_search",
                request_type="acceptance",
                target_region=region,
                query=query,
                result_count=len(results),
                accepted_count=len(accepted),
                rejection_reason=",".join(sorted({reason for reason, _ in rejections})) or None,
                rejection_count=len(rejections) or None,
            )
            if accepted:
                return accepted[0], None
            if rejections:
                last_reason = ",".join(sorted({reason for reason, _ in rejections}))
            elif not results:
                last_reason = "empty_results"
            else:
                last_reason = "no_acceptable_result"
        return None, last_reason

    def _placeholder_article(
        self,
        cluster: ArticleCluster,
        region: str,
        reason: str,
    ) -> Article:
        """Synthesize an inline placeholder Article for a missing perspective.

        The placeholder is rendered flat in the source list with the country
        flag + a short failure label + tooltip detail. It never counts toward
        cluster.is_multi_source (caller does not append its source_name).
        """
        region_name = _REGION_NAMES.get(region, region.upper())
        cluster_key = getattr(cluster, "cluster_key", "") or getattr(cluster, "topic_category", "")
        article = Article(
            id=None,
            url=f"placeholder:{region}:{cluster_key}",
            title=f"待补充：{region_name}视角",
            source_name=f"[{region_name}视角待补]",
            published_at=datetime.now(tz=timezone.utc),
            content="",
            is_searched=True,
            search_region=region,
            source_kind="news",
            origin_region=region,
            searched_provider="tavily_search",
        )
        article.is_placeholder = True
        article.search_acceptance_status = "failed"
        article.search_acceptance_reason = reason
        return article

    def _accept_results(
        self,
        cluster: ArticleCluster,
        region: str,
        results: list[dict[str, Any]],
        keyword: str,
        centroid: np.ndarray | None,
    ) -> tuple[list[Article], list[tuple[str, str]]]:
        accepted: list[Article] = []
        rejections: list[tuple[str, str]] = []
        existing_urls = {article.url for article in cluster.articles}
        existing_titles = [article.title for article in cluster.articles]
        for result in results:
            article = self._result_to_article(result, region)
            if article is None:
                rejections.append(("thin_result", str(result.get("url"))))
                continue
            if article.url in existing_urls:
                rejections.append(("already_present", article.url))
                continue
            reason = self._rejection_reason(article, region, existing_titles, centroid)
            if reason:
                rejections.append((reason, article.url))
                continue
            accepted.append(article)
            if len(accepted) >= self.max_results_per_region:
                break
        return accepted, rejections

    def _rejection_reason(
        self,
        article: Article,
        region: str,
        existing_titles: list[str],
        centroid: np.ndarray | None,
    ) -> str:
        if not self._is_region_valid(article, region):
            return "region_mismatch"
        if not self._is_fresh(article.published_at):
            return "stale_result"
        if any(
            fuzz.token_set_ratio(article.title, title) / 100.0 >= self.max_existing_title_overlap
            for title in existing_titles
        ):
            return "duplicate_of_existing"
        if centroid is not None:
            embedding = get_model().encode(
                [f"{article.title} {article.content[:400]}"],
                normalize_embeddings=True,
                show_progress_bar=False,
            )[0]
            if float(np.dot(embedding, centroid)) < self.min_semantic_event_match:
                return "event_mismatch"
        return ""

    def _is_region_valid(self, article: Article, region: str) -> bool:
        if article.origin_region == region:
            return True
        domain = urllib.parse.urlparse(article.url).netloc.lower()
        tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
        if _TLD_REGIONS.get(tld) == region:
            return True
        config = self.region_config.get(region)
        if config and any(domain.endswith(trusted) for trusted in config.trusted_domains):
            return True
        return False

    def _is_fresh(self, published_at: datetime | None) -> bool:
        if published_at is None:
            # No publish date extractable from either the Tavily field or the
            # URL path. The search was already date-bounded by the query's
            # ``days: 3`` parameter, so trust Tavily's freshness rather than
            # rejecting 100% of results (the 2026-07-22 incident: 237 fresh
            # results all rejected as stale because published_date=None).
            return True
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=self.result_max_age_hours)
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        return published_at >= cutoff

    def _cluster_centroid(self, cluster: ArticleCluster) -> np.ndarray | None:
        embeddings = [
            np.array(article.embedding, dtype=float)
            for article in cluster.articles
            if article.embedding is not None
        ]
        if not embeddings:
            return None
        centroid = np.mean(embeddings, axis=0)
        norm = np.linalg.norm(centroid)
        if norm == 0:
            return None
        return centroid / norm

    # ─── QUERIES ─────────────────────────────────────────────────────────────

    def _build_search_queries(self, cluster: ArticleCluster, region: str, keyword: str) -> list[str]:
        config = self.region_config.get(region)
        english_query = f"{keyword} news {_REGION_NAMES.get(region, region)}"
        if not config or config.language == "en":
            return [english_query]
        localized = self._localize_search_keyword(cluster, region, keyword)
        if localized and localized != keyword:
            return [localized, english_query]
        return [english_query]

    def _localize_search_keyword(self, cluster: ArticleCluster, region: str, keyword: str) -> str:
        config = self.region_config.get(region)
        if not config or config.language == "en":
            return keyword
        language_name = _LANGUAGE_NAMES.get(config.language, config.language)
        region_name = _REGION_NAMES.get(region, region)
        context = "\n".join(f"- {article.title}" for article in cluster.articles[:5])
        prompt = (
            f"Convert this English news search query into concise natural {language_name} used by local media in "
            f"{region_name}. Use native script when normal for that language.\n\n"
            f"Event headlines:\n{context}\n\n"
            f"English query: {keyword}\n\n"
            "Return ONLY the localized search query, 3-8 words, with no explanation or quotes."
        )
        try:
            response = litellm.completion(
                model=self.evaluator_model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=40,
                **self.completion_compat_kwargs,
            )
            content = (response.choices[0].message.content or "").strip()
            localized = content.splitlines()[0].strip().strip("\"'")
            return localized or keyword
        except Exception as exc:
            logger.debug("Failed to localize search keyword for %s: %s", region, exc)
            return keyword

    # ─── TAVILY ──────────────────────────────────────────────────────────────

    def _search_tavily(self, region: str, query: str) -> tuple[list[dict[str, Any]], str | None]:
        """Return (results, failure_reason).

        failure_reason is one of: http_401, http_403, http_<other>, network, None.
        On 401/403, the active key is marked exhausted and the next configured
        key is tried within the same call. Only when ALL keys are exhausted (or
        a non-auth error occurs) is failure_reason returned.
        """
        cache_key = (region, query)
        if cache_key in self._search_cache:
            return self._search_cache[cache_key], None

        config = self.region_config.get(region)
        base_payload: dict[str, Any] = {
            "query": query,
            "search_depth": "basic",
            "include_raw_content": True,
            "max_results": max(self.max_results_per_region + 2, 4),
            "days": 3,
        }
        if config and config.trusted_domains:
            base_payload["include_domains"] = config.trusted_domains[:5]

        # Build the key try-order: start from the last known-good key, then any
        # remaining keys that aren't yet exhausted this run.
        try_order: list[int] = []
        if self._active_key_idx not in self._exhausted_keys:
            try_order.append(self._active_key_idx)
        try_order.extend(
            idx
            for idx in range(len(self.tavily_api_keys))
            if idx not in try_order and idx not in self._exhausted_keys
        )
        if not try_order:
            # All keys exhausted earlier this run — short-circuit so we don't
            # spam Tavily with known-bad credentials.
            self._record_search_event(
                provider="tavily_search",
                request_type="search",
                target_region=region,
                query=query,
                http_status=401,
                result_count=0,
            )
            return [], "http_401"

        last_failure_reason: str | None = None
        last_status: int | None = None
        for key_idx in try_order:
            payload = {**base_payload, "api_key": self.tavily_api_keys[key_idx]}
            try:
                started = monotonic()
                with httpx.Client(timeout=30, follow_redirects=True) as client:
                    resp = client.post("https://api.tavily.com/search", json=payload)
                    duration_ms = int((monotonic() - started) * 1000)
                    if resp.status_code in (401, 403):
                        # Auth/quota issue with this key — failover to the next.
                        self._exhausted_keys.add(key_idx)
                        last_failure_reason = f"http_{resp.status_code}"
                        last_status = resp.status_code
                        self._record_search_event(
                            provider="tavily_search",
                            request_type="search",
                            target_region=region,
                            query=query,
                            http_status=resp.status_code,
                            result_count=0,
                            duration_ms=duration_ms,
                        )
                        logger.info(
                            "Tavily key #%d returned HTTP %d; rotating to next key",
                            key_idx + 1,
                            resp.status_code,
                        )
                        continue
                    resp.raise_for_status()
                    data = resp.json()
            except httpx.HTTPError as exc:
                response = getattr(exc, "response", None)
                status = getattr(response, "status_code", None)
                if status in (401, 403):
                    self._exhausted_keys.add(key_idx)
                    last_failure_reason = f"http_{status}"
                    last_status = status
                    self._record_search_event(
                        provider="tavily_search",
                        request_type="search",
                        target_region=region,
                        query=query,
                        http_status=status,
                        result_count=0,
                    )
                    continue
                # Non-auth HTTP error or network error — not a key problem, so
                # don't rotate; just report the failure for this query.
                self._record_search_event(
                    provider="tavily_search",
                    request_type="search",
                    target_region=region,
                    query=query,
                    http_status=status,
                    result_count=0,
                )
                logger.warning("Tavily search failed: %s", exc)
                return [], f"http_{status}" if status else "network"
            else:
                # Success — pin this key as the active one for subsequent calls.
                self._active_key_idx = key_idx
                results: list[dict[str, Any]] = []
                for result in data.get("results", []):
                    url = result.get("url", "")
                    source_domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                    results.append(
                        {
                            "url": url,
                            "title": result.get("title"),
                            "content": result.get("raw_content") or result.get("content", ""),
                            "published_at": result.get("published_date") or result.get("published_at"),
                            "source_name": source_domain,
                            "searched_provider": "tavily_search",
                        }
                    )
                self._record_search_event(
                    provider="tavily_search",
                    request_type="search",
                    target_region=region,
                    query=query,
                    http_status=resp.status_code,
                    result_count=len(results),
                    duration_ms=duration_ms,
                )
                self._search_cache[cache_key] = results
                return results, None

        # All candidate keys returned auth failure.
        self._record_search_event(
            provider="tavily_search",
            request_type="search",
            target_region=region,
            query=query,
            http_status=last_status,
            result_count=0,
        )
        return [], last_failure_reason or "http_401"

    def _result_to_article(self, result: dict[str, Any], region: str) -> Article | None:
        url = result.get("url")
        title = (result.get("title") or "").strip()
        content = (result.get("content") or "").strip()
        if not url or not title or len(content) < self.min_content_chars:
            return None
        domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
        source_name = result.get("source_name") or domain
        configured_region = self.source_regions.get(source_name)
        tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
        origin_region = configured_region or _TLD_REGIONS.get(tld) or region
        # Tavily frequently returns published_date=None even for fresh results
        # (the URL path like /2026/07/20/ is clearly recent). Try the explicit
        # field first, then fall back to a URL-path date parse so the freshness
        # gate has something concrete to evaluate.
        published_at = self._parse_published_at(result.get("published_at"))
        if published_at is None:
            published_at = self._parse_url_date(url)
        return Article(
            id=None,
            url=url,
            title=title,
            source_name=source_name,
            published_at=published_at,
            content=content,
            is_searched=True,
            search_region=region,
            source_kind="news",
            origin_region=origin_region,
            searched_provider=str(result.get("searched_provider") or "tavily_search"),
        )

    def _parse_published_at(self, value: Any) -> datetime | None:
        if isinstance(value, str) and value.strip():
            try:
                from dateutil import parser as date_parser

                parsed = date_parser.parse(value)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except (ValueError, OverflowError):
                pass
        return None

    # Match common URL date patterns: /2026/07/20/, /2026-07-20/, /20260720/.
    # Returns None when no date-like segment is found.
    _URL_DATE_PATTERN = re.compile(r"/(?P<date>(?:19|20)\d{2})[-/]?(?P<month>[01]\d)[-/]?(?P<day>[0-3]\d)(?:[/-]|\b)")

    def _parse_url_date(self, url: str | None) -> datetime | None:
        """Best-effort extraction of a publish date from a URL path.

        Tavily returns published_date=None for many outlets (NYT, CNN, Time,
        northeastern.edu, …). Their URL paths almost always carry the date
        (/2026/07/20/article-slug). Without this fallback the freshness gate
        rejected 100% of results — see the 2026-07-22 incident where
        accepted_count=0 despite 237 fresh results.
        """
        if not url:
            return None
        match = self._URL_DATE_PATTERN.search(url)
        if not match:
            return None
        try:
            return datetime(
                int(match.group("date")),
                int(match.group("month")),
                int(match.group("day")),
                tzinfo=timezone.utc,
            )
        except ValueError:
            return None

    # ─── REGION CONFIG / TELEMETRY ───────────────────────────────────────────

    def _build_region_config(self, profiles: dict[str, Any]) -> dict[str, RegionConfig]:
        region_config: dict[str, RegionConfig] = {}
        for region, profile in (profiles or {}).items():
            if region not in _REGION_NAMES:
                continue
            language = str((profile or {}).get("language", "en"))
            region_config[region] = RegionConfig(language=language)
        for source in self.cfg.sources:
            config = region_config.get(source.region)
            if config is None:
                continue
            domain = urllib.parse.urlparse(source.url).netloc.lower().removeprefix("www.")
            if domain and domain not in config.trusted_domains:
                config.trusted_domains.append(domain)
        return region_config

    def _record_search_event(
        self,
        provider: str,
        request_type: str,
        target_region: str | None = None,
        query: str | None = None,
        http_status: int | None = None,
        result_count: int | None = None,
        accepted_count: int | None = None,
        rejection_reason: str | None = None,
        rejection_count: int | None = None,
        duration_ms: int | None = None,
    ) -> None:
        if not self.telemetry_enabled:
            return
        try:
            insert_search_request_event(
                SearchRequestEvent(
                    provider=provider,
                    request_type=request_type,
                    target_region=target_region,
                    query=query,
                    http_status=http_status,
                    result_count=result_count,
                    accepted_count=accepted_count,
                    rejection_reason=rejection_reason,
                    rejection_count=rejection_count,
                    duration_ms=duration_ms,
                ),
                db_path=self.telemetry_db_path,
            )
        except Exception as exc:
            logger.debug("Search telemetry write failed: %s", exc)
