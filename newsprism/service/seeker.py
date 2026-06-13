"""Active Perspective Seeker — fetches missing regional perspectives via Tavily.

Triggered only where the impact evaluation says search money is worth spending
(seek_more_evidence status, or high-composite hot-topic clusters). One small
evaluator LLM call per enriched cluster picks the event-relevant regions that
are missing from the cluster and an English search keyword; non-English regions
get one keyword-localization call. Candidates must pass region, freshness, and
embedding event-match gates — the seeker prefers no injection over a bad one.

Layer: service (imports types, config, repo for telemetry)
"""
from __future__ import annotations

import json
import logging
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
        self.tavily_api_key = cfg.tavily_api_key
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
        if not self.tavily_api_key:
            logger.info("Active seeker disabled: no TAVILY_API_KEY configured")
            return clusters
        if not self.region_config:
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
        return bool(cluster.is_hot_topic and impact.composite >= self.hot_composite_trigger)

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
            article = self._search_region(cluster, region, keyword, centroid)
            if article is None:
                continue
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
        return added

    def _search_region(
        self,
        cluster: ArticleCluster,
        region: str,
        keyword: str,
        centroid: np.ndarray | None,
    ) -> Article | None:
        for query in self._build_search_queries(cluster, region, keyword):
            results = self._search_tavily(region, query)
            accepted, rejections = self._accept_results(cluster, region, results, keyword, centroid)
            if rejections:
                self._record_search_event(
                    provider="tavily_search",
                    request_type="acceptance",
                    target_region=region,
                    query=query,
                    result_count=len(results),
                    accepted_count=len(accepted),
                    rejection_reason=",".join(sorted({reason for reason, _ in rejections})),
                    rejection_count=len(rejections),
                )
            if accepted:
                return accepted[0]
        return None

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
            return False
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

    def _search_tavily(self, region: str, query: str) -> list[dict[str, Any]]:
        cache_key = (region, query)
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]

        config = self.region_config.get(region)
        payload: dict[str, Any] = {
            "api_key": self.tavily_api_key,
            "query": query,
            "search_depth": "basic",
            "include_raw_content": True,
            "max_results": max(self.max_results_per_region + 2, 4),
            "days": 3,
        }
        if config and config.trusted_domains:
            payload["include_domains"] = config.trusted_domains[:5]

        try:
            started = monotonic()
            with httpx.Client(timeout=30, follow_redirects=True) as client:
                resp = client.post("https://api.tavily.com/search", json=payload)
                duration_ms = int((monotonic() - started) * 1000)
                resp.raise_for_status()
                data = resp.json()

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
            return results
        except Exception as exc:
            response = getattr(exc, "response", None)
            self._record_search_event(
                provider="tavily_search",
                request_type="search",
                target_region=region,
                query=query,
                http_status=getattr(response, "status_code", None),
                result_count=0,
            )
            logger.debug("Tavily search failed: %s", exc)
            return []

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
        return Article(
            id=None,
            url=url,
            title=title,
            source_name=source_name,
            published_at=self._parse_published_at(result.get("published_at")),
            content=content,
            is_searched=True,
            search_region=region,
            source_kind="news",
            origin_region=origin_region,
            searched_provider=str(result.get("searched_provider") or "tavily_search"),
        )

    def _parse_published_at(self, value: Any) -> datetime:
        if isinstance(value, str) and value.strip():
            try:
                from dateutil import parser as date_parser

                parsed = date_parser.parse(value)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except (ValueError, OverflowError):
                pass
        # Unknown publish time fails the freshness gate via a sentinel in the past.
        return datetime.now(tz=timezone.utc) - timedelta(hours=self.result_max_age_hours + 1)

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
