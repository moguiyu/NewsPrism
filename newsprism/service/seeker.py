"""Active Perspective Seeker — dynamically fetches missing country perspectives.

Uses a strict two-stage process:
1. Identify which central regions are missing from the cluster.
2. Search only for region-valid, timely, event-matched candidates.

Search fallback chain per region:
  1. BrightData SERP API (geo-targeted, native language) — if BRIGHTDATA_API_KEY set
  2. Tavily (open web, native then English query)
  3. Official web sources (curated ministry / regulator / state-media sources)
  4. Official social timelines (curated X / YouTube accounts only, region-gated)

If no valid regional news candidate is found after all fallbacks, the seeker
prefers no injection over an irrelevant or repeated perspective.

Layer: service
"""
from __future__ import annotations

import json
import logging
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable

import feedparser
import httpx
import litellm
import numpy as np
import trafilatura
from bs4 import BeautifulSoup

from newsprism.config import Config
from newsprism.repo import DB_PATH, insert_search_request_event
from newsprism.service.clusterer import _get_model
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.types import Article, ArticleCluster, SearchRequestEvent

logger = logging.getLogger(__name__)

litellm.set_verbose = False


@dataclass
class RegionConfig:
    """Search configuration for a specific region."""
    language: str
    native_query_suffix: str
    geo_location: str
    trusted_domains: list[str] = field(default_factory=list)


class SocialFallbackProvider:
    platform = "unknown"

    def fetch_recent(self, source: dict[str, Any], keyword: str, region: str) -> list[dict[str, Any]]:
        raise NotImplementedError


class XOfficialProvider(SocialFallbackProvider):
    platform = "x"

    def __init__(
        self,
        bearer_token: str,
        timeout: int = 20,
        event_recorder: Callable[..., None] | None = None,
        cost_lookup: Callable[[str, str, int | None, int | None], float | None] | None = None,
    ) -> None:
        self.bearer_token = bearer_token
        self.timeout = timeout
        self.event_recorder = event_recorder
        self.cost_lookup = cost_lookup

    def _record_event(
        self,
        request_type: str,
        region: str | None,
        query: str | None,
        account_id: str | None,
        http_status: int | None,
        result_count: int | None,
        duration_ms: int | None,
    ) -> None:
        if self.event_recorder is None:
            return
        estimated_cost = None
        if self.cost_lookup is not None:
            estimated_cost = self.cost_lookup("x", request_type, result_count, http_status)
        self.event_recorder(
            provider="x",
            request_type=request_type,
            target_region=region,
            query=query,
            account_id=account_id,
            http_status=http_status,
            result_count=result_count,
            accepted_count=None,
            duration_ms=duration_ms,
            estimated_cost_usd=estimated_cost,
        )

    def fetch_recent(self, source: dict[str, Any], keyword: str, region: str) -> list[dict[str, Any]]:
        if not self.bearer_token:
            return []

        username = source.get("username")
        user_id = source.get("user_id")
        if not username and not user_id:
            return []

        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            if not user_id and username:
                started = monotonic()
                try:
                    resp = client.get(f"https://api.x.com/2/users/by/username/{username}", headers=headers)
                    duration_ms = int((monotonic() - started) * 1000)
                    resp.raise_for_status()
                    data = resp.json().get("data") or {}
                    user_id = data.get("id")
                    username = data.get("username") or username
                    self._record_event(
                        request_type="user_lookup",
                        region=region,
                        query=None,
                        account_id=username,
                        http_status=resp.status_code,
                        result_count=1 if data else 0,
                        duration_ms=duration_ms,
                    )
                except Exception as exc:
                    response = getattr(exc, "response", None)
                    self._record_event(
                        request_type="user_lookup",
                        region=region,
                        query=None,
                        account_id=username,
                        http_status=getattr(response, "status_code", None),
                        result_count=0,
                        duration_ms=None,
                    )
                    raise

            if not user_id:
                return []

            started = monotonic()
            try:
                resp = client.get(
                    f"https://api.x.com/2/users/{user_id}/tweets",
                    headers=headers,
                    params={
                        "max_results": min(int(source.get("max_results", 10)), 20),
                        "tweet.fields": "created_at,text",
                        "exclude": "retweets,replies",
                    },
                )
                duration_ms = int((monotonic() - started) * 1000)
                resp.raise_for_status()
                tweets = resp.json().get("data", [])
                self._record_event(
                    request_type="user_timeline",
                    region=region,
                    query=keyword,
                    account_id=user_id,
                    http_status=resp.status_code,
                    result_count=len(tweets),
                    duration_ms=duration_ms,
                )
            except Exception as exc:
                response = getattr(exc, "response", None)
                self._record_event(
                    request_type="user_timeline",
                    region=region,
                    query=keyword,
                    account_id=user_id,
                    http_status=getattr(response, "status_code", None),
                    result_count=0,
                    duration_ms=None,
                )
                raise

        results: list[dict[str, Any]] = []
        for tweet in tweets:
            tweet_text = (tweet.get("text") or "").strip()
            tweet_id = tweet.get("id")
            if not tweet_text or not tweet_id:
                continue
            account_name = source.get("source_name") or username or user_id
            results.append(
                {
                    "url": f"https://x.com/{username or user_id}/status/{tweet_id}",
                    "title": tweet_text[:160],
                    "content": tweet_text,
                    "published_at": tweet.get("created_at"),
                    "source_name": account_name,
                    "origin_region": region,
                    "source_kind": "official_social",
                    "platform": "x",
                    "account_id": user_id,
                    "is_official_source": True,
                    "searched_provider": "x_user_timeline",
                }
            )
        return results

    def search_keyword(
        self, region: str, keyword: str, language: str = "en", max_results: int = 5
    ) -> list[dict[str, Any]]:
        """Search recent tweets by keyword. Falls back gracefully on rate limits or auth errors."""
        if not self.bearer_token:
            return []

        # Search with both native language and English if non-English region
        if language != "en":
            x_query = f"({keyword}) (lang:{language} OR lang:en) -is:retweet"
        else:
            x_query = f"{keyword} -is:retweet"

        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                started = monotonic()
                resp = client.get(
                    "https://api.x.com/2/tweets/search/recent",
                    headers=headers,
                    params={
                        "query": x_query,
                        "max_results": min(max(max_results, 10), 100),
                        "tweet.fields": "created_at,text,author_id",
                        "expansions": "author_id",
                        "user.fields": "name,username",
                    },
                )
            duration_ms = int((monotonic() - started) * 1000)
            if resp.status_code == 429:
                self._record_event("recent_search", region, x_query, None, resp.status_code, 0, duration_ms)
                logger.debug("X API rate limit hit for region %s keyword search", region)
                return []
            if resp.status_code == 403:
                self._record_event("recent_search", region, x_query, None, resp.status_code, 0, duration_ms)
                logger.debug("X API access denied for keyword search (subscription tier may not support search)")
                return []
            resp.raise_for_status()
            data = resp.json()
            tweets = data.get("data") or []
            self._record_event("recent_search", region, x_query, None, resp.status_code, len(tweets), duration_ms)
        except Exception as exc:
            response = getattr(exc, "response", None)
            self._record_event(
                "recent_search",
                region,
                x_query,
                None,
                getattr(response, "status_code", None),
                0,
                None,
            )
            logger.debug("X keyword search failed for %s: %s", region, exc)
            return []

        users_by_id: dict[str, dict[str, Any]] = {
            u["id"]: u for u in (data.get("includes") or {}).get("users") or []
        }

        results: list[dict[str, Any]] = []
        for tweet in tweets:
            tweet_text = (tweet.get("text") or "").strip()
            tweet_id = tweet.get("id")
            author_id = tweet.get("author_id")
            if not tweet_text or not tweet_id:
                continue
            user = users_by_id.get(author_id or "", {})
            username = user.get("username") or author_id or "unknown"
            source_name = user.get("name") or username
            results.append(
                {
                    "url": f"https://x.com/{username}/status/{tweet_id}",
                    "title": tweet_text[:160],
                    "content": tweet_text,
                    "published_at": tweet.get("created_at"),
                    "source_name": source_name,
                    "origin_region": region,
                    "source_kind": "official_social",
                    "platform": "x",
                    "account_id": author_id,
                    "is_official_source": False,
                    "searched_provider": "x_recent_search",
                }
            )
        return results


class YouTubeOfficialProvider(SocialFallbackProvider):
    platform = "youtube"

    def __init__(self, api_key: str, timeout: int = 20) -> None:
        self.api_key = api_key
        self.timeout = timeout

    def fetch_recent(self, source: dict[str, Any], keyword: str, region: str) -> list[dict[str, Any]]:
        if not self.api_key:
            return []

        channel_id = source.get("channel_id")
        if not channel_id:
            return []

        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            resp = client.get(
                "https://www.googleapis.com/youtube/v3/channels",
                params={
                    "part": "contentDetails,snippet",
                    "id": channel_id,
                    "key": self.api_key,
                },
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if not items:
                return []

            channel = items[0]
            uploads_playlist = (
                channel.get("contentDetails", {})
                .get("relatedPlaylists", {})
                .get("uploads")
            )
            if not uploads_playlist:
                return []

            videos = client.get(
                "https://www.googleapis.com/youtube/v3/playlistItems",
                params={
                    "part": "snippet",
                    "playlistId": uploads_playlist,
                    "maxResults": min(int(source.get("max_results", 10)), 20),
                    "key": self.api_key,
                },
            )
            videos.raise_for_status()
            items = videos.json().get("items", [])

        results: list[dict[str, Any]] = []
        default_name = source.get("source_name") or channel.get("snippet", {}).get("title") or channel_id
        for item in items:
            snippet = item.get("snippet") or {}
            resource = snippet.get("resourceId") or {}
            video_id = resource.get("videoId")
            title = (snippet.get("title") or "").strip()
            if not video_id or not title:
                continue
            results.append(
                {
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "title": title,
                    "content": (snippet.get("description") or "").strip(),
                    "published_at": snippet.get("publishedAt"),
                    "source_name": default_name,
                    "origin_region": region,
                    "source_kind": "official_social",
                    "platform": "youtube",
                    "account_id": channel_id,
                    "is_official_source": True,
                    "searched_provider": "youtube_channel_timeline",
                }
            )
        return results


class ActiveSeeker:
    _NATIVE_QUERY_SUFFIXES: dict[str, str] = {
        "ja": "ニュース",
        "zh": "新闻",
        "ko": "뉴스",
        "ru": "новости",
        "fa": "خبر",
        "it": "notizie",
        "es": "noticias",
        "he": "חדשות",
        "uk": "новини",
        "tr": "haber",
        "de": "Nachrichten",
        "nl": "nieuws",
        "pl": "wiadomości",
        "ar": "أخبار",
        "hi": "समाचार",
        "pt": "notícias",
        "th": "ข่าว",
        "id": "berita",
        "vi": "tin tức",
        "ur": "خبریں",
        "en": "news",
    }

    _DEFAULT_REGION_SEARCH_LANGUAGES: dict[str, str] = {
        "ae": "ar",
        "ar": "es",  # Argentina
        "au": "en",
        "az": "az",
        "bd": "bn",
        "br": "pt",
        "by": "ru",
        "ca": "en",
        "ch": "de",
        "cl": "es",
        "cn": "zh",
        "co": "es",
        "de": "de",
        "eg": "ar",
        "es": "es",
        "fr": "fr",
        "gb": "en",
        "he": "he",
        "id": "id",
        "il": "he",
        "in": "en",
        "iq": "ar",
        "ir": "fa",
        "it": "it",
        "jp": "ja",
        "jo": "ar",
        "ke": "en",
        "kr": "ko",
        "kw": "ar",
        "lb": "ar",
        "ly": "ar",
        "ma": "ar",
        "mx": "es",
        "my": "ms",
        "ng": "en",
        "nl": "nl",
        "om": "ar",
        "pe": "es",
        "ph": "en",
        "pk": "ur",
        "pl": "pl",
        "qa": "ar",
        "ru": "ru",
        "sa": "ar",
        "sg": "zh",
        "sy": "ar",
        "th": "th",
        "tr": "tr",
        "tw": "zh",
        "ua": "uk",
        "uk": "uk",
        "us": "en",
        "ve": "es",
        "vn": "vi",
        "ye": "ar",
        "za": "en",
    }

    # ccTLD → ISO alpha-2 region code.
    # Most ccTLDs are identical to ISO codes; exceptions are listed explicitly.
    _CCTLD_REGION_MAP: dict[str, str] = {
        "af": "af", "al": "al", "dz": "dz", "ao": "ao", "ar": "ar",
        "am": "am", "au": "au", "at": "at", "az": "az", "bh": "bh",
        "bd": "bd", "by": "by", "be": "be", "bz": "bz", "bo": "bo",
        "ba": "ba", "br": "br", "bg": "bg", "kh": "kh", "ca": "ca",
        "cl": "cl", "cn": "cn", "co": "co", "cr": "cr", "hr": "hr",
        "cu": "cu", "cz": "cz", "dk": "dk", "eg": "eg", "ee": "ee",
        "et": "et", "fi": "fi", "fr": "fr", "ge": "ge", "de": "de",
        "gh": "gh", "gr": "gr", "gt": "gt", "hn": "hn", "hk": "hk",
        "hu": "hu", "in": "in", "id": "id", "ir": "ir", "iq": "iq",
        "ie": "ie", "il": "il", "it": "it", "jm": "jm", "jp": "jp",
        "jo": "jo", "kz": "kz", "ke": "ke", "kp": "kp", "kr": "kr",
        "kw": "kw", "lv": "lv", "lb": "lb", "ly": "ly", "lt": "lt",
        "lu": "lu", "my": "my", "mx": "mx", "ma": "ma", "nl": "nl",
        "nz": "nz", "ng": "ng", "no": "no", "om": "om", "pk": "pk",
        "pa": "pa", "pe": "pe", "ph": "ph", "pl": "pl", "pt": "pt",
        "qa": "qa", "ro": "ro", "ru": "ru", "sa": "sa", "rs": "rs",
        "sg": "sg", "sk": "sk", "si": "si", "za": "za", "es": "es",
        "lk": "lk", "se": "se", "ch": "ch", "sy": "sy", "tw": "tw",
        "th": "th", "tn": "tn", "tr": "tr", "ua": "ua", "ae": "ae",
        "uk": "gb",  # .uk ccTLD → GB (ISO alpha-2 for United Kingdom)
        "us": "us", "uz": "uz", "ve": "ve", "vn": "vn", "ye": "ye",
        "zw": "zw",
    }

    # Domains that use .com/.net/.org but have a clear geo-identity.
    _KNOWN_DOMAIN_REGIONS: dict[str, str] = {
        # Ukraine
        "kyivindependent.com": "ua", "ukrinform.net": "ua", "unian.net": "ua",
        "radiosvoboda.org": "ua",
        # Iran — state media
        "presstv.ir": "ir", "tasnimnews.com": "ir", "farsnews.ir": "ir",
        "mehrnews.com": "ir", "iribnews.ir": "ir", "isna.ir": "ir",
        # Iran — diaspora / independent (report Iran's perspective, not from Iran)
        "iranintl.com": "gb", "radiofarda.com": "us", "iranwire.com": "gb",
        # Iraq
        "rudaw.net": "iq", "baghdadpost.net": "iq", "iraqinews.com": "iq",
        "shafaq.com": "iq",
        # Gulf / Arab
        "aljazeera.com": "qa", "aljazeera.net": "qa",
        "arabnews.com": "sa", "gulfnews.com": "ae",
        "thenationalnews.com": "ae", "middleeasteye.net": "gb",
        # Israel
        "timesofisrael.com": "il", "haaretz.com": "il",
        "jpost.com": "il", "ynetnews.com": "il",
        # Russia
        "rt.com": "ru", "tass.com": "ru", "interfax.ru": "ru",
        "ria.ru": "ru",
        # China
        "xinhua.net": "cn", "globaltimes.cn": "cn", "cgtn.com": "cn",
        # India / Pakistan
        "thehindu.com": "in", "ndtv.com": "in", "hindustantimes.com": "in",
        "dawn.com": "pk", "geo.tv": "pk",
        # Latin America
        "infobae.com": "ar", "clarin.com": "ar",
        "folha.uol.com.br": "br", "globo.com": "br",
        "latercera.com": "cl", "emol.com": "cl",
        "eltiempo.com": "co",
        # Major Western outlets (needed because .com is geo-ambiguous)
        "reuters.com": "gb", "bbc.com": "gb", "theguardian.com": "gb",
        "apnews.com": "us", "nytimes.com": "us", "washingtonpost.com": "us",
        "cnn.com": "us", "france24.com": "fr", "dw.com": "de",
    }

    # Countries where native internet is heavily restricted/geo-blocked.
    # BrightData unavailability for these is logged at WARN level.
    _BLOCKED_REGIONS: frozenset[str] = frozenset({"ir", "kp", "by", "cu", "sy", "ru"})

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.tavily_api_key = cfg.tavily_api_key
        self.brightdata_api_key = cfg.brightdata_api_key
        self.evaluator_model = cfg.evaluator_model
        self.api_key = cfg.litellm_api_key
        self.base_url = cfg.litellm_base_url
        self.completion_compat_kwargs = completion_compat_kwargs(self.evaluator_model, self.base_url)

        self.active_search = cfg.active_search or {}
        self.telemetry_enabled = bool(self.active_search.get("telemetry_enabled", False))
        self.telemetry_db_path = Path(self.active_search.get("telemetry_db_path", DB_PATH))
        self.cost_tracking = self.active_search.get("cost_tracking", {}) or {}
        self.billing = self.cost_tracking.get("billing", {}) or {}
        self.result_max_age_h = self.active_search.get("result_max_age_hours", 72)
        self.min_content_chars = self.active_search.get("min_content_chars", 150)
        self.max_results_per_region = max(1, int(self.active_search.get("max_results_per_region", 1)))
        self.min_query_token_overlap = self.active_search.get("min_query_token_overlap", 0.34)
        self.min_cluster_title_overlap = self.active_search.get("min_cluster_title_overlap", 0.08)
        self.max_existing_title_overlap = self.active_search.get("max_existing_title_overlap", 0.82)
        self.semantic_match_threshold = self.active_search.get("semantic_match_threshold", 0.58)
        self.allow_unknown_freshness_for_official = self.active_search.get(
            "allow_unknown_freshness_for_official", False
        )
        self.official_web_sources: dict[str, list[dict[str, Any]]] = (
            self.active_search.get("official_web_sources", {}) or {}
        )
        self.official_social_sources: dict[str, list[dict[str, Any]]] = (
            self.active_search.get("official_social_sources", {}) or {}
        )
        self.search_profiles: dict[str, dict[str, Any]] = self.active_search.get("search_profiles", {}) or {}

        self.region_config: dict[str, RegionConfig] = {}
        self._build_region_config()
        self._web_search_cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self._official_web_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._official_social_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.social_providers: dict[str, SocialFallbackProvider] = {
            "x": XOfficialProvider(
                cfg.x_bearer_token,
                event_recorder=self._record_search_event,
                cost_lookup=self._estimate_request_cost,
            ),
            "youtube": YouTubeOfficialProvider(cfg.youtube_api_key),
        }

    def _reset_request_caches(self) -> None:
        self._web_search_cache.clear()
        self._official_web_cache.clear()
        self._official_social_cache.clear()

    def _billing_config(self, provider_key: str) -> dict[str, Any]:
        return self.billing.get(provider_key, {}) or {}

    def _estimate_request_cost(
        self,
        provider: str,
        request_type: str,
        result_count: int | None,
        http_status: int | None,
    ) -> float | None:
        provider_key = {
            ("brightdata_serp", "search"): "brightdata_serp",
            ("tavily_search", "search"): "tavily_search",
            ("x", "recent_search"): "x_recent_search",
            ("x", "user_lookup"): "x_user_lookup",
            ("x", "user_timeline"): "x_user_timeline",
        }.get((provider, request_type))
        if provider_key is None:
            return None

        billing = self._billing_config(provider_key)
        unit_cost = billing.get("unit_cost_usd")
        if unit_cost is None:
            return None
        if http_status is not None and http_status >= 400:
            return 0.0
        if http_status is None and not result_count:
            return 0.0

        pricing_mode = billing.get("pricing_mode", "per_request")
        if pricing_mode == "per_result":
            billed_results = max(int(result_count or 0), 0)
            return round(float(unit_cost) * billed_results, 6)
        return round(float(unit_cost), 6)

    def _record_search_event(
        self,
        provider: str,
        request_type: str,
        target_region: str | None = None,
        query: str | None = None,
        account_id: str | None = None,
        http_status: int | None = None,
        result_count: int | None = None,
        accepted_count: int | None = None,
        duration_ms: int | None = None,
        estimated_cost_usd: float | None = None,
    ) -> None:
        if not self.telemetry_enabled:
            return
        insert_search_request_event(
            SearchRequestEvent(
                provider=provider,
                request_type=request_type,
                target_region=target_region,
                query=query,
                account_id=account_id,
                http_status=http_status,
                result_count=result_count,
                accepted_count=accepted_count,
                duration_ms=duration_ms,
                estimated_cost_usd=estimated_cost_usd,
            ),
            db_path=self.telemetry_db_path,
        )

    def _build_region_config(self) -> None:
        region_data: dict[str, dict[str, Any]] = {}

        for source in self.cfg.sources:
            if source.region not in region_data:
                region_data[source.region] = {"language_counts": {}, "domains": []}
            language_counts = region_data[source.region]["language_counts"]
            language_counts[source.language] = language_counts.get(source.language, 0) + 1
            domain = urllib.parse.urlparse(source.url).netloc.replace("www.", "")
            if domain and domain not in region_data[source.region]["domains"]:
                region_data[source.region]["domains"].append(domain)

        all_regions = set(region_data) | set(self.search_profiles)
        for region in all_regions:
            data = region_data.get(region, {"language_counts": {}, "domains": []})
            profile = self.search_profiles.get(region, {})
            language_counts = data["language_counts"]
            language = (
                profile.get("language")
                or self._DEFAULT_REGION_SEARCH_LANGUAGES.get(region)
                or (
                    max(language_counts.items(), key=lambda item: (item[1], item[0] == "en"))[0]
                    if language_counts
                    else "en"
                )
            )
            trusted_domains = list(
                dict.fromkeys([*data["domains"], *(profile.get("trusted_domains", []) or [])])
            )
            self.region_config[region] = RegionConfig(
                language=language,
                native_query_suffix=profile.get("native_query_suffix")
                or self._NATIVE_QUERY_SUFFIXES.get(language, "news"),
                geo_location=profile.get("geo_location", region),
                trusted_domains=trusted_domains,
            )

        # Auto-populate minimal configs for all known regions not yet covered.
        # This ensures the LLM can seek any country it identifies as central,
        # without requiring every country to have a configured news source.
        for region, language in self._DEFAULT_REGION_SEARCH_LANGUAGES.items():
            if region not in self.region_config:
                self.region_config[region] = RegionConfig(
                    language=language,
                    native_query_suffix=self._NATIVE_QUERY_SUFFIXES.get(language, "news"),
                    geo_location=region,
                    trusted_domains=[],
                )

    def enhance_clusters(self, clusters: list[ArticleCluster]) -> list[ArticleCluster]:
        can_search = bool(
            self.tavily_api_key
            or self.brightdata_api_key
            or self.official_web_sources
            or self.cfg.x_bearer_token
            or self.official_social_sources
        )
        if not can_search:
            logger.warning("No search credentials configured. Skipping active perspective seeking.")
            return clusters

        self._reset_request_caches()
        provider_counts: dict[str, int] = {}
        for cluster in clusters:
            try:
                self._enrich_cluster(cluster, provider_counts)
            except Exception as exc:
                logger.error("Active seeking failed for cluster '%s': %s", cluster.topic_category, exc)

        if provider_counts:
            summary = ", ".join(f"{v} via {k}" for k, v in sorted(provider_counts.items()))
            total = sum(provider_counts.values())
            logger.info("Seeker: %d articles injected (%s)", total, summary)

        return clusters

    def _enrich_cluster(self, cluster: ArticleCluster, provider_counts: dict[str, int] | None = None) -> None:
        missing_regions, search_keyword = self._analyze_missing_perspectives(cluster)
        if not missing_regions or not search_keyword:
            return

        for region in missing_regions:
            search_queries = self._build_search_queries(cluster, region, search_keyword)
            logger.info(
                "Cluster '%s': Missing perspective from %s. canonical_query='%s' search_queries=%s",
                cluster.topic_category,
                region,
                search_keyword,
                search_queries,
            )
            new_articles, provider = self._search_and_fetch(cluster, region, search_keyword, search_queries)
            if not new_articles:
                logger.info(
                    "Cluster '%s': No valid regional perspective found for %s",
                    cluster.topic_category,
                    region,
                )
                continue

            cluster.articles.extend(new_articles)
            for article in new_articles:
                if article.source_name not in cluster.sources:
                    cluster.sources.append(article.source_name)
            logger.info("Injected %d articles for region %s (via %s)", len(new_articles), region, provider)
            if provider_counts is not None and provider:
                provider_counts[provider] = provider_counts.get(provider, 0) + len(new_articles)

    def _analyze_missing_perspectives(self, cluster: ArticleCluster) -> tuple[list[str], str]:
        current_regions = {self._get_source_region(source) for source in cluster.sources}
        current_regions.update(
            article.origin_region for article in cluster.articles if article.origin_region
        )
        current_regions.discard("unknown")
        current_regions.discard(None)

        context = "\n".join(f"- {article.title}" for article in cluster.articles[:5])
        prompt = (
            f"Here are the headlines for a news event:\n{context}\n\n"
            "Analyze these headlines and output a JSON object with these fields:\n"
            "1. 'central_countries': A list of ISO 3166-1 alpha-2 country codes (e.g. ['us', 'jp']) "
            "for countries CENTRAL to this story whose perspective is essential.\n"
            "2. 'search_query': A concise 3-5 word English search query to find news about this event.\n"
            "Return ONLY the JSON object, nothing else."
        )

        try:
            response = litellm.completion(
                model=self.evaluator_model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=100,
                **self.completion_compat_kwargs,
            )
            content = response.choices[0].message.content or "{}"
            start = content.find("{")
            end = content.rfind("}") + 1
            if start == -1 or end == 0:
                return [], ""

            data = json.loads(content[start:end])
            central_countries = [region.lower() for region in data.get("central_countries", [])]
            search_query = data.get("search_query", "")
            potentially_missing = [region for region in central_countries if region not in current_regions]
            if not potentially_missing:
                return [], search_query

            truly_missing = []
            for region in potentially_missing:
                if self._is_perspective_missing(cluster, region):
                    truly_missing.append(region)
                else:
                    logger.debug(
                        "Cluster '%s': %s already represented by existing reporting",
                        cluster.topic_category,
                        region,
                    )

            return truly_missing, search_query

        except Exception as exc:
            logger.debug("Failed to analyze missing perspectives: %s", exc)
            return [], ""

    def _is_perspective_missing(self, cluster: ArticleCluster, region: str) -> bool:
        region_name = self._get_region_name(region)
        context = "\n".join(
            f"- [{article.source_name} / {article.origin_region or self._get_source_region(article.source_name)}] "
            f"{article.title}"
            for article in cluster.articles[:5]
        )
        prompt = (
            f"Here are headlines from articles about a news event, with source outlet and outlet region:\n{context}\n\n"
            f"Is {region_name}'s perspective (official position, industry response, or public opinion) "
            f"already represented by ORIGINAL reporting or official statements from {region_name} in these articles?\n\n"
            "Do NOT count translated or syndicated reporting, quoted material, or third-country coverage ABOUT that "
            "country as that country's own perspective.\n\n"
            "Return ONLY a JSON object: {\"perspective_covered\": true} or {\"perspective_covered\": false}"
        )

        try:
            response = litellm.completion(
                model=self.evaluator_model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=30,
                **self.completion_compat_kwargs,
            )
            content = response.choices[0].message.content or "{}"
            start = content.find("{")
            end = content.rfind("}") + 1
            if start != -1 and end != 0:
                data = json.loads(content[start:end])
                return not data.get("perspective_covered", False)
            return True
        except Exception as exc:
            logger.debug("Semantic check failed for %s: %s", region, exc)
            return True

    def _search_and_fetch(
        self,
        cluster: ArticleCluster,
        region: str,
        search_keyword: str,
        search_queries: list[str] | None = None,
    ) -> tuple[list[Article], str]:
        if search_queries is None:
            search_queries = self._build_search_queries(cluster, region, search_keyword)

        # Stage 1 & 2: News search (BrightData → Tavily per query)
        for query in search_queries:
            news_results = self._search_brightdata(region, query)
            if news_results:
                accepted = self._accept_results(cluster, region, query, news_results)
                if accepted:
                    return accepted, "BrightData"
            news_results = self._search_tavily(region, query)
            if news_results:
                accepted = self._accept_results(cluster, region, query, news_results)
                if accepted:
                    return accepted, "Tavily"

        # Stage 3: Curated official web sources
        official_web_results = self._search_official_web(region)
        if official_web_results:
            accepted_web = self._accept_results(cluster, region, search_queries[0], official_web_results)
            if accepted_web:
                return accepted_web, "official_web"

        # Stage 4: Curated official social timelines, only when region policy enables X as the final fallback
        if not self._x_final_fallback_enabled(region):
            return [], ""
        social_results = self._search_official_social(region, search_queries[0])
        if not social_results:
            return [], ""
        accepted_social = self._accept_results(cluster, region, search_queries[0], social_results)
        if accepted_social:
            return accepted_social, "official_social"
        return [], ""

    def _accept_results(
        self,
        cluster: ArticleCluster,
        region: str,
        search_keyword: str,
        results: list[dict[str, Any]],
    ) -> list[Article]:
        existing_urls = {article.url for article in cluster.articles}
        accepted_sources = {article.source_name for article in cluster.articles if article.search_region == region}
        accepted: list[Article] = []
        for result in results:
            article = self._result_to_article(result, region)
            if article is None or article.url in existing_urls:
                continue
            if article.source_name in accepted_sources:
                continue
            if not self._is_result_acceptable(cluster, article, search_keyword):
                continue
            accepted.append(article)
            accepted_sources.add(article.source_name)
            if len(accepted) >= self.max_results_per_region:
                break
        return accepted

    def _result_to_article(self, result: dict[str, Any], region: str) -> Article | None:
        url = result.get("url")
        title = (result.get("title") or "").strip()
        content = (result.get("content") or "").strip()
        min_content_chars = (
            30 if result.get("source_kind") in {"official_social", "official_web"} else self.min_content_chars
        )
        if not url or not title or len(content) < min_content_chars:
            return None

        source_domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
        source_name = result.get("source_name") or self._get_source_name_by_domain(source_domain) or source_domain
        published_at = self._parse_published_at(result.get("published_at"), url)
        source_kind = result.get("source_kind", "news")
        origin_region = result.get("origin_region")
        if not origin_region and source_kind == "news":
            origin_region = self._get_source_region(source_name)
        if not origin_region and result.get("is_official_source"):
            origin_region = region

        return Article(
            id=None,
            url=url,
            title=title,
            source_name=source_name,
            published_at=published_at or datetime.now(tz=timezone.utc),
            content=content,
            is_searched=True,
            search_region=region,
            source_kind=source_kind,
            platform=result.get("platform"),
            account_id=result.get("account_id"),
            is_official_source=bool(result.get("is_official_source", False)),
            origin_region=origin_region,
            searched_provider=result.get("searched_provider"),
        )

    def _is_result_acceptable(self, cluster: ArticleCluster, article: Article, search_keyword: str) -> bool:
        if not self._is_region_valid(article):
            logger.debug("Rejected %s: region mismatch", article.url)
            return False
        if not self._passes_freshness(article):
            logger.debug("Rejected %s: stale or unknown freshness", article.url)
            return False
        if not self._passes_event_match(cluster, article, search_keyword):
            logger.debug("Rejected %s: event match too weak", article.url)
            return False
        if not self._adds_new_angle(cluster, article):
            logger.debug("Rejected %s: repeated existing angle", article.url)
            return False
        return True

    def _is_region_valid(self, article: Article) -> bool:
        return article.origin_region == article.search_region and article.search_region is not None

    def _passes_freshness(self, article: Article) -> bool:
        parsed = self._parse_published_at(article.published_at, article.url)
        if parsed is None:
            return article.is_official_source and self.allow_unknown_freshness_for_official
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=self.result_max_age_h)
        return parsed >= cutoff

    def _passes_event_match(self, cluster: ArticleCluster, article: Article, search_keyword: str) -> bool:
        lexical_score = max(
            self._query_token_overlap(search_keyword, f"{article.title}\n{article.content[:800]}"),
            self._cluster_title_overlap(cluster, article.title),
        )
        if lexical_score >= max(self.min_query_token_overlap, self.min_cluster_title_overlap):
            return True
        return self._semantic_event_match(cluster, article) >= self.semantic_match_threshold

    def _adds_new_angle(self, cluster: ArticleCluster, article: Article) -> bool:
        max_overlap = 0.0
        for existing in cluster.articles:
            max_overlap = max(max_overlap, self._text_overlap(existing.title, article.title))
            if existing.url == article.url:
                return False
            if existing.source_name == article.source_name and article.source_kind == existing.source_kind:
                return False
        return max_overlap < self.max_existing_title_overlap

    def _query_token_overlap(self, search_keyword: str, text: str) -> float:
        tokens = self._keyword_tokens(search_keyword)
        if not tokens:
            return 0.0
        normalized_text = text.lower()
        matched = sum(1 for token in tokens if token in normalized_text)
        return matched / len(tokens)

    def _cluster_title_overlap(self, cluster: ArticleCluster, candidate_title: str) -> float:
        return max(
            (self._text_overlap(article.title, candidate_title) for article in cluster.articles[:6]),
            default=0.0,
        )

    def _semantic_event_match(self, cluster: ArticleCluster, article: Article) -> float:
        model = _get_model()
        cluster_context = " ".join(existing.title for existing in cluster.articles[:5])
        article_context = f"{article.title} {article.content[:500]}"
        embs = model.encode([cluster_context, article_context], normalize_embeddings=True, show_progress_bar=False)
        return float(np.dot(embs[0], embs[1]))

    def _x_final_fallback_enabled(self, region: str) -> bool:
        profile = self.search_profiles.get(region, {}) or {}
        return bool(profile.get("x_final_fallback", False))

    def _search_official_web(self, region: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for source in self.official_web_sources.get(region, []):
            source_key = str(source.get("url") or source.get("source_name") or "")
            platform = str(source.get("platform") or "static_html")
            cache_key = (platform, source_key)
            if cache_key in self._official_web_cache:
                results.extend(self._official_web_cache[cache_key])
                continue
            try:
                provider_results = self._fetch_official_web_source(region, source)
                self._official_web_cache[cache_key] = provider_results
                results.extend(provider_results)
            except Exception as exc:
                logger.debug(
                    "Official web fallback failed for %s/%s: %s",
                    region,
                    source.get("source_name") or source.get("url"),
                    exc,
                )
        return results

    def _fetch_official_web_source(self, region: str, source: dict[str, Any]) -> list[dict[str, Any]]:
        platform = str(source.get("platform") or "static_html")
        if platform in {"rss", "rsshub"}:
            return self._fetch_official_web_rss(region, source, platform)
        if platform == "json_api":
            return self._fetch_official_web_json_api(region, source)
        if platform == "static_html":
            return self._fetch_official_web_static_html(region, source)
        logger.debug("Unsupported official_web platform %s for region %s", platform, region)
        return []

    def _fetch_official_web_rss(
        self,
        region: str,
        source: dict[str, Any],
        request_type: str,
    ) -> list[dict[str, Any]]:
        url = str(source.get("url") or "").strip()
        if not url:
            return []
        headers = self._official_web_headers(source)
        started = monotonic()
        try:
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                resp = client.get(url, headers=headers)
                duration_ms = int((monotonic() - started) * 1000)
                resp.raise_for_status()
            feed = feedparser.parse(resp.text)
            entries = list(feed.entries or [])[: self._official_source_limit(source)]
            results: list[dict[str, Any]] = []
            for entry in entries:
                link = (entry.get("link") or "").strip()
                title = (entry.get("title") or "").strip()
                if not link or not title:
                    continue
                content = self._content_from_feed_entry(entry)
                if len(content) < 30:
                    content = self._fetch_official_web_article_text(link, headers)
                published = entry.get("published") or entry.get("updated")
                if not published:
                    pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
                    if pub_struct:
                        published = datetime(*pub_struct[:6], tzinfo=timezone.utc).isoformat()
                results.append(
                    self._official_web_result(
                        region=region,
                        source=source,
                        url=link,
                        title=title,
                        content=content,
                        published_at=published,
                        searched_provider=f"official_web_{request_type}",
                    )
                )
            self._record_search_event(
                provider="official_web",
                request_type=request_type,
                target_region=region,
                query=url,
                account_id=source.get("source_name"),
                http_status=resp.status_code,
                result_count=len(results),
                accepted_count=None,
                duration_ms=duration_ms,
                estimated_cost_usd=None,
            )
            return results
        except Exception as exc:
            response = getattr(exc, "response", None)
            self._record_search_event(
                provider="official_web",
                request_type=request_type,
                target_region=region,
                query=url,
                account_id=source.get("source_name"),
                http_status=getattr(response, "status_code", None),
                result_count=0,
                accepted_count=None,
                duration_ms=None,
                estimated_cost_usd=None,
            )
            raise

    def _fetch_official_web_json_api(self, region: str, source: dict[str, Any]) -> list[dict[str, Any]]:
        url = str(source.get("url") or "").strip()
        if not url:
            return []
        headers = self._official_web_headers(source)
        started = monotonic()
        try:
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                resp = client.get(url, headers=headers)
                duration_ms = int((monotonic() - started) * 1000)
                resp.raise_for_status()
                payload = resp.json()
            items = self._extract_json_items(payload)[: self._official_source_limit(source)]
            results: list[dict[str, Any]] = []
            for item in items:
                link = self._resolve_official_web_url(url, self._json_value(item, "url", "link", "href"))
                title = self._json_value(item, "title", "headline", "name")
                if not link or not title:
                    continue
                content = self._json_value(item, "content", "summary", "description", "body", "excerpt")
                if len(content) < 30:
                    content = self._fetch_official_web_article_text(link, headers)
                results.append(
                    self._official_web_result(
                        region=region,
                        source=source,
                        url=link,
                        title=title,
                        content=content,
                        published_at=self._json_value(
                            item,
                            "published_at",
                            "published",
                            "date",
                            "updated_at",
                            "created_at",
                        ),
                        searched_provider="official_web_json_api",
                    )
                )
            self._record_search_event(
                provider="official_web",
                request_type="json_api",
                target_region=region,
                query=url,
                account_id=source.get("source_name"),
                http_status=resp.status_code,
                result_count=len(results),
                accepted_count=None,
                duration_ms=duration_ms,
                estimated_cost_usd=None,
            )
            return results
        except Exception as exc:
            response = getattr(exc, "response", None)
            self._record_search_event(
                provider="official_web",
                request_type="json_api",
                target_region=region,
                query=url,
                account_id=source.get("source_name"),
                http_status=getattr(response, "status_code", None),
                result_count=0,
                accepted_count=None,
                duration_ms=None,
                estimated_cost_usd=None,
            )
            raise

    def _fetch_official_web_static_html(self, region: str, source: dict[str, Any]) -> list[dict[str, Any]]:
        url = str(source.get("url") or "").strip()
        item_selector = str(source.get("item_selector") or "").strip()
        if not url or not item_selector:
            return []
        headers = self._official_web_headers(source)
        started = monotonic()
        try:
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                resp = client.get(url, headers=headers)
                duration_ms = int((monotonic() - started) * 1000)
                resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            items = soup.select(item_selector)[: self._official_source_limit(source)]
            results: list[dict[str, Any]] = []
            for item in items:
                title = self._selected_text(item, source.get("title_selector")) or item.get_text(" ", strip=True)
                link = self._selected_attr(item, source.get("link_selector"), "href")
                link = self._resolve_official_web_url(url, link)
                if not title or not link:
                    continue
                content = item.get_text(" ", strip=True)
                if len(content) < 30:
                    content = self._fetch_official_web_article_text(link, headers)
                results.append(
                    self._official_web_result(
                        region=region,
                        source=source,
                        url=link,
                        title=title,
                        content=content,
                        published_at=self._selected_text(item, source.get("date_selector")),
                        searched_provider="official_web_static_html",
                    )
                )
            self._record_search_event(
                provider="official_web",
                request_type="static_html",
                target_region=region,
                query=url,
                account_id=source.get("source_name"),
                http_status=resp.status_code,
                result_count=len(results),
                accepted_count=None,
                duration_ms=duration_ms,
                estimated_cost_usd=None,
            )
            return results
        except Exception as exc:
            response = getattr(exc, "response", None)
            self._record_search_event(
                provider="official_web",
                request_type="static_html",
                target_region=region,
                query=url,
                account_id=source.get("source_name"),
                http_status=getattr(response, "status_code", None),
                result_count=0,
                accepted_count=None,
                duration_ms=None,
                estimated_cost_usd=None,
            )
            raise

    def _official_web_result(
        self,
        *,
        region: str,
        source: dict[str, Any],
        url: str,
        title: str,
        content: str,
        published_at: str | datetime | None,
        searched_provider: str,
    ) -> dict[str, Any]:
        source_name = str(source.get("source_name") or self._get_source_name_by_domain(
            urllib.parse.urlparse(url).netloc.replace("www.", "")
        ) or urllib.parse.urlparse(url).netloc.replace("www.", ""))
        return {
            "url": url,
            "title": title.strip(),
            "content": content.strip(),
            "published_at": published_at,
            "source_name": source_name,
            "origin_region": source.get("region") or region,
            "source_kind": "official_web",
            "platform": None,
            "account_id": None,
            "is_official_source": True,
            "searched_provider": searched_provider,
        }

    def _official_web_headers(self, source: dict[str, Any]) -> dict[str, str]:
        headers = {
            "User-Agent": "NewsPrism/1.0 (+official-web-fallback)",
            "Accept-Language": "en-US,en;q=0.9",
        }
        extra_headers = source.get("headers") or {}
        if isinstance(extra_headers, dict):
            headers.update({str(key): str(value) for key, value in extra_headers.items()})
        return headers

    def _official_source_limit(self, source: dict[str, Any]) -> int:
        configured = int(source.get("max_results", self.max_results_per_region + 4) or (self.max_results_per_region + 4))
        return max(1, min(configured, 10))

    def _content_from_feed_entry(self, entry: Any) -> str:
        contents: list[str] = []
        if entry.get("summary"):
            contents.append(str(entry.get("summary")))
        if entry.get("description"):
            contents.append(str(entry.get("description")))
        for value in entry.get("content", []) or []:
            if isinstance(value, dict) and value.get("value"):
                contents.append(str(value["value"]))
        combined = " ".join(part for part in contents if part)
        return BeautifulSoup(combined, "lxml").get_text(" ", strip=True) if combined else ""

    def _fetch_official_web_article_text(self, url: str, headers: dict[str, str] | None = None) -> str:
        started = monotonic()
        try:
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                resp = client.get(url, headers=headers or self._official_web_headers({}))
                duration_ms = int((monotonic() - started) * 1000)
                resp.raise_for_status()
            extracted = trafilatura.extract(resp.text, include_comments=False, favor_recall=True) or ""
            if not extracted:
                extracted = BeautifulSoup(resp.text, "lxml").get_text("\n", strip=True)
            self._record_search_event(
                provider="official_web",
                request_type="article_fetch",
                target_region=None,
                query=url,
                account_id=None,
                http_status=resp.status_code,
                result_count=1 if extracted else 0,
                accepted_count=None,
                duration_ms=duration_ms,
                estimated_cost_usd=None,
            )
            return extracted.strip()
        except Exception as exc:
            response = getattr(exc, "response", None)
            self._record_search_event(
                provider="official_web",
                request_type="article_fetch",
                target_region=None,
                query=url,
                account_id=None,
                http_status=getattr(response, "status_code", None),
                result_count=0,
                accepted_count=None,
                duration_ms=None,
                estimated_cost_usd=None,
            )
            return ""

    def _selected_text(self, element: Any, selector: Any) -> str:
        selector_text = str(selector or "").strip()
        if not selector_text:
            return ""
        if selector_text == ":self":
            return element.get_text(" ", strip=True)
        selected = element.select_one(selector_text)
        if selected is None:
            return ""
        return selected.get_text(" ", strip=True)

    def _selected_attr(self, element: Any, selector: Any, attr: str) -> str:
        selector_text = str(selector or "").strip()
        if not selector_text:
            return ""
        if selector_text == ":self":
            return str(element.get(attr) or "").strip()
        selected = element.select_one(selector_text)
        if selected is None:
            return ""
        return str(selected.get(attr) or "").strip()

    def _resolve_official_web_url(self, base_url: str, maybe_url: Any) -> str:
        value = str(maybe_url or "").strip()
        if not value:
            return ""
        return urllib.parse.urljoin(base_url, value)

    def _extract_json_items(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("items", "results", "articles", "posts", "news", "data"):
                value = payload.get(key)
                items = self._extract_json_items(value)
                if items:
                    return items
        return []

    def _json_value(self, item: dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, dict):
                nested = self._json_value(value, *keys)
                if nested:
                    return nested
        return ""

    def _search_official_social(self, region: str, keyword: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for source in self.official_social_sources.get(region, []):
            provider = self.social_providers.get(source.get("platform"))
            if not provider:
                continue
            source_key = str(source.get("user_id") or source.get("username") or source.get("channel_id") or "")
            cache_key = (str(source.get("platform") or "unknown"), source_key)
            if cache_key in self._official_social_cache:
                results.extend(self._official_social_cache[cache_key])
                continue
            try:
                provider_results = provider.fetch_recent(source, keyword, region)
                self._official_social_cache[cache_key] = provider_results
                results.extend(provider_results)
            except Exception as exc:
                logger.debug("Official social fallback failed for %s/%s: %s", region, source.get("platform"), exc)
        return results

    def _infer_region_from_url(self, url: str) -> str | None:
        """Infer region from URL using domain whitelist then ccTLD mapping."""
        if not url:
            return None
        domain = urllib.parse.urlparse(url).netloc.lower().replace("www.", "").strip(".")
        if not domain:
            return None

        # Check known domain → region whitelist first (exact match)
        if domain in self._KNOWN_DOMAIN_REGIONS:
            return self._KNOWN_DOMAIN_REGIONS[domain]

        # Check if any known domain is a suffix of the URL domain
        for known_domain, region in self._KNOWN_DOMAIN_REGIONS.items():
            if domain.endswith("." + known_domain) or domain == known_domain:
                return region

        # Extract ccTLD and look up in map
        parts = domain.split(".")
        if len(parts) >= 2:
            tld = parts[-1]
            if tld in self._CCTLD_REGION_MAP:
                return self._CCTLD_REGION_MAP[tld]
            # Handle second-level ccTLDs like .co.uk, .com.au
            if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "net", "gov", "ac"):
                sld_tld = parts[-1]
                if sld_tld in self._CCTLD_REGION_MAP:
                    return self._CCTLD_REGION_MAP[sld_tld]

        return None

    def _build_search_queries(self, cluster: ArticleCluster, region: str, keyword: str) -> list[str]:
        config = self.region_config.get(region)
        english_query = self._build_english_query(region, keyword)
        if not config or config.language == "en":
            return [english_query]

        localized_keyword = self._localize_search_keyword(cluster, region, keyword)
        native_query = self._build_native_query(region, localized_keyword or keyword)
        if native_query == english_query:
            return [english_query]
        return [native_query, english_query]

    def _build_english_query(self, region: str, keyword: str) -> str:
        region_name = self._get_region_name(region)
        return f"{keyword} news {region_name}"

    def _localize_search_keyword(self, cluster: ArticleCluster, region: str, keyword: str) -> str:
        config = self.region_config.get(region)
        if not config or config.language == "en":
            return keyword

        language_name = self._get_language_name(config.language)
        region_name = self._get_region_name(region)
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

    def _build_native_query(self, region: str, keyword: str) -> str:
        config = self.region_config.get(region)
        if config and config.language != "en":
            if config.native_query_suffix in keyword:
                return keyword
            return f"{keyword} {config.native_query_suffix}"
        return self._build_english_query(region, keyword)

    def _search_brightdata(self, region: str, keyword: str) -> list[dict[str, Any]]:
        """Geo-targeted SERP search via BrightData SERP API. Primary search provider.

        Requires BRIGHTDATA_API_KEY env var. Returns Light JSON (organic results only).
        Falls through to Tavily if unavailable.
        """
        cache_key = ("brightdata_serp", region, keyword)
        if cache_key in self._web_search_cache:
            return self._web_search_cache[cache_key]

        config = self.region_config.get(region)
        if not config or not self.brightdata_api_key:
            if region in self._BLOCKED_REGIONS and not self.brightdata_api_key:
                logger.warning(
                    "BrightData not configured — geo-targeted search unavailable for restricted region %s. "
                    "Set BRIGHTDATA_API_KEY to improve coverage.",
                    region,
                )
            return []

        native_query = self._build_native_query(region, keyword)
        try:
            payload: dict[str, Any] = {
                "query": native_query,
                "country": region,
                "lang": config.language,
                "num": max(self.max_results_per_region + 2, 4),
                "output": "json_light",  # organic[]{url,title,description,date} only
            }
            started = monotonic()
            with httpx.Client(timeout=30) as client:
                resp = client.post(
                    "https://api.brightdata.com/serp/google",
                    json=payload,
                    headers={"Authorization": f"Bearer {self.brightdata_api_key}"},
                )
                duration_ms = int((monotonic() - started) * 1000)
                resp.raise_for_status()
                data = resp.json()

            results: list[dict[str, Any]] = []
            for item in data.get("organic", []):
                url = item.get("url") or item.get("link", "")
                if not url:
                    continue
                domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                source_name = self._get_source_name_by_domain(domain) or domain
                origin_region = self._get_source_region(source_name)
                if origin_region == "unknown":
                    origin_region = self._infer_region_from_url(url) or "unknown"
                results.append({
                    "url": url,
                    "title": item.get("title", ""),
                    "content": item.get("description") or item.get("snippet", ""),
                    "published_at": item.get("date"),
                    "source_name": source_name,
                    "origin_region": origin_region,
                    "source_kind": "news",
                    "platform": None,
                    "account_id": None,
                    "is_official_source": False,
                    "searched_provider": "brightdata_serp",
                })
            self._record_search_event(
                provider="brightdata_serp",
                request_type="search",
                target_region=region,
                query=native_query,
                http_status=resp.status_code,
                result_count=len(results),
                accepted_count=None,
                duration_ms=duration_ms,
                estimated_cost_usd=self._estimate_request_cost(
                    "brightdata_serp",
                    "search",
                    len(results),
                    resp.status_code,
                ),
            )
            self._web_search_cache[cache_key] = results
            logger.debug("BrightData returned %d results for %s: %s", len(results), region, native_query)
            return results

        except Exception as exc:
            response = getattr(exc, "response", None)
            self._record_search_event(
                provider="brightdata_serp",
                request_type="search",
                target_region=region,
                query=native_query,
                http_status=getattr(response, "status_code", None),
                result_count=0,
                accepted_count=None,
                duration_ms=None,
                estimated_cost_usd=self._estimate_request_cost(
                    "brightdata_serp",
                    "search",
                    0,
                    getattr(response, "status_code", None),
                ),
            )
            logger.debug("BrightData search failed for %s: %s", region, exc)
            return []

    def _search_tavily(self, region: str, query: str) -> list[dict[str, Any]]:
        cache_key = ("tavily_search", region, query)
        if cache_key in self._web_search_cache:
            return self._web_search_cache[cache_key]

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
                source_name = self._get_source_name_by_domain(source_domain) or source_domain
                origin_region = self._get_source_region(source_name)
                # If the configured-source lookup fails, infer from URL domain/TLD
                if origin_region == "unknown":
                    origin_region = self._infer_region_from_url(url) or "unknown"
                results.append(
                    {
                        "url": url,
                        "title": result.get("title"),
                        "content": result.get("raw_content") or result.get("content", ""),
                        "published_at": result.get("published_date") or result.get("published_at"),
                        "source_name": source_name,
                        "origin_region": origin_region,
                        "source_kind": "news",
                        "platform": None,
                        "account_id": None,
                        "is_official_source": False,
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
                accepted_count=None,
                duration_ms=duration_ms,
                estimated_cost_usd=self._estimate_request_cost(
                    "tavily_search",
                    "search",
                    len(results),
                    resp.status_code,
                ),
            )
            self._web_search_cache[cache_key] = results
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
                accepted_count=None,
                duration_ms=None,
                estimated_cost_usd=self._estimate_request_cost(
                    "tavily_search",
                    "search",
                    0,
                    getattr(response, "status_code", None),
                ),
            )
            logger.debug("Tavily search failed: %s", exc)
            return []

    def _get_source_region(self, source_name: str) -> str:
        for source in self.cfg.sources:
            if source.name == source_name:
                return source.region
        return "unknown"

    def _get_source_name_by_domain(self, domain: str) -> str | None:
        if not domain:
            return None
        for source in self.cfg.sources:
            if domain in source.url:
                return source.name
        return None

    def _get_region_name(self, region: str) -> str:
        region_map = {
            "ae": "UAE",
            "ar": "Argentina",
            "au": "Australia",
            "az": "Azerbaijan",
            "bd": "Bangladesh",
            "br": "Brazil",
            "by": "Belarus",
            "ca": "Canada",
            "ch": "Switzerland",
            "cl": "Chile",
            "cn": "China",
            "co": "Colombia",
            "cu": "Cuba",
            "de": "Germany",
            "eg": "Egypt",
            "es": "Spain",
            "fr": "France",
            "gb": "UK",
            "id": "Indonesia",
            "il": "Israel",
            "in": "India",
            "iq": "Iraq",
            "ir": "Iran",
            "it": "Italy",
            "jo": "Jordan",
            "jp": "Japan",
            "ke": "Kenya",
            "kp": "North Korea",
            "kr": "South Korea",
            "kw": "Kuwait",
            "lb": "Lebanon",
            "ly": "Libya",
            "ma": "Morocco",
            "mx": "Mexico",
            "my": "Malaysia",
            "ng": "Nigeria",
            "nl": "Netherlands",
            "om": "Oman",
            "pe": "Peru",
            "ph": "Philippines",
            "pk": "Pakistan",
            "pl": "Poland",
            "qa": "Qatar",
            "ru": "Russia",
            "sa": "Saudi Arabia",
            "sg": "Singapore",
            "sy": "Syria",
            "th": "Thailand",
            "tr": "Turkey",
            "tw": "Taiwan",
            "ua": "Ukraine",
            "us": "US",
            "ve": "Venezuela",
            "vn": "Vietnam",
            "ye": "Yemen",
            "za": "South Africa",
        }
        return region_map.get(region, region.upper())

    def _get_language_name(self, language: str) -> str:
        return {
            "ar": "Arabic",
            "az": "Azerbaijani",
            "bn": "Bengali",
            "de": "German",
            "en": "English",
            "es": "Spanish",
            "fa": "Persian",
            "fr": "French",
            "he": "Hebrew",
            "hi": "Hindi",
            "id": "Indonesian",
            "it": "Italian",
            "ja": "Japanese",
            "ko": "Korean",
            "ms": "Malay",
            "nl": "Dutch",
            "pl": "Polish",
            "pt": "Portuguese",
            "ru": "Russian",
            "th": "Thai",
            "tr": "Turkish",
            "uk": "Ukrainian",
            "ur": "Urdu",
            "vi": "Vietnamese",
            "zh": "Simplified Chinese",
            "zh-tw": "Traditional Chinese",
        }.get(language, language)

    def _parse_published_at(self, value: Any, url: str | None = None) -> datetime | None:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        if isinstance(value, str) and value:
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return parsed.astimezone(timezone.utc)
            except ValueError:
                pass
        if url:
            match = re.search(r"(20\d{2})[-/](\d{2})[-/](\d{2})", url)
            if match:
                try:
                    return datetime(
                        int(match.group(1)),
                        int(match.group(2)),
                        int(match.group(3)),
                        tzinfo=timezone.utc,
                    )
                except ValueError:
                    return None
        return None

    def _keyword_tokens(self, text: str) -> list[str]:
        tokens = [token for token in re.findall(r"[a-z0-9]{2,}", text.lower()) if token not in {"news"}]
        if re.search(r"[^\x00-\x7f]", text):
            tokens.extend(sorted(self._ngram_tokens(text)))
        # Preserve order while removing duplicates.
        return list(dict.fromkeys(tokens))

    def _text_overlap(self, left: str, right: str) -> float:
        left_tokens = self._ngram_tokens(left)
        right_tokens = self._ngram_tokens(right)
        if not left_tokens or not right_tokens:
            return 0.0
        union = left_tokens | right_tokens
        if not union:
            return 0.0
        return len(left_tokens & right_tokens) / len(union)

    def _ngram_tokens(self, text: str, n: int = 2) -> set[str]:
        compact = re.sub(r"[^\w\u4e00-\u9fff]+", "", text.lower())
        if not compact:
            return set()
        if len(compact) <= n:
            return {compact}
        return {compact[idx : idx + n] for idx in range(len(compact) - n + 1)}
