"""Active Perspective Seeker — fills a specific missing first-party voice.

Search is allowed only after impact selection. It extracts the named actor from
the event itself, seeks that actor's official site or verified social account
first, and only then seeks a rigorously verified editorial source in that
actor's related country. It never searches unrelated absent countries and
never treats a result's country label as evidence of provenance.

Layer: service (imports types, config, repo for telemetry)
"""
from __future__ import annotations

import json
import logging
import re
import urllib.parse
import unicodedata
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any

import httpx
import litellm
import numpy as np
import tldextract
from rapidfuzz import fuzz

from newsprism.config import Config
from newsprism.repo import DB_PATH, insert_search_candidate_review, insert_search_request_event
from newsprism.service.embeddings import get_model
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.service.locales import (
    country_name,
    is_recognized_country,
    is_territory_name,
    language_name,
    query_languages,
)
from newsprism.types import (
    OWNERSHIP_GATE_ALLOW,
    Article,
    ArticleCluster,
    SearchCandidateReview,
    SearchRequestEvent,
)

logger = logging.getLogger(__name__)

litellm.set_verbose = False

@dataclass
class RegionConfig:
    """Search configuration for one major region."""
    language: str | list[str] | None = None


@dataclass(frozen=True)
class VoiceTarget:
    """One event-derived voice need, with country fallback when official fails."""
    region: str
    label: str
    role: str = "organization"
    event_role: str = field(default="principal", compare=False)
    evidence_text: str = field(default="", compare=False)
    materiality: str = field(default="required", compare=False)
    why_voice_needed: str = field(default="", compare=False)
    cluster_key: str = field(default="", compare=False)
    coverage_before: str = field(default="missing", compare=False)


@dataclass(frozen=True)
class CandidateIdentity:
    """Publisher ownership evidence produced by the candidate verifier."""

    source_type: str = "reject"
    publisher_entity: str = ""
    publisher_region: str = ""
    relationship: str = "uncertain"
    ownership_evidence: str = ""
    confidence: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "publisher_entity": self.publisher_entity,
            "publisher_region": self.publisher_region,
            "relationship": self.relationship,
            "ownership_evidence": self.ownership_evidence,
            "confidence": self.confidence,
        }


_ACTOR_ROLES = {
    "company", "government", "government_agency", "ministry", "party", "organization", "person",
}
_MATERIAL_EVENT_ROLES = {
    "principal",
    "decision_maker",
    "regulator",
    "claimant",
    "respondent",
    "accused_party",
    "directly_affected_principal",
    "contracting_party",
}
_NON_VOICE_LABEL_PATTERN = re.compile(
    r"(?:facility|factory|plant|mall|site|model|product|platform|store|"
    r"设施|核设施|工厂|商场|门店|产品|模型|平台)$",
    re.IGNORECASE,
)
_SOURCE_TYPES = {"official_web", "official_social", "country_editorial", "reject"}
_RELATIONSHIPS = {
    "same_entity", "quoted_by_third_party", "covered_by_third_party", "uncertain", "unrelated",
}
_TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())
# Public-suffix labels that mark a domain as clearly governmental. Checked
# against every dot-separated component of the registered suffix so both a
# bare TLD (centcom.mil -> "mil") and a ccTLD second-level suffix
# (gov.uk/gov.cn/gouv.fr) match.
_GOVERNMENTAL_SUFFIX_MARKERS = {"gov", "mil", "govt", "gouv"}
_CURRENT_EVENT_ACCEPTANCE_REASON = "current_event_perspective"
_OFFICIAL_BACKGROUND_PATTERN = re.compile(
    r"(?:"
    r"annual[-_ ]?report|business[-_ ]?report|financial[-_ ]?report|"
    r"quarterly[-_ ]?report|10[-_ ]?k|year[-_ ]?in[-_ ]?review|"
    r"investor[-_ ]?relations|(?:press|photo|media)?[-_ ]?gallery|"
    r"fact[-_ ]?sheet|factsheet|brochure|about[-_ ]?(?:the|us)|"
    r"company[-_ ]?overview|institutional[-_ ]?history|our[-_ ]?history|"
    r"backgrounder"
    r")",
    re.IGNORECASE,
)
_MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sept": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}


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
        self.max_localized_query_variants = int(search_cfg.get("max_localized_query_variants", 2))
        self.max_existing_title_overlap = float(search_cfg.get("max_existing_title_overlap", 0.82))
        self.min_semantic_event_match = float(search_cfg.get("min_semantic_event_match", 0.58))
        # A result may be useful context for the same conflict while still not
        # covering this event.  Keep that material visible as background, but
        # require a higher bar before it can increase perspective counts.
        self.min_direct_semantic_event_match = float(
            search_cfg.get("min_direct_semantic_event_match", 0.70)
        )
        # Above this verifier confidence, a same_entity official_web candidate
        # on a clearly governmental domain is trusted without also requiring
        # target.label and publisher_entity to normalize to identical text —
        # the verifier LLM already did that semantic judgment, and free-text
        # equality can't survive translation (中文 target label vs an English
        # publisher_entity) or acronym/full-name variance (CENTCOM vs "U.S.
        # Central Command (CENTCOM)"). See commit 0ffca0f for why this check
        # exists at all, and the 2026-07-31 false-rejection fix for why it was
        # loosened for this specific high-confidence + governmental-domain case.
        self.official_identity_confidence_floor = float(
            search_cfg.get("official_identity_confidence_floor", 0.85)
        )
        self.hot_composite_trigger = float(search_cfg.get("hot_composite_trigger", 0.55))
        self.max_queries_per_stage = max(1, int(search_cfg.get("max_queries_per_stage", 2)))
        self.max_requests_per_run = max(1, int(search_cfg.get("max_requests_per_run", 40)))
        # Country-stage acceptance: Tavily is already time-bounded (days=3) but
        # frequently omits published_date and URL-path dates aren't always
        # recoverable. Undated country-editorial results still must clear the
        # semantic event-match gate and the reviewed country-domain registry;
        # dated-stale results stay rejected. Official-stage pages remain strict
        # because undated official pages are usually background material.
        self.country_allow_undated = bool(search_cfg.get("country_allow_undated", True))
        # When an entity's own coverage can't be found (no result, mismatch, or
        # only dated-stale results), fall back to an entity-free country query
        # so at least one voice from that country appears. Reviewed domains only.
        self.country_entity_free_fallback = bool(
            search_cfg.get("country_entity_free_fallback", True)
        )

        profiles = search_cfg.get("search_profiles", {}) if isinstance(search_cfg, dict) else {}
        self.region_config = self._build_region_config(profiles)
        verdicts = search_cfg.get("source_verdicts", {}) if isinstance(search_cfg, dict) else {}
        self.source_verdicts = {
            str(domain).lower().removeprefix("www."): dict(spec)
            for domain, spec in (verdicts or {}).items()
            if isinstance(spec, dict)
        }
        if bool(search_cfg.get("use_configured_country_sources", True)):
            for source in cfg.sources:
                if source.ownership not in OWNERSHIP_GATE_ALLOW:
                    continue
                domain = self._registrable_domain(source.url)
                if not domain:
                    continue
                self.source_verdicts.setdefault(
                    domain,
                    {
                        "verdict": "country_editorial",
                        "region": source.region,
                        "source": source.name,
                    },
                )
        account_bindings = search_cfg.get("official_account_bindings", {}) if isinstance(search_cfg, dict) else {}
        self.official_account_bindings = {
            str(platform).casefold(): {
                str(account).casefold(): dict(spec)
                for account, spec in (accounts or {}).items()
                if isinstance(spec, dict)
            }
            for platform, accounts in (account_bindings or {}).items()
            if isinstance(accounts, dict)
        }
        self.source_regions = {source.name: source.region for source in cfg.sources}
        self._search_cache: dict[
            tuple[str, str, str, tuple[str, ...]], list[dict[str, Any]]
        ] = {}
        self._resolved_official_bindings: dict[
            tuple[str, str], dict[str, CandidateIdentity]
        ] = {}
        self._request_count = 0
        self._provider_failure_reason: str | None = None

    # ─── PUBLIC API ──────────────────────────────────────────────────────────

    def enhance_clusters(self, clusters: list[ArticleCluster]) -> list[ArticleCluster]:
        if not self.tavily_api_keys:
            logger.info("Active seeker disabled: no TAVILY_API_KEYS configured")
            return clusters
        # Reset per-run state: keys that auth-failed earlier may have been reset by the provider.
        self._exhausted_keys = set()
        self._search_cache.clear()
        self._resolved_official_bindings.clear()
        self._request_count = 0
        self._provider_failure_reason = None
        enriched = 0
        for cluster in clusters:
            if not self._should_enrich(cluster):
                continue
            targets = self._missing_voice_targets(cluster)
            if not targets:
                continue
            keyword = self._analyze_search_keyword(cluster, targets)
            try:
                if self._enrich_cluster(cluster, targets, keyword):
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

    # ─── TRIGGER / TARGETING ─────────────────────────────────────────────────

    def _should_enrich(self, cluster: ArticleCluster) -> bool:
        impact = cluster.impact
        if impact is None:
            return False
        if impact.status == "seek_more_evidence":
            return True
        # No longer gated on is_hot_topic — high-composite main-feed clusters
        # earn the same search budget as hot-topic ones.
        return bool(impact.composite >= self.hot_composite_trigger)

    def _missing_voice_targets(self, cluster: ArticleCluster) -> list[VoiceTarget]:
        """Return missing event entities; only use country as a fallback target.

        The entity list comes from impact evaluation of the news text, not a
        maintained catalogue. A country-only target is used only when the
        event has no eligible
        named actor but the impact evaluator identified an involved country.
        """
        impact = cluster.impact
        if impact is None:
            return []
        impact_targets = self._validated_actor_targets(
            cluster,
            (
                (
                    need.label,
                    need.country,
                    need.kind,
                    need.evidence_text or need.label,
                    need.event_role,
                    need.materiality,
                    need.why_voice_needed,
                )
                for need in impact.voice_needs
            ),
        )
        if impact_targets:
            covered = [
                replace(target, coverage_before=self._voice_coverage(cluster, target))
                for target in impact_targets
            ]
            return [
                target for target in covered if not self._coverage_satisfies(cluster, target)
            ][: self.max_regions_per_cluster]

        recovered_targets = self._recover_actor_targets(cluster)
        if recovered_targets:
            covered = [
                replace(target, coverage_before=self._voice_coverage(cluster, target))
                for target in recovered_targets
            ]
            return [
                target for target in covered if not self._coverage_satisfies(cluster, target)
            ][: self.max_regions_per_cluster]

        targets: list[VoiceTarget] = []
        for region in impact.subject_regions:
            if not is_recognized_country(region):
                continue
            target = VoiceTarget(
                region=region,
                label=country_name(region),
                role="country",
                cluster_key=impact.cluster_key,
            )
            if not self._has_country_voice(cluster, target):
                targets.append(
                    replace(target, coverage_before=self._voice_coverage(cluster, target))
                )
        return targets[: self.max_regions_per_cluster]

    def _validated_actor_targets(
        self,
        cluster: ArticleCluster,
        values: Any,
    ) -> list[VoiceTarget]:
        targets: list[VoiceTarget] = []
        seen: set[tuple[str, str]] = set()
        for value in values:
            parts = list(value) if not isinstance(value, dict) else [
                value.get("label"),
                value.get("country"),
                value.get("kind"),
                value.get("evidence_text"),
                value.get("event_role"),
                value.get("materiality"),
                value.get("why_voice_needed"),
            ]
            parts.extend([None] * (7 - len(parts)))
            target = self._validate_actor_target(cluster, *parts[:7])
            if target is None:
                continue
            key = (self._identity_text(target.label), target.region)
            if key not in seen:
                seen.add(key)
                targets.append(target)
        return targets

    def _validate_actor_target(
        self,
        cluster: ArticleCluster,
        label: Any,
        region: Any,
        role: Any,
        evidence_text: Any,
        event_role: Any = None,
        materiality: Any = None,
        why_voice_needed: Any = None,
    ) -> VoiceTarget | None:
        normalized_label = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(label or "")).strip())[:100]
        normalized_region = str(region or "").strip().lower()
        normalized_role = str(role or "organization").strip().lower()
        normalized_evidence = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(evidence_text or "")).strip())[:200]
        normalized_event_role = str(event_role or "principal").strip().lower()
        normalized_materiality = str(materiality or "required").strip().lower()
        normalized_reason = re.sub(
            r"\s+", " ", unicodedata.normalize("NFKC", str(why_voice_needed or "")).strip()
        )[:200]
        if not is_recognized_country(normalized_region) or not normalized_label:
            return None
        config = self.region_config.get(normalized_region)
        territory_languages = query_languages(
            normalized_region, config.language if config else None
        )
        if normalized_role == "country" or is_territory_name(normalized_label, territory_languages):
            return None
        if normalized_role not in _ACTOR_ROLES:
            normalized_role = "organization"
        if normalized_materiality != "required" or normalized_event_role not in _MATERIAL_EVENT_ROLES:
            return None
        if _NON_VOICE_LABEL_PATTERN.search(normalized_label):
            return None
        event_text = self._event_text(cluster)
        normalized_event_text = self._identity_text(event_text)
        label_present = self._identity_text(normalized_label) in normalized_event_text
        evidence_present = bool(
            normalized_evidence
            and self._identity_text(normalized_evidence) in normalized_event_text
        )
        if not label_present and not evidence_present:
            return None
        return VoiceTarget(
            region=normalized_region,
            label=normalized_label,
            role=normalized_role,
            event_role=normalized_event_role,
            evidence_text=normalized_evidence or normalized_label,
            materiality=normalized_materiality,
            why_voice_needed=normalized_reason,
            cluster_key=cluster.impact.cluster_key if cluster.impact is not None else "",
        )

    def _recover_actor_targets(self, cluster: ArticleCluster) -> list[VoiceTarget]:
        """Recover explicit event actors only when impact supplied none."""
        impact = cluster.impact
        if impact is None:
            return []
        evidence = "\n".join(
            f"- {article.title}\n  {article.content[:400]}" for article in cluster.articles[:5]
        )
        official_sources = [article.source_name for article in cluster.articles if article.is_official_source]
        prompt = (
            "Extract only named actors whose direct response would clarify this exact news event. "
            "A principal public official or decision-maker may use kind=person. "
            "Do not return countries, incidental people, products, facilities, locations, comparison-only entities, "
            "merely damaged businesses, media, or inferred formal names. "
            "Return JSON only: {\"targets\":[{\"label\":\"Microsoft\",\"country\":\"us\","
            "\"kind\":\"company\",\"event_role\":\"decision_maker\","
            "\"evidence_text\":\"Microsoft\",\"materiality\":\"required\","
            "\"why_voice_needed\":\"It made the decision being reported\"}]}.\n\n"
            f"Event material:\n{evidence}\n\nExisting official sources: {official_sources}\n"
            f"Related countries: {impact.subject_regions}"
        )
        try:
            response = litellm.completion(
                model=self.evaluator_model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=220,
                response_format={"type": "json_object"},
                **self.completion_compat_kwargs,
            )
            content = (response.choices[0].message.content or "").strip()
            parsed = json.loads(content[content.find("{"): content.rfind("}") + 1])
            return self._validated_actor_targets(
                cluster,
                (item for item in parsed.get("targets", []) if isinstance(item, dict)),
            )
        except Exception as exc:
            logger.debug("Actor recovery failed for '%s': %s", cluster.topic_category, exc)
            return []

    @staticmethod
    def _event_text(cluster: ArticleCluster) -> str:
        return "\n".join(f"{article.title}\n{article.content[:1200]}" for article in cluster.articles[:5])

    @staticmethod
    def _identity_text(value: str) -> str:
        return "".join(char for char in unicodedata.normalize("NFKC", value).casefold() if char.isalnum())

    def _has_country_voice(self, cluster: ArticleCluster, target: VoiceTarget) -> bool:
        return self._voice_coverage(cluster, target) == "verified_related_country_editorial"

    def _has_official_voice(self, cluster: ArticleCluster, target: VoiceTarget) -> bool:
        return any(
            article.is_official_source
            and not article.is_placeholder
            and self._article_mentions_target(article, target)
            for article in cluster.articles
        )

    def _voice_coverage(self, cluster: ArticleCluster, target: VoiceTarget) -> str:
        """Classify whether the requested voice is already represented."""
        if target.role != "country" and self._has_official_voice(cluster, target):
            return "official_direct"
        if target.role != "country" and any(
            self._has_attributed_direct_quote(article, target)
            for article in cluster.articles
            if not article.is_placeholder
        ):
            return "direct_quote"
        for article in cluster.articles:
            if article.is_placeholder:
                continue
            region = article.origin_region or self.source_regions.get(article.source_name)
            if region != target.region:
                continue
            if not article.is_searched and article.source_name in self.source_regions:
                return "verified_related_country_editorial"
            if (
                article.is_searched
                and article.search_acceptance_status == "accepted"
                and str(article.searched_provider or "").endswith("_country")
            ):
                return "verified_related_country_editorial"
        if target.role != "country" and any(
            self._article_mentions_target(article, target)
            for article in cluster.articles
            if not article.is_placeholder
        ):
            return "secondary_only"
        return "missing"

    def _coverage_satisfies(self, cluster: ArticleCluster, target: VoiceTarget) -> bool:
        coverage = self._voice_coverage(cluster, target)
        if target.role == "country":
            return coverage == "verified_related_country_editorial"
        return coverage in {
            "official_direct", "direct_quote", "verified_related_country_editorial"
        }

    @staticmethod
    def _has_attributed_direct_quote(article: Article, target: VoiceTarget) -> bool:
        text = f"{article.title}\n{article.content[:1800]}"
        label_index = text.casefold().find(target.label.casefold())
        if label_index < 0:
            return False
        nearby = text[max(0, label_index - 80): label_index + len(target.label) + 160]
        has_quote = bool(re.search(r"[\"'“”‘’][^\"'“”‘’]{4,}[\"'“”‘’]", nearby))
        has_attribution = bool(
            re.search(
                r"(?:said|stated|told|responded|announced|表示|回应|称|声明|指出)[：,:]?",
                nearby,
                re.IGNORECASE,
            )
        )
        return has_quote and has_attribution

    def _analyze_search_keyword(self, cluster: ArticleCluster, targets: list[VoiceTarget]) -> str:
        """Formulate an exact event query; the impact layer supplies the targets."""
        evidence = "\n".join(f"- {article.title}" for article in cluster.articles[:5])
        labels = ", ".join(target.label for target in targets)
        prompt = (
            "Write one concise English web-search query for this exact news event.\n"
            f"Event headlines:\n{evidence}\n\n"
            f"Missing voices: {labels}\n\n"
            "Return compact JSON only: {\"keyword\": \"<3-8 words for this exact event>\"}."
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
            keyword = str(parsed.get("keyword") or "").strip()[:160]
            return keyword or self._fallback_search_keyword(cluster)
        except Exception as exc:
            fallback = self._fallback_search_keyword(cluster)
            logger.info(
                "Search target analysis failed for '%s'; using deterministic headline fallback: %s",
                cluster.topic_category,
                exc,
            )
            return fallback

    @staticmethod
    def _fallback_search_keyword(cluster: ArticleCluster) -> str:
        for article in cluster.articles:
            title = re.sub(r"\s+", " ", str(article.title or "")).strip()
            if title:
                return title[:160]
        return re.sub(r"\s+", " ", str(cluster.topic_category or "")).strip()[:160]

    # ─── ENRICHMENT ──────────────────────────────────────────────────────────

    def _enrich_cluster(
        self, cluster: ArticleCluster, targets: list[VoiceTarget], keyword: str
    ) -> bool:
        centroid = self._cluster_centroid(cluster)
        added = False
        for target in targets:
            if not keyword:
                cluster.articles.append(self._placeholder_article(cluster, target, "query_generation_failed"))
                continue
            article, fail_reason, stage_trace = self._search_target(
                cluster, target, keyword, centroid
            )
            if article is not None:
                cluster.articles.append(article)
                if article.source_name not in cluster.sources:
                    cluster.sources.append(article.source_name)
                logger.info(
                    "Seeker added %s perspective to '%s': %s (%s)",
                    target.label,
                    cluster.topic_category,
                    article.title[:60],
                    article.source_name,
                )
                added = True
                continue
            if fail_reason == "coverage_satisfied":
                logger.info(
                    "Seeker stopped for %s on '%s': qualifying coverage already exists",
                    target.label,
                    cluster.topic_category,
                )
                continue
            # Synthesize a flat inline placeholder so the reader sees that this
            # region's perspective was targeted but unavailable — never silent.
            placeholder = self._placeholder_article(
                cluster,
                target,
                fail_reason or "unknown",
                stage_trace=stage_trace,
            )
            cluster.articles.append(placeholder)
            # Do NOT add the placeholder source_name to cluster.sources — it must
            # not count toward is_multi_source or appear in perspective grouping.
            logger.info(
                "Seeker placeholder for %s on '%s': reason=%s",
                target.label,
                cluster.topic_category,
                fail_reason,
            )
        return added

    def _search_target(
        self,
        cluster: ArticleCluster,
        target: VoiceTarget,
        keyword: str,
        centroid: np.ndarray | None,
    ) -> tuple[Article | None, str | None, list[dict[str, str]]]:
        """Return (accepted_article, failure_reason).

        failure_reason is set only when no article was accepted, so the caller
        can synthesize an inline placeholder that surfaces the cause to readers.
        """
        stages = ("country",) if target.role == "country" else ("official", "country")
        last_reason: str | None = None
        stage_trace: list[dict[str, str]] = []
        for stage in stages:
            include_domains: list[str] = []
            stage_queries = self._build_search_queries(cluster, target, keyword, stage)[
                : self.max_queries_per_stage
            ]
            if stage == "official":
                include_domains, resolve_reason = self._resolve_official_domains(target)
                if resolve_reason in {
                    "http_401", "http_402", "http_403", "http_429", "network",
                    "request_budget_exhausted",
                }:
                    stage_trace.append({"stage": stage, "reason": resolve_reason})
                    return None, resolve_reason, stage_trace
                if not include_domains and not self._reviewed_social_searches(target, keyword):
                    last_reason = resolve_reason or "official_not_found"
                    stage_trace.append({"stage": stage, "reason": last_reason})
                    continue
            elif stage == "country":
                include_domains = self._reviewed_country_domains(target.region)

            searches: list[tuple[str, list[str], str]] = []
            if stage != "official" or include_domains:
                searches.extend(
                    (query, include_domains, f"search_{stage}")
                    for query in stage_queries
                )
            if stage == "official":
                searches.extend(
                    (query, domains, "search_official")
                    for query, domains in self._reviewed_social_searches(target, keyword)
                )
            if stage == "country":
                # A broad result set is discovery-only. Unknown publishers stay
                # pending; only reviewed country bindings can be published.
                discovery_query = stage_queries[0] if stage_queries else keyword
                if include_domains:
                    searches.append((discovery_query, [], "search_country"))
                else:
                    searches = [(discovery_query, [], "search_country")]
                # Entity-free fallback: an entity may have no fresh coverage
                # (no result, entity_mismatch, or only dated-stale results).
                # Retry the event + country without the entity constraint so
                # at least one voice from that country appears. Reviewed
                # country-editorial domains only; one extra bounded query.
                if (
                    self.country_entity_free_fallback
                    and target.role != "country"
                    and include_domains
                    and stage_queries
                ):
                    entity_free = self._build_search_queries(
                        cluster, target, keyword, stage, entity_scoped=False
                    )[:1]
                    if entity_free:
                        searches.append(
                            (entity_free[0], include_domains, "search_country_fallback")
                        )

            for query, restricted_domains, request_type in searches:
                results, search_fail = self._search_tavily(
                    target,
                    query,
                    include_domains=restricted_domains,
                    request_type=request_type,
                )
                if search_fail:
                    # Provider-level failure means no reliable conclusion about
                    # this missing voice; do not disguise it as no result.
                    stage_trace.append({"stage": stage, "reason": search_fail})
                    return None, search_fail, stage_trace
                accepted, rejections = self._accept_results(
                    cluster, target, results, centroid, stage
                )
                self._record_search_event(
                    provider="tavily_search",
                    request_type=f"acceptance_{stage}",
                    target=target,
                    query=query,
                    restricted_domains=restricted_domains,
                    result_count=len(results),
                    accepted_count=len(accepted),
                    rejection_reason=",".join(sorted({reason for reason, _ in rejections})) or None,
                    rejection_count=len(rejections) or None,
                )
                if accepted:
                    stage_trace.append({"stage": stage, "reason": "accepted"})
                    accepted[0].search_stage_trace = list(stage_trace)
                    return accepted[0], None, stage_trace
                if rejections:
                    reasons = {reason for reason, _ in rejections}
                    if "coverage_satisfied" in reasons:
                        stage_trace.append({"stage": stage, "reason": "coverage_satisfied"})
                        return None, "coverage_satisfied", stage_trace
                    last_reason = self._primary_failure_reason(reasons)
                elif not results:
                    last_reason = "official_not_found" if stage == "official" else "country_fallback_not_found"
                else:
                    last_reason = "candidate_unverified"
            stage_trace.append({"stage": stage, "reason": last_reason or "unknown"})
        return None, last_reason, stage_trace

    @staticmethod
    def _primary_failure_reason(reasons: set[str]) -> str:
        priority = (
            "publisher_binding_unverified",
            "candidate_pending_review",
            "not_related_country_source",
            "publisher_target_mismatch",
            "not_official_source",
            "event_mismatch",
            "background_context",
            "entity_mismatch",
            "invalid_result_url",
            "stale_result",
            "duplicate_of_existing",
            "thin_result",
        )
        return next((reason for reason in priority if reason in reasons), sorted(reasons)[0])

    def _placeholder_article(
        self,
        cluster: ArticleCluster,
        target: VoiceTarget,
        reason: str,
        stage_trace: list[dict[str, str]] | None = None,
    ) -> Article:
        """Synthesize an inline placeholder Article for a missing perspective.

        The placeholder is rendered flat in the source list with the country
        flag + a short failure label + tooltip detail. It never counts toward
        cluster.is_multi_source (caller does not append its source_name).
        """
        target_name = target.label or country_name(target.region)
        cluster_key = getattr(cluster, "cluster_key", "") or getattr(cluster, "topic_category", "")
        article = Article(
            id=None,
            url=f"placeholder:{target.region}:{cluster_key}",
            title=f"待补充：{target_name}声音",
            source_name=f"[{target_name}声音待补]",
            published_at=datetime.now(tz=timezone.utc),
            content="",
            is_searched=True,
            search_region=target.region,
            source_kind="news",
            origin_region=target.region,
            searched_provider="tavily_search",
        )
        article.is_placeholder = True
        article.search_acceptance_status = "failed"
        article.search_acceptance_reason = reason
        article.search_stage_trace = list(stage_trace or [])
        return article

    def _accept_results(
        self,
        cluster: ArticleCluster,
        target: VoiceTarget,
        results: list[dict[str, Any]],
        centroid: np.ndarray | None,
        stage: str,
    ) -> tuple[list[Article], list[tuple[str, str]]]:
        accepted: list[Article] = []
        rejections: list[tuple[str, str]] = []
        existing_by_url = {article.url: article for article in cluster.articles}
        existing_titles = [article.title for article in cluster.articles]
        for result in results:
            article = self._result_to_article(
                result,
                target.region,
                allow_thin=stage == "official",
            )
            if article is None:
                reason = (
                    "invalid_result_url"
                    if not self._canonical_result_url(str(result.get("url") or ""))
                    else "thin_result"
                )
                self._record_raw_candidate_rejection(result, target, stage, reason)
                rejections.append((reason, str(result.get("provider_url") or result.get("url"))))
                continue
            if article.url in existing_by_url:
                existing = existing_by_url[article.url]
                if self._existing_article_satisfies_target(existing, target, stage):
                    reason = "coverage_satisfied"
                else:
                    reason = "duplicate_of_existing"
                self._record_candidate_review(
                    article, target, stage, CandidateIdentity(), "rejected", reason
                )
                rejections.append((reason, article.url))
                continue
            reason = self._rejection_reason(
                article,
                target,
                existing_titles,
                centroid,
                stage=stage,
            )
            if reason:
                self._record_candidate_review(
                    article, target, stage, CandidateIdentity(), "rejected", reason
                )
                rejections.append((reason, article.url))
                continue
            dynamic_identity = self._dynamic_official_identity(article.url, target)
            identity = dynamic_identity or self._candidate_identity(
                self._verify_candidate(article, target, stage)
            )
            registry_identity, registry_reason = self._registry_identity(article.url, target, stage)
            if registry_reason:
                decision = (
                    "pending_review"
                    if registry_reason == "publisher_binding_unverified"
                    else "rejected"
                )
                self._record_candidate_review(
                    article, target, stage, identity, decision, registry_reason
                )
                rejections.append((registry_reason, article.url))
                continue
            if registry_identity is not None:
                identity = registry_identity
            elif stage == "official" and self._social_account_binding(article.url, target):
                identity = CandidateIdentity(
                    source_type="official_social",
                    publisher_entity=target.label,
                    relationship="same_entity",
                    ownership_evidence="reviewed exact social account binding",
                )
            if stage == "official" and target.role == "country":
                self._record_candidate_review(
                    article, target, stage, identity, "rejected", "country_target_official_forbidden"
                )
                rejections.append(("country_target_official_forbidden", article.url))
                continue
            if stage == "official":
                official_reason = self._official_identity_reason(
                    article,
                    target,
                    identity,
                    registry_identity or dynamic_identity,
                )
                if official_reason:
                    decision = (
                        "pending_review"
                        if official_reason == "publisher_binding_unverified"
                        else "rejected"
                    )
                    self._record_candidate_review(
                        article, target, stage, identity, decision, official_reason
                    )
                    rejections.append((official_reason, article.url))
                    continue
            elif identity.source_type != "country_editorial":
                self._record_candidate_review(
                    article, target, stage, identity, "rejected", "not_related_country_source"
                )
                rejections.append(("not_related_country_source", article.url))
                continue
            if (
                stage == "country"
                and identity.publisher_region
                and identity.publisher_region != target.region
            ):
                self._record_candidate_review(
                    article, target, stage, identity, "rejected", "not_related_country_source"
                )
                rejections.append(("not_related_country_source", article.url))
                continue
            if stage == "country" and registry_identity is None:
                # Discovery remains open, but a newly found local publisher is
                # not promoted to a country's voice until reviewed. This avoids
                # recreating a static search whitelist or trusting sponsored
                # outlets based only on a model's first impression.
                self._record_candidate_review(
                    article, target, stage, identity, "pending_review", "candidate_pending_review"
                )
                rejections.append(("candidate_pending_review", article.url))
                continue
            article.origin_region = target.region
            article.is_official_source = identity.source_type.startswith("official_")
            article.source_kind = identity.source_type if article.is_official_source else "news"
            article.searched_provider = f"tavily_search_{stage}"
            article.search_acceptance_status = "accepted"
            # Keep the existing source_kind role (official_web, official_social,
            # or news) and use the existing acceptance-reason field to retain
            # the evidence role without widening the Article/DB contract.
            article.search_acceptance_reason = _CURRENT_EVENT_ACCEPTANCE_REASON
            article.search_evidence_role = self._search_evidence_role(article, centroid)
            self._record_candidate_review(
                article,
                target,
                stage,
                identity,
                "accepted",
                _CURRENT_EVENT_ACCEPTANCE_REASON,
            )
            accepted.append(article)
            if len(accepted) >= self.max_results_per_region:
                break
        return accepted, rejections

    def _search_evidence_role(
        self,
        article: Article,
        centroid: np.ndarray | None,
    ) -> str:
        """Classify accepted search material as direct evidence or context.

        The admission threshold intentionally remains broad enough to surface
        a missing country's relevant reporting.  The stricter role threshold
        prevents a merely same-conflict result from being presented as a
        distinct current-event perspective.
        """
        if centroid is None:
            return "direct_event"
        embedding = get_model().encode(
            [f"{article.title} {article.content[:400]}"],
            normalize_embeddings=True,
            show_progress_bar=False,
        )[0]
        similarity = float(np.dot(embedding, centroid))
        if similarity >= self.min_direct_semantic_event_match:
            return "direct_event"
        logger.info(
            "Search candidate retained as background context: source=%s similarity=%.3f direct_floor=%.3f",
            article.source_name,
            similarity,
            self.min_direct_semantic_event_match,
        )
        return "background_context"

    def _existing_article_satisfies_target(
        self,
        article: Article,
        target: VoiceTarget,
        stage: str,
    ) -> bool:
        if article.is_placeholder:
            return False
        if stage == "official":
            return bool(
                article.is_official_source and self._article_mentions_target(article, target)
            )
        region = article.origin_region or self.source_regions.get(article.source_name)
        return bool(
            region == target.region
            and (
                (not article.is_searched and article.source_name in self.source_regions)
                or (
                    article.is_searched
                    and article.search_acceptance_status == "accepted"
                    and str(article.searched_provider or "").endswith("_country")
                )
            )
        )

    def _rejection_reason(
        self,
        article: Article,
        target: VoiceTarget,
        existing_titles: list[str],
        centroid: np.ndarray | None,
        stage: str | None = None,
    ) -> str:
        dynamic_identity = self._dynamic_official_identity(article.url, target)
        if (
            target.role != "country"
            and (stage != "country" or centroid is None)
            and not self._article_mentions_target(article, target)
            and not (stage == "official" and dynamic_identity is not None)
        ):
            return "entity_mismatch"
        if not self._is_fresh(
            article.published_at,
            allow_undated=bool(stage == "country" and self.country_allow_undated),
        ):
            return "stale_result"
        if stage == "official" and self._looks_like_official_background(article):
            return "background_context"
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
            # Ownership establishes who published a page, not whether it is
            # about this event. Keep the event-materiality threshold identical
            # for official and country candidates.
            if float(np.dot(embedding, centroid)) < self.min_semantic_event_match:
                return "event_mismatch"
        return ""

    @staticmethod
    def _looks_like_official_background(article: Article) -> bool:
        """Reject first-party background material as a current perspective.

        Official ownership is useful provenance, but annual reports, investor
        material, galleries, and generic institutional pages are not direct
        responses to the event being enriched. Date freshness is checked first
        so an old page still reports ``stale_result`` rather than hiding the
        more actionable date failure.
        """
        material = f"{article.title}\n{article.url}"
        return bool(_OFFICIAL_BACKGROUND_PATTERN.search(material))

    def _article_mentions_target(self, article: Article, target: VoiceTarget) -> bool:
        text = self._identity_text(f"{article.title}\n{article.content[:1200]}")
        aliases = [target.label]
        evidence = str(target.evidence_text or "").strip()
        if evidence and len(evidence) <= 120:
            aliases.append(evidence)
        for spec in self.source_verdicts.values():
            names = [str(spec.get("entity") or ""), *(spec.get("entity_aliases") or [])]
            normalized_names = {self._identity_text(name) for name in names if str(name).strip()}
            if self._identity_text(target.label) in normalized_names:
                aliases.extend(names)
        return any(
            (normalized := self._identity_text(alias)) and normalized in text
            for alias in aliases
        )

    def _verify_candidate(self, article: Article, target: VoiceTarget, stage: str) -> CandidateIdentity:
        """Fail closed unless the candidate is clearly official or local editorial.

        A country TLD, provider country label, or a page claiming an affiliation
        is explicitly insufficient. This is intentionally conservative because
        sponsored/shadow outlets cannot be safely identified from a domain list.
        """
        host = urllib.parse.urlparse(article.url).netloc.lower().removeprefix("www.")
        required = "official_web or official_social" if stage == "official" else "country_editorial"
        prompt = (
            "Verify one news-search candidate for a missing event voice. Return JSON only. "
            "A result is official_web only when the page is directly published by the named actor; "
            "official_social only when it is that actor's clearly verified/owned social account. "
            "country_editorial only when it is an independently operated editorial publisher based in "
            "the target country; reject aggregators, mirrors, generic social platforms, state/sponsored "
            "outlets with unclear independence, and any affiliation that cannot be confirmed from the "
            "candidate. Do not infer from country TLD or search query. If uncertain, reject.\n\n"
            f"Required stage: {stage}; accepted verdicts: {required}.\n"
            f"Named actor: {target.label}; actor country: {target.region}; actor type: {target.role}.\n"
            f"Candidate host: {host}\nTitle: {article.title}\nContent: {article.content[:1800]}\n\n"
            "Return {\"source_type\":\"official_web|official_social|country_editorial|reject\","
            "\"publisher_entity\":\"publisher name\","
            "\"publisher_region\":\"lowercase ISO alpha-2 country where the publisher is based, or empty\","
            "\"relationship\":\"same_entity|quoted_by_third_party|covered_by_third_party|uncertain|unrelated\","
            "\"ownership_evidence\":\"short evidence\",\"confidence\":0.0}."
        )
        try:
            response = litellm.completion(
                model=self.evaluator_model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=160,
                response_format={"type": "json_object"},
                **self.completion_compat_kwargs,
            )
            content = (response.choices[0].message.content or "").strip()
            return self._candidate_identity(json.loads(content[content.find("{"): content.rfind("}") + 1]))
        except Exception as exc:
            logger.debug("Search candidate verification failed for %s: %s", article.url, exc)
            return CandidateIdentity()

    def _candidate_identity(self, value: CandidateIdentity | dict[str, Any] | str) -> CandidateIdentity:
        """Normalize verifier output; string support keeps older test doubles harmless."""
        if isinstance(value, CandidateIdentity):
            return value
        if isinstance(value, str):
            return CandidateIdentity(source_type=value if value in _SOURCE_TYPES else "reject")
        source_type = str(value.get("source_type") or value.get("verdict") or "reject")
        relationship = str(value.get("relationship") or "uncertain")
        confidence = value.get("confidence")
        try:
            confidence = float(confidence) if confidence is not None else None
        except (TypeError, ValueError):
            confidence = None
        return CandidateIdentity(
            source_type=source_type if source_type in _SOURCE_TYPES else "reject",
            publisher_entity=str(value.get("publisher_entity") or "").strip()[:120],
            publisher_region=str(value.get("publisher_region") or "").strip().lower()[:2],
            relationship=relationship if relationship in _RELATIONSHIPS else "uncertain",
            ownership_evidence=str(value.get("ownership_evidence") or "").strip()[:300],
            confidence=confidence,
        )

    def _registry_identity(
        self, url: str, target: VoiceTarget, stage: str
    ) -> tuple[CandidateIdentity | None, str | None]:
        """Read reviewed bindings; discovery candidates never become a binding."""
        host = urllib.parse.urlparse(url).netloc.lower().removeprefix("www.")
        for domain, spec in self.source_verdicts.items():
            if host != domain and not host.endswith(f".{domain}"):
                continue
            verdict = str(spec.get("verdict") or "").strip()
            approved_region = str(spec.get("region") or "").strip().lower()
            approved_entity = str(spec.get("entity") or "").strip()
            # entity_aliases lets one reviewed domain binding match the target
            # under any accepted name form (translation, acronym, full name)
            # instead of requiring a single exact normalized string.
            alias_names = [approved_entity, *(spec.get("entity_aliases") or [])]
            alias_texts = {self._identity_text(str(alias)) for alias in alias_names if str(alias).strip()}
            if approved_region and approved_region != target.region:
                return None, "not_related_country_source"
            if stage == "official":
                # Social ownership is account-scoped; a host/domain binding is
                # never enough to establish who published a social post.
                if verdict == "official_social":
                    return None, "publisher_binding_unverified"
                if verdict != "official_web":
                    return None, "not_official_source"
                if not alias_texts or self._identity_text(target.label) not in alias_texts:
                    if self._is_governmental_domain(url):
                        # A reviewed, clearly-governmental domain (.gov/.mil/…)
                        # whose alias list simply doesn't happen to cover this
                        # particular target-label phrasing should not hard-
                        # block the candidate — fall through (None, None) so
                        # _official_identity_reason's verifier-based judgment
                        # gets to decide, instead of a closed alias list
                        # gatekeeping every possible label form for a domain
                        # we already know is legitimate. Non-governmental
                        # registry domains keep the strict alias requirement.
                        return None, None
                    return None, "publisher_binding_unverified"
                return CandidateIdentity(
                    source_type=verdict,
                    publisher_entity=approved_entity,
                    publisher_region=target.region,
                    relationship="same_entity",
                    ownership_evidence="reviewed exact entity binding",
                ), None
            if verdict != "country_editorial" or not approved_region:
                return None, "not_related_country_source"
            return CandidateIdentity(
                source_type="country_editorial",
                publisher_region=approved_region,
                ownership_evidence="reviewed country editorial binding",
            ), None
        return None, None

    def _official_identity_reason(
        self,
        article: Article,
        target: VoiceTarget,
        identity: CandidateIdentity,
        registry_identity: CandidateIdentity | None,
    ) -> str | None:
        if identity.source_type == "official_social" and self._social_account_binding(article.url, target):
            return None
        if identity.source_type not in {"official_web", "official_social"}:
            return "not_official_source"
        if registry_identity is not None:
            return None
        # The verifier's own relationship judgment is the primary signal for
        # "is this actually the target entity". A verdict other than
        # same_entity (uncertain/unrelated/quoted_by_third_party/...) is
        # always rejected here — this is the protection commit 0ffca0f added
        # and it stays intact regardless of confidence or domain.
        if identity.relationship != "same_entity":
            return "publisher_target_mismatch"
        # A publisher_region the verifier did populate must agree with the
        # target's region — same_entity plus an explicit country conflict is
        # still a mismatch (a foreign ministry sharing a translated label is
        # not the target). Absent/empty publisher_region is common (the
        # CENTCOM production JSON omitted it) and is not itself a conflict.
        if identity.publisher_region and identity.publisher_region != target.region:
            return "publisher_target_mismatch"
        # High-confidence same_entity official_web on a domain that is itself
        # clearly governmental (.gov/.mil/ccTLD gov analog) is trusted without
        # also demanding target.label and publisher_entity normalize to
        # identical text. That text-equality check cannot survive translation
        # (target label in Chinese, publisher_entity in English) or
        # acronym/full-name variance (CENTCOM vs "U.S. Central Command
        # (CENTCOM)") even when the verifier already correctly identified the
        # same entity with full confidence — re-litigating a semantic
        # judgment the LLM already made with a brittle string comparison was
        # the bug. A domain that is NOT clearly governmental (e.g.
        # microsoft.ai) still falls through to the strict checks below, so an
        # unrelated org the verifier mislabels same_entity is still caught.
        if (
            identity.source_type == "official_web"
            and identity.confidence is not None
            and identity.confidence >= self.official_identity_confidence_floor
            and self._is_governmental_domain(article.url)
        ):
            return None
        if self._identity_text(identity.publisher_entity) != self._identity_text(target.label):
            return "publisher_target_mismatch"
        if identity.source_type == "official_social":
            return None if self._social_account_binding(article.url, target) else "publisher_binding_unverified"
        # The registrable domain label of a real government site (state.gov,
        # treasury.gov, whitehouse.gov, centcom.mil) essentially never equals
        # the entity's full name, so this equality check would false-reject
        # governmental domains even on an exact publisher_entity match (e.g.
        # confidence omitted by the verifier so the fast path above didn't
        # fire). Skip it there; the domain's own suffix already vouches for
        # government provenance.
        if not self._is_governmental_domain(article.url):
            registered = _TLD_EXTRACT(urllib.parse.urlparse(article.url).netloc)
            if self._identity_text(registered.domain) != self._identity_text(target.label):
                return "publisher_binding_unverified"
        return None

    @staticmethod
    def _is_governmental_domain(url: str) -> bool:
        """True when the URL's registered suffix marks it as governmental.

        Matches a bare gov/mil TLD (centcom.mil) as well as ccTLD second-level
        government suffixes (gov.uk, gov.cn, gouv.fr) by checking every
        dot-separated component of the extracted public suffix.
        """
        suffix = _TLD_EXTRACT(urllib.parse.urlparse(url).netloc).suffix.lower()
        return any(part in _GOVERNMENTAL_SUFFIX_MARKERS for part in suffix.split("."))

    def _social_account_binding(self, url: str, target: VoiceTarget) -> bool:
        social = self._social_account(url)
        if social is None:
            return False
        platform, account_id = social
        spec = self.official_account_bindings.get(platform, {}).get(account_id.casefold())
        return bool(
            spec
            and self._identity_text(str(spec.get("entity") or "")) == self._identity_text(target.label)
            and str(spec.get("region") or "").strip().lower() == target.region
        )

    @staticmethod
    def _social_account(url: str) -> tuple[str, str] | None:
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc.casefold().removeprefix("www.")
        parts = [part for part in parsed.path.split("/") if part]
        if host == "x.com" and parts and re.fullmatch(r"[A-Za-z0-9_]{1,15}", parts[0]):
            return "x", parts[0]
        if host == "youtube.com" and parts:
            if parts[0].startswith("@") and len(parts[0]) > 1:
                return "youtube", parts[0][1:]
            if parts[0] == "channel" and len(parts) == 2 and parts[1]:
                return "youtube", parts[1]
        return None

    def _record_candidate_review(
        self,
        article: Article,
        target: VoiceTarget,
        stage: str,
        identity: CandidateIdentity,
        decision: str,
        reason: str,
    ) -> None:
        if not self.telemetry_enabled:
            return
        try:
            host = urllib.parse.urlparse(article.url).netloc.lower().removeprefix("www.")
            insert_search_candidate_review(
                SearchCandidateReview(
                    url=article.url,
                    domain=host,
                    title=article.title,
                    source_name=article.source_name,
                    target_label=target.label,
                    target_region=target.region,
                    target_role=target.role,
                    stage=stage,
                    verdict=identity.source_type,
                    decision=decision,
                    reason=reason,
                    identity_evidence=identity.as_dict(),
                    published_at=article.published_at,
                ),
                db_path=self.telemetry_db_path,
            )
        except Exception as exc:
            logger.debug("Search candidate telemetry write failed: %s", exc)

    def _record_raw_candidate_rejection(
        self,
        result: dict[str, Any],
        target: VoiceTarget,
        stage: str,
        reason: str,
    ) -> None:
        if not self.telemetry_enabled:
            return
        raw_url = str(result.get("provider_url") or result.get("url") or "").strip()
        if not raw_url:
            return
        published_at = self._parse_published_at(
            result.get("published_at") or result.get("published_date") or result.get("date")
        )
        if published_at is None:
            published_at = self._parse_url_date(raw_url)
        article = Article(
            url=raw_url,
            title=str(result.get("title") or "").strip() or "Untitled search result",
            source_name=str(result.get("source_name") or "search-result"),
            published_at=published_at,
            content=str(result.get("content") or ""),
            is_searched=True,
            search_region=target.region,
        )
        self._record_candidate_review(
            article,
            target,
            stage,
            CandidateIdentity(),
            "rejected",
            reason,
        )

    def _is_fresh(self, published_at: datetime | None, allow_undated: bool = False) -> bool:
        if published_at is None:
            # A provider query bound is not evidence about an individual page:
            # official sites frequently return undated annual reports, PDFs,
            # and evergreen background pages. Accepted supplements must carry
            # a provider date or a date recovered from the URL path. In the
            # country stage, an undated result is not proof of staleness
            # (Tavily itself is time-bounded to `days=3`), so the caller may
            # allow it through; dated-stale results are still rejected there.
            return allow_undated
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

    def _resolve_official_domains(
        self,
        target: VoiceTarget,
    ) -> tuple[list[str], str | None]:
        """Resolve entity-bound web domains before searching for event content."""
        key = (self._identity_text(target.label), target.region)
        cached = self._resolved_official_bindings.get(key)
        if cached is not None:
            return sorted(cached), None if cached else "official_binding_not_found"

        bindings: dict[str, CandidateIdentity] = {}
        for domain, spec in self.source_verdicts.items():
            if str(spec.get("verdict") or "") != "official_web":
                continue
            alias_names = [str(spec.get("entity") or ""), *(spec.get("entity_aliases") or [])]
            if key[0] not in {
                self._identity_text(str(alias)) for alias in alias_names if str(alias).strip()
            }:
                continue
            region = str(spec.get("region") or "").strip().lower()
            if region and region != target.region:
                continue
            bindings[domain] = CandidateIdentity(
                source_type="official_web",
                publisher_entity=target.label,
                relationship="same_entity",
                ownership_evidence="reviewed exact entity binding",
            )
        if bindings:
            self._resolved_official_bindings[key] = bindings
            return sorted(bindings), None

        query = f"{target.label} official website"
        results, failure = self._search_tavily(
            target,
            query,
            request_type="identity_resolution",
        )
        if failure:
            return [], failure

        rejection_reasons: set[str] = set()
        for result in results:
            article = self._result_to_article(result, target.region, allow_thin=True)
            if article is None:
                rejection_reasons.add("thin_result")
                continue
            identity = self._candidate_identity(
                self._verify_candidate(article, target, "official")
            )
            registry_identity, registry_reason = self._registry_identity(
                article.url, target, "official"
            )
            if registry_reason:
                rejection_reasons.add(registry_reason)
                self._record_candidate_review(
                    article,
                    target,
                    "identity",
                    identity,
                    "pending_review" if registry_reason == "publisher_binding_unverified" else "rejected",
                    registry_reason,
                )
                continue
            if registry_identity is not None:
                identity = registry_identity
            if identity.source_type == "official_social":
                # Social bindings are account-scoped and handled by
                # _reviewed_social_searches; never cache the whole host.
                reason = (
                    None if self._social_account_binding(article.url, target)
                    else "publisher_binding_unverified"
                )
            else:
                reason = self._official_identity_reason(
                    article, target, identity, registry_identity
                )
            if reason:
                rejection_reasons.add(reason)
                self._record_candidate_review(
                    article,
                    target,
                    "identity",
                    identity,
                    "pending_review" if reason == "publisher_binding_unverified" else "rejected",
                    reason,
                )
                continue
            if identity.source_type != "official_web":
                continue
            domain = self._registrable_domain(article.url)
            if not domain:
                rejection_reasons.add("publisher_binding_unverified")
                continue
            bindings[domain] = identity
            self._record_candidate_review(
                article,
                target,
                "identity",
                identity,
                "accepted",
                "identity_binding_resolved",
            )

        self._resolved_official_bindings[key] = bindings
        if bindings:
            return sorted(bindings), None
        if rejection_reasons:
            return [], self._primary_failure_reason(rejection_reasons)
        return [], "official_binding_not_found"

    def _reviewed_country_domains(self, region: str) -> list[str]:
        return sorted(
            domain
            for domain, spec in self.source_verdicts.items()
            if str(spec.get("verdict") or "") == "country_editorial"
            and str(spec.get("region") or "").strip().lower() == region
        )

    def _reviewed_social_searches(
        self,
        target: VoiceTarget,
        keyword: str,
    ) -> list[tuple[str, list[str]]]:
        searches: list[tuple[str, list[str]]] = []
        for platform, accounts in self.official_account_bindings.items():
            for account, spec in accounts.items():
                if self._identity_text(str(spec.get("entity") or "")) != self._identity_text(target.label):
                    continue
                if str(spec.get("region") or "").strip().lower() != target.region:
                    continue
                if platform == "x":
                    searches.append((f"site:x.com/{account} {keyword}", ["x.com"]))
                elif platform == "youtube":
                    path = f"@{account}" if not account.startswith("uc") else f"channel/{account}"
                    searches.append((f"site:youtube.com/{path} {keyword}", ["youtube.com"]))
        return searches

    @staticmethod
    def _registrable_domain(url: str) -> str:
        host = urllib.parse.urlparse(url).netloc.lower().removeprefix("www.")
        extracted = _TLD_EXTRACT(host)
        return str(getattr(extracted, "top_domain_under_public_suffix", "") or "")

    def _dynamic_official_identity(
        self,
        url: str,
        target: VoiceTarget,
    ) -> CandidateIdentity | None:
        key = (self._identity_text(target.label), target.region)
        domain = self._registrable_domain(url)
        if not domain:
            return None
        return self._resolved_official_bindings.get(key, {}).get(domain)

    def _build_search_queries(
        self,
        cluster: ArticleCluster,
        target: VoiceTarget,
        keyword: str,
        stage: str,
        entity_scoped: bool = True,
    ) -> list[str]:
        config = self.region_config.get(target.region)
        if stage == "official":
            english_query = f"{target.label} official statement {keyword}"
        else:
            # Country stage: keep the entity in the query when scoped so the
            # result talks about the same actor; drop it for the entity-free
            # fallback so the country's editorial voice can cover the event
            # even when the entity itself has no fresh coverage.
            target_prefix = (
                f"{target.label} " if (target.role != "country" and entity_scoped) else ""
            )
            english_query = f"{target_prefix}{keyword} {country_name(target.region)} local news"
        languages = query_languages(target.region, config.language if config else None)
        if not languages:
            # ISO country names are complete via Babel, while the language list
            # is intentionally conservative. For an uncovered country, ask the
            # evaluator to choose its mainstream local-news language instead of
            # silently treating English as authoritative.
            localized = self._localize_search_keyword(cluster, target.region, english_query, None)
            return list(dict.fromkeys([localized, english_query]))
        queries: list[str] = []
        for language in languages[: self.max_localized_query_variants]:
            if language == "en":
                continue
            queries.append(self._localize_search_keyword(cluster, target.region, english_query, language))
        queries.append(english_query)
        return list(dict.fromkeys(query for query in queries if query))

    def _localize_search_keyword(
        self, cluster: ArticleCluster, region: str, keyword: str, language: str | None
    ) -> str:
        if language == "en":
            return keyword
        target_language = language_name(language) if language else "the predominant language used by mainstream local news outlets"
        region_name = country_name(region)
        context = "\n".join(f"- {article.title}" for article in cluster.articles[:5])
        prompt = (
            f"Convert this English news search query into concise natural {target_language} used by local media in "
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

    def _search_tavily(
        self,
        target: VoiceTarget | str,
        query: str,
        include_domains: list[str] | None = None,
        request_type: str = "search",
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Return (results, failure_reason).

        failure_reason is one of: http_401, http_403, http_<other>, network, None.
        On 401/403, the active key is marked exhausted and the next configured
        key is tried within the same call. Only when ALL keys are exhausted (or
        a non-auth error occurs) is failure_reason returned.
        """
        target_obj = (
            target
            if isinstance(target, VoiceTarget)
            else VoiceTarget(region=str(target), label=country_name(str(target)), role="country")
        )
        region = target_obj.region
        normalized_domains = tuple(sorted(set(include_domains or [])))
        cache_key = (
            region,
            self._identity_text(target_obj.label),
            query,
            normalized_domains,
        )
        if cache_key in self._search_cache:
            return self._search_cache[cache_key], None
        if self._provider_failure_reason:
            return [], self._provider_failure_reason
        if self._request_count >= self.max_requests_per_run:
            return [], "request_budget_exhausted"

        base_payload: dict[str, Any] = {
            "query": query,
            "search_depth": "basic",
            "include_raw_content": True,
            "max_results": max(self.max_results_per_region + 2, 4),
            "days": 3,
        }
        if normalized_domains:
            base_payload["include_domains"] = list(normalized_domains)
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
                request_type=request_type,
                target=target_obj,
                query=query,
                restricted_domains=list(normalized_domains),
                http_status=401,
                result_count=0,
            )
            return [], "http_401"

        last_failure_reason: str | None = None
        last_status: int | None = None
        self._request_count += 1
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
                            request_type=request_type,
                            target=target_obj,
                            query=query,
                            restricted_domains=list(normalized_domains),
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
                        request_type=request_type,
                        target=target_obj,
                        query=query,
                        restricted_domains=list(normalized_domains),
                        http_status=status,
                        result_count=0,
                    )
                    continue
                # Non-auth HTTP error or network error — not a key problem, so
                # don't rotate; just report the failure for this query.
                self._record_search_event(
                    provider="tavily_search",
                    request_type=request_type,
                    target=target_obj,
                    query=query,
                    restricted_domains=list(normalized_domains),
                    http_status=status,
                    result_count=0,
                )
                logger.warning("Tavily search failed: %s", exc)
                failure_reason = f"http_{status}" if status else "network"
                if failure_reason == "network" or (status is not None and status >= 500):
                    self._provider_failure_reason = failure_reason
                return [], failure_reason
            else:
                # Success — pin this key as the active one for subsequent calls.
                self._active_key_idx = key_idx
                results: list[dict[str, Any]] = []
                for result in data.get("results", []):
                    provider_url = str(result.get("url") or "")
                    url = self._canonical_result_url(provider_url)
                    source_domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                    results.append(
                        {
                            "url": url,
                            "provider_url": provider_url,
                            "title": result.get("title"),
                            "content": result.get("raw_content") or result.get("content", ""),
                            "published_at": result.get("published_date") or result.get("published_at"),
                            "source_name": source_domain,
                            "searched_provider": "tavily_search",
                        }
                    )
                self._record_search_event(
                    provider="tavily_search",
                    request_type=request_type,
                    target=target_obj,
                    query=query,
                    restricted_domains=list(normalized_domains),
                    http_status=resp.status_code,
                    result_count=len(results),
                    duration_ms=duration_ms,
                )
                self._search_cache[cache_key] = results
                return results, None

        # All candidate keys returned auth failure.
        self._record_search_event(
            provider="tavily_search",
            request_type=request_type,
            target=target_obj,
            query=query,
            restricted_domains=list(normalized_domains),
            http_status=last_status,
            result_count=0,
        )
        return [], last_failure_reason or "http_401"

    def _result_to_article(
        self,
        result: dict[str, Any],
        region: str,
        allow_thin: bool = False,
    ) -> Article | None:
        url = self._canonical_result_url(str(result.get("url") or ""))
        title = (result.get("title") or "").strip()
        content = (result.get("content") or "").strip()
        if not url or not title or (not allow_thin and len(content) < self.min_content_chars):
            return None
        domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
        source_name = result.get("source_name") or domain
        configured_region = self.source_regions.get(source_name)
        origin_region = configured_region
        # Tavily frequently returns published_date=None even for fresh results
        # (the URL path like /2026/07/20/ is clearly recent). Try the explicit
        # field first, then fall back to a URL-path date parse so the freshness
        # gate has something concrete to evaluate.
        published_at = self._parse_published_at(
            result.get("published_at")
            or result.get("published_date")
            or result.get("date")
        )
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

    @staticmethod
    def _canonical_result_url(value: str) -> str:
        """Keep real HTTP(S) destinations and unwrap transparent redirect URLs."""
        raw = str(value or "").strip()
        parsed = urllib.parse.urlparse(raw)
        if parsed.scheme.lower() in {"http", "https"} and parsed.netloc:
            redirect_values = urllib.parse.parse_qs(parsed.query).get("url", [])
            if redirect_values:
                destination = urllib.parse.unquote(redirect_values[0]).strip()
                target = urllib.parse.urlparse(destination)
                if target.scheme.lower() in {"http", "https"} and target.netloc:
                    return destination
            return raw
        redirect_values = urllib.parse.parse_qs(parsed.query).get("url", [])
        if redirect_values:
            destination = urllib.parse.unquote(redirect_values[0]).strip()
            target = urllib.parse.urlparse(destination)
            if target.scheme.lower() in {"http", "https"} and target.netloc:
                return destination
        return ""

    def _parse_published_at(self, value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str) and value.strip():
            try:
                from dateutil import parser as date_parser

                parsed = date_parser.parse(value)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except (ValueError, OverflowError):
                pass
        return None

    # URL date fallbacks cover precise dates plus lower-precision publication
    # paths used by official reports and newsroom archives. Lower-precision
    # dates are represented by the first day of the known period; this is
    # intentionally conservative for the 72-hour freshness gate.
    _URL_FULL_DATE_PATTERN = re.compile(
        r"(?<!\d)(?P<year>(?:19|20)\d{2})[-/_]"
        r"(?P<month>0?[1-9]|1[0-2])[-/_]"
        r"(?P<day>0?[1-9]|[12]\d|3[01])(?!\d)"
    )
    _URL_COMPACT_DATE_PATTERN = re.compile(
        r"(?<!\d)(?P<year>(?:19|20)\d{2})"
        r"(?P<month>0[1-9]|1[0-2])"
        r"(?P<day>0[1-9]|[12]\d|3[01])(?!\d)"
    )
    _URL_QUARTER_PATTERNS = (
        re.compile(
            r"(?<!\d)(?P<year>(?:19|20)\d{2})[_-](?P<quarter>[1-4])Q(?!\d)",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?<!\d)(?P<year>(?:19|20)\d{2})[_-]?Q(?P<quarter>[1-4])(?!\d)",
            re.IGNORECASE,
        ),
    )
    _URL_YEAR_MONTH_PATTERN = re.compile(
        r"(?<!\d)(?P<year>(?:19|20)\d{2})(?:[/_-])"
        r"(?P<month>0?[1-9]|1[0-2])(?=$|[/_.-])"
    )
    _URL_YEAR_MONTH_NAME_PATTERN = re.compile(
        r"(?<!\d)(?P<year>(?:19|20)\d{2})(?:[/_-])"
        r"(?P<month>[A-Za-z]{3,9})(?=$|[/_.-])",
        re.IGNORECASE,
    )

    def _parse_url_date(self, url: str | None) -> datetime | None:
        """Best-effort extraction of a publish date from a URL path.

        Tavily returns published_date=None for many outlets (NYT, CNN, Time,
        northeastern.edu, …). Their URL paths often carry at least a year and
        month (/2026/07/20/article-slug, /2025/september/statement,
        /2021/02/report, or /2023_4Q_business-report). The path is evidence;
        the provider's explicit date remains authoritative when available.
        """
        if not url:
            return None
        path = urllib.parse.unquote(urllib.parse.urlparse(url).path)

        def build_date(year: str, month: int = 1, day: int = 1) -> datetime | None:
            try:
                return datetime(int(year), month, day, tzinfo=timezone.utc)
            except (TypeError, ValueError):
                return None

        match = self._URL_FULL_DATE_PATTERN.search(path) or self._URL_COMPACT_DATE_PATTERN.search(path)
        if match:
            return build_date(
                match.group("year"),
                int(match.group("month")),
                int(match.group("day")),
            )

        for pattern in self._URL_QUARTER_PATTERNS:
            match = pattern.search(path)
            if match:
                quarter = int(match.group("quarter"))
                return build_date(match.group("year"), (quarter - 1) * 3 + 1)

        match = self._URL_YEAR_MONTH_NAME_PATTERN.search(path)
        if match:
            month = _MONTH_NAME_TO_NUMBER.get(match.group("month").casefold())
            if month is not None:
                return build_date(match.group("year"), month)

        match = self._URL_YEAR_MONTH_PATTERN.search(path)
        if match:
            return build_date(match.group("year"), int(match.group("month")))

        # A pure year path (for example /2025/annual-report/) is still useful
        # evidence that the page is historical, even though it has only
        # year-level precision.
        for segment in path.split("/"):
            if re.fullmatch(r"(?:19|20)\d{2}", segment):
                return build_date(segment)
        return None

    # ─── REGION CONFIG / TELEMETRY ───────────────────────────────────────────

    def _build_region_config(self, profiles: dict[str, Any]) -> dict[str, RegionConfig]:
        region_config: dict[str, RegionConfig] = {}
        for region, profile in (profiles or {}).items():
            language = (profile or {}).get("language")
            region_config[str(region).lower()] = RegionConfig(language=language)
        return region_config

    def _record_search_event(
        self,
        provider: str,
        request_type: str,
        target: VoiceTarget | None = None,
        target_region: str | None = None,
        query: str | None = None,
        http_status: int | None = None,
        result_count: int | None = None,
        accepted_count: int | None = None,
        rejection_reason: str | None = None,
        rejection_count: int | None = None,
        duration_ms: int | None = None,
        restricted_domains: list[str] | None = None,
    ) -> None:
        if not self.telemetry_enabled:
            return
        try:
            insert_search_request_event(
                SearchRequestEvent(
                    provider=provider,
                    request_type=request_type,
                    target_region=target.region if target is not None else target_region,
                    target_label=target.label if target is not None else None,
                    target_role=target.role if target is not None else None,
                    cluster_key=target.cluster_key if target is not None else None,
                    target_event_role=target.event_role if target is not None else None,
                    target_reason=target.why_voice_needed if target is not None else None,
                    coverage_before=target.coverage_before if target is not None else None,
                    restricted_domains=list(restricted_domains or []),
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
