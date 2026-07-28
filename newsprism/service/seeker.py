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
from dataclasses import dataclass
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
from newsprism.types import Article, ArticleCluster, SearchCandidateReview, SearchRequestEvent

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


@dataclass(frozen=True)
class CandidateIdentity:
    """Publisher ownership evidence produced by the candidate verifier."""

    source_type: str = "reject"
    publisher_entity: str = ""
    relationship: str = "uncertain"
    ownership_evidence: str = ""
    confidence: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "publisher_entity": self.publisher_entity,
            "relationship": self.relationship,
            "ownership_evidence": self.ownership_evidence,
            "confidence": self.confidence,
        }


_ACTOR_ROLES = {
    "company", "government", "government_agency", "ministry", "party", "organization",
}
_SOURCE_TYPES = {"official_web", "official_social", "country_editorial", "reject"}
_RELATIONSHIPS = {
    "same_entity", "quoted_by_third_party", "covered_by_third_party", "uncertain", "unrelated",
}
_TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())


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
        self.hot_composite_trigger = float(search_cfg.get("hot_composite_trigger", 0.55))

        profiles = search_cfg.get("search_profiles", {}) if isinstance(search_cfg, dict) else {}
        self.region_config = self._build_region_config(profiles)
        verdicts = search_cfg.get("source_verdicts", {}) if isinstance(search_cfg, dict) else {}
        self.source_verdicts = {
            str(domain).lower().removeprefix("www."): dict(spec)
            for domain, spec in (verdicts or {}).items()
            if isinstance(spec, dict)
        }
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
        self._search_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

    # ─── PUBLIC API ──────────────────────────────────────────────────────────

    def enhance_clusters(self, clusters: list[ArticleCluster]) -> list[ArticleCluster]:
        if not self.tavily_api_keys:
            logger.info("Active seeker disabled: no TAVILY_API_KEYS configured")
            return clusters
        # Reset per-run state: keys that auth-failed earlier may have been reset by the provider.
        self._exhausted_keys = set()
        self._search_cache.clear()
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
            ((need.label, need.country, need.kind, need.label) for need in impact.voice_needs),
        )
        if impact_targets:
            return [
                target for target in impact_targets if not self._has_official_voice(cluster, target)
            ][: self.max_regions_per_cluster]

        recovered_targets = self._recover_actor_targets(cluster)
        if recovered_targets:
            return [
                target for target in recovered_targets if not self._has_official_voice(cluster, target)
            ][: self.max_regions_per_cluster]

        targets: list[VoiceTarget] = []
        for region in impact.subject_regions:
            if not is_recognized_country(region):
                continue
            target = VoiceTarget(region=region, label=country_name(region), role="country")
            if not self._has_country_voice(cluster, target):
                targets.append(target)
        return targets[: self.max_regions_per_cluster]

    def _validated_actor_targets(
        self,
        cluster: ArticleCluster,
        values: Any,
    ) -> list[VoiceTarget]:
        targets: list[VoiceTarget] = []
        seen: set[tuple[str, str]] = set()
        for label, region, role, evidence_text in values:
            target = self._validate_actor_target(cluster, label, region, role, evidence_text)
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
    ) -> VoiceTarget | None:
        normalized_label = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(label or "")).strip())[:100]
        normalized_region = str(region or "").strip().lower()
        normalized_role = str(role or "organization").strip().lower()
        normalized_evidence = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(evidence_text or "")).strip())[:200]
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
        event_text = self._event_text(cluster)
        normalized_event_text = self._identity_text(event_text)
        label_present = self._identity_text(normalized_label) in normalized_event_text
        evidence_present = bool(
            normalized_evidence
            and self._identity_text(normalized_evidence) in normalized_event_text
        )
        if not label_present and not evidence_present:
            return None
        return VoiceTarget(region=normalized_region, label=normalized_label, role=normalized_role)

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
            "Extract only named organizations whose direct response would clarify this exact news event. "
            "Do not return countries, people, products, media, or inferred formal names. "
            "Return JSON only: {\"targets\":[{\"label\":\"Microsoft\",\"country\":\"us\","
            "\"kind\":\"company\",\"evidence_text\":\"Microsoft\"}]}.\n\n"
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
            values = (
                (item.get("label"), item.get("country"), item.get("kind"), item.get("evidence_text"))
                for item in parsed.get("targets", []) if isinstance(item, dict)
            )
            return self._validated_actor_targets(cluster, values)
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
        return any(
            not article.is_searched
            and (article.origin_region or self.source_regions.get(article.source_name)) == target.region
            for article in cluster.articles
        )

    def _has_official_voice(self, cluster: ArticleCluster, target: VoiceTarget) -> bool:
        return any(
            article.is_official_source
            and not article.is_placeholder
            and self._article_mentions_target(article, target)
            for article in cluster.articles
        )

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
            return str(parsed.get("keyword") or "").strip()[:160]
        except Exception as exc:
            logger.debug("Search target analysis failed for '%s': %s", cluster.topic_category, exc)
            return ""

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
            article, fail_reason = self._search_target(cluster, target, keyword, centroid)
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
            # Synthesize a flat inline placeholder so the reader sees that this
            # region's perspective was targeted but unavailable — never silent.
            placeholder = self._placeholder_article(cluster, target, fail_reason or "unknown")
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
    ) -> tuple[Article | None, str | None]:
        """Return (accepted_article, failure_reason).

        failure_reason is set only when no article was accepted, so the caller
        can synthesize an inline placeholder that surfaces the cause to readers.
        """
        stages = ("country",) if target.role == "country" else ("official", "country")
        last_reason: str | None = None
        for stage in stages:
            for query in self._build_search_queries(cluster, target, keyword, stage):
                results, search_fail = self._search_tavily(target, query)
                if search_fail:
                    # Provider-level failure means no reliable conclusion about
                    # this missing voice; do not disguise it as no result.
                    return None, search_fail
                accepted, rejections = self._accept_results(
                    cluster, target, results, centroid, stage
                )
                self._record_search_event(
                    provider="tavily_search",
                    request_type=f"acceptance_{stage}",
                    target=target,
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
                    last_reason = "official_not_found" if stage == "official" else "country_fallback_not_found"
                else:
                    last_reason = "candidate_unverified"
        return None, last_reason

    def _placeholder_article(
        self,
        cluster: ArticleCluster,
        target: VoiceTarget,
        reason: str,
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
        existing_urls = {article.url for article in cluster.articles}
        existing_titles = [article.title for article in cluster.articles]
        for result in results:
            article = self._result_to_article(result, target.region)
            if article is None:
                rejections.append(("thin_result", str(result.get("url"))))
                continue
            if article.url in existing_urls:
                rejections.append(("already_present", article.url))
                continue
            reason = self._rejection_reason(article, target, existing_titles, centroid)
            if reason:
                rejections.append((reason, article.url))
                continue
            identity = self._candidate_identity(self._verify_candidate(article, target, stage))
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
                official_reason = self._official_identity_reason(article, target, identity, registry_identity)
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
            self._record_candidate_review(article, target, stage, identity, "accepted", "")
            accepted.append(article)
            if len(accepted) >= self.max_results_per_region:
                break
        return accepted, rejections

    def _rejection_reason(
        self,
        article: Article,
        target: VoiceTarget,
        existing_titles: list[str],
        centroid: np.ndarray | None,
    ) -> str:
        if target.role != "country" and not self._article_mentions_target(article, target):
            return "entity_mismatch"
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

    def _article_mentions_target(self, article: Article, target: VoiceTarget) -> bool:
        text = f"{article.title}\n{article.content[:1200]}".casefold()
        return target.label.casefold() in text

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
            if approved_region and approved_region != target.region:
                return None, "not_related_country_source"
            if stage == "official":
                # Social ownership is account-scoped; a host/domain binding is
                # never enough to establish who published a social post.
                if verdict == "official_social":
                    return None, "publisher_binding_unverified"
                if verdict != "official_web":
                    return None, "not_official_source"
                if not approved_entity or self._identity_text(approved_entity) != self._identity_text(target.label):
                    return None, "publisher_binding_unverified"
                return CandidateIdentity(
                    source_type=verdict,
                    publisher_entity=approved_entity,
                    relationship="same_entity",
                    ownership_evidence="reviewed exact entity binding",
                ), None
            if verdict != "country_editorial" or not approved_region:
                return None, "not_related_country_source"
            return CandidateIdentity(
                source_type="country_editorial",
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
        if identity.relationship != "same_entity" or self._identity_text(identity.publisher_entity) != self._identity_text(target.label):
            return "publisher_target_mismatch"
        if identity.source_type == "official_social":
            return None if self._social_account_binding(article.url, target) else "publisher_binding_unverified"
        registered = _TLD_EXTRACT(urllib.parse.urlparse(article.url).netloc)
        if self._identity_text(registered.domain) != self._identity_text(target.label):
            return "publisher_binding_unverified"
        return None

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
                ),
                db_path=self.telemetry_db_path,
            )
        except Exception as exc:
            logger.debug("Search candidate telemetry write failed: %s", exc)

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

    def _build_search_queries(
        self,
        cluster: ArticleCluster,
        target: VoiceTarget,
        keyword: str,
        stage: str,
    ) -> list[str]:
        config = self.region_config.get(target.region)
        if stage == "official":
            english_query = f"{target.label} official statement {keyword}"
        else:
            english_query = f"{keyword} {country_name(target.region)} local news"
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
        cache_key = (region, query)
        if cache_key in self._search_cache:
            return self._search_cache[cache_key], None

        base_payload: dict[str, Any] = {
            "query": query,
            "search_depth": "basic",
            "include_raw_content": True,
            "max_results": max(self.max_results_per_region + 2, 4),
            "days": 3,
        }
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
                target=target_obj,
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
                            target=target_obj,
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
                        target=target_obj,
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
                    target=target_obj,
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
                    target=target_obj,
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
            target=target_obj,
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
        origin_region = configured_region
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
