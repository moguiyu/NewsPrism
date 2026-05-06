"""Cluster-level quality assessment and editorial gating.

Layer: service (imports config, types; never imports repo or runtime)
"""
from __future__ import annotations

import hashlib
import logging
import re
from collections import Counter
from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.types import (
    Article,
    ArticleCluster,
    Claim,
    ClusterQualityReport,
    ClusterSummary,
    Evidence,
    QualityDecision,
)

logger = logging.getLogger(__name__)

_HIGH_RISK_TERMS = {
    "war", "strike", "attack", "conflict", "sanction", "tariff", "lawsuit", "court", "crash", "killed", "dead",
    "战争", "打击", "袭击", "冲突", "制裁", "关税", "诉讼", "法院", "坠毁", "死亡", "遇难", "爆炸", "危机",
}
_CORRECTION_TERMS = {
    "correction", "corrected", "retract", "retracted", "denies", "denied", "clarifies",
    "更正", "纠正", "撤回", "否认", "澄清", "辟谣",
}
_FORECAST_TERMS = {
    "will", "could", "may", "plans", "expects", "forecast", "predict",
    "将", "可能", "计划", "预计", "预测", "或将",
}
_CAUSAL_TERMS = {"because", "after", "amid", "due to", "导致", "由于", "因", "之后", "受"}


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _claim_tokens(text: str) -> set[str]:
    normalized = text.lower()
    tokens = {token for token in re.findall(r"[a-z0-9]{3,}", normalized)}
    for chunk in re.findall(r"[\u4e00-\u9fff]{2,24}", text):
        if len(chunk) <= 3:
            tokens.add(chunk)
            continue
        for size in (2, 3):
            for idx in range(len(chunk) - size + 1):
                tokens.add(chunk[idx:idx + size])
    return tokens


def _token_overlap(left: str, right: str) -> float:
    left_tokens = _claim_tokens(left)
    right_tokens = _claim_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens)


def _first_sentence(text: str, max_chars: int = 220) -> str:
    cleaned = _compact_text(text)
    if not cleaned:
        return ""
    parts = re.split(r"(?<=[。！？.!?])\s+", cleaned)
    candidate = parts[0] if parts else cleaned
    return candidate[:max_chars].strip()


def _source_key(article: Article) -> str:
    return article.source_name.strip() or article.url


class ClaimExtractor:
    def __init__(self, max_claims: int = 6) -> None:
        self.max_claims = max(1, max_claims)

    def extract(self, cluster: ArticleCluster) -> list[Claim]:
        claims: list[Claim] = []
        seen: set[str] = set()
        for article in cluster.articles:
            candidates = [article.title, _first_sentence(article.content)]
            for candidate in candidates:
                normalized = _compact_text(candidate)
                if len(normalized) < 8:
                    continue
                fingerprint = self._fingerprint(normalized)
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                claim_id = self._claim_id(cluster, normalized, len(claims) + 1)
                claims.append(
                    Claim(
                        claim_id=claim_id,
                        text=normalized,
                        claim_type=self._claim_type(normalized),
                        importance=self._importance(normalized, article),
                        source_names=[_source_key(article)],
                    )
                )
                if len(claims) >= self.max_claims:
                    return claims
        return claims

    def _fingerprint(self, text: str) -> str:
        tokens = sorted(_claim_tokens(text))
        if not tokens:
            return text.lower()[:80]
        return " ".join(tokens[:20])

    def _claim_id(self, cluster: ArticleCluster, text: str, index: int) -> str:
        seed = "|".join(article.url for article in cluster.articles[:4]) + "|" + text
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
        return f"claim-{index}-{digest}"

    def _claim_type(self, text: str) -> str:
        lowered = text.lower()
        if any(term in lowered or term in text for term in _CORRECTION_TERMS):
            return "correction"
        if re.search(r"\d", text):
            return "number"
        if any(term in lowered or term in text for term in _FORECAST_TERMS):
            return "forecast"
        if any(term in lowered or term in text for term in _CAUSAL_TERMS):
            return "causal"
        return "event"

    def _importance(self, text: str, article: Article) -> float:
        score = 0.45
        if text == article.title:
            score += 0.20
        if any(term in text.lower() or term in text for term in _HIGH_RISK_TERMS):
            score += 0.15
        if re.search(r"\d", text):
            score += 0.10
        return _clamp(score)


class EvidenceMatcher:
    def __init__(self, min_support_confidence: float = 0.55) -> None:
        self.min_support_confidence = min_support_confidence

    def match(self, claims: list[Claim], cluster: ArticleCluster) -> list[Evidence]:
        evidence: list[Evidence] = []
        for claim in claims:
            for article in cluster.articles:
                confidence = max(
                    _token_overlap(claim.text, article.title),
                    _token_overlap(claim.text, article.content[:1200]),
                )
                stance = "supports" if confidence >= self.min_support_confidence else "uncovered"
                evidence.append(
                    Evidence(
                        claim_id=claim.claim_id or "",
                        source_name=_source_key(article),
                        stance=stance,
                        excerpt=self._excerpt(article, claim.text) if stance == "supports" else "",
                        confidence=_clamp(confidence),
                    )
                )
        return evidence

    def _excerpt(self, article: Article, claim: str) -> str:
        title_overlap = _token_overlap(claim, article.title)
        if title_overlap >= self.min_support_confidence:
            return article.title[:240]
        return _first_sentence(article.content, max_chars=240)


class SourceReliabilityScorer:
    def __init__(self, cfg: Config) -> None:
        editorial = cfg.editorial_values or {}
        reliability = editorial.get("source_reliability", {}) if isinstance(editorial, dict) else {}
        self.tier_scores = reliability.get("tier_scores", {}) if isinstance(reliability, dict) else {}
        self.searched_provider_penalty = float(reliability.get("searched_provider_penalty", 0.08))
        self.independent_source_bonus = float(reliability.get("independent_source_bonus", 0.06))
        self.official_source_penalty = float(reliability.get("official_source_penalty", 0.03))
        self.source_tiers = {source.name: source.tier for source in cfg.sources}

    def score_cluster(self, cluster: ArticleCluster) -> float:
        if not cluster.articles:
            return 0.0
        scores = [self.score_article(article) for article in cluster.articles]
        return _clamp(sum(scores) / len(scores))

    def score_article(self, article: Article) -> float:
        tier = self.source_tiers.get(article.source_name)
        if not tier and article.source_kind != "news":
            tier = article.source_kind
        if not tier and article.is_searched:
            tier = "active_search"
        tier = tier or "unknown"
        score = float(self.tier_scores.get(tier, self.tier_scores.get("unknown", 0.45)))
        if article.is_searched:
            score -= self.searched_provider_penalty
        if article.is_official_source:
            score -= self.official_source_penalty
        elif tier in {"editorial", "tech"}:
            score += self.independent_source_bonus
        return _clamp(score)


class BiasFrameAnalyzer:
    def __init__(self, source_regions: dict[str, str] | None = None) -> None:
        self.source_regions = source_regions or {}

    def analyze(self, cluster: ArticleCluster) -> tuple[float, list[str]]:
        flags: list[str] = []
        source_regions = {
            article.origin_region or self.source_regions.get(article.source_name)
            for article in cluster.articles
            if article.origin_region or self.source_regions.get(article.source_name)
        }
        source_kinds = Counter(article.source_kind for article in cluster.articles)
        official_count = sum(1 for article in cluster.articles if article.is_official_source or article.source_kind != "news")
        text = " ".join([cluster.topic_category] + [article.title for article in cluster.articles])
        high_risk = any(term in text.lower() or term in text for term in _HIGH_RISK_TERMS)

        risk = 0.15
        if high_risk:
            risk += 0.25
            flags.append("high_risk_topic")
        if len(cluster.articles) == 1:
            risk += 0.25
            flags.append("single_source")
        if len(source_regions) <= 1 and len(cluster.articles) >= 2:
            risk += 0.12
            flags.append("single_region_frame")
        if official_count and official_count == len(cluster.articles):
            risk += 0.20
            flags.append("official_only")
        elif official_count:
            flags.append("official_source_present")
        if source_kinds.get("official_social", 0):
            risk += 0.06
            flags.append("social_official_source")
        return _clamp(risk), flags


class QualityGate:
    def __init__(self, cfg: Config) -> None:
        editorial = cfg.editorial_values or {}
        quality = editorial.get("quality", {}) if isinstance(editorial, dict) else {}
        self.min_publish_score = float(quality.get("min_publish_score", 0.45))
        self.min_review_score = float(quality.get("min_review_score", 0.35))
        self.suppress_score = float(quality.get("suppress_score", 0.20))
        self.high_risk_single_source_review = bool(quality.get("high_risk_single_source_review", True))
        self.high_risk_single_source_suppress = bool(quality.get("high_risk_single_source_suppress", False))

    def decide(self, report: ClusterQualityReport) -> QualityDecision:
        constraints = [
            "Only state facts supported by the provided article evidence.",
            "Attribute contested or single-source claims explicitly.",
        ]
        flags = set(report.flags)
        if report.overall_score <= self.suppress_score:
            return QualityDecision("suppress", "quality score below suppress threshold", False, constraints)
        if self.high_risk_single_source_suppress and {"high_risk_topic", "single_source"} <= flags:
            return QualityDecision("suppress", "high-risk story has only one source", False, constraints)
        if {"high_risk_topic", "single_source"} <= flags and self.high_risk_single_source_review:
            return QualityDecision("seek_more_evidence", "high-risk story needs independent confirmation", True, constraints)
        if report.contested_claims:
            constraints.append("Do not present contested claims as settled facts.")
            return QualityDecision("needs_review", "contested claims detected", False, constraints)
        if report.overall_score < self.min_review_score:
            return QualityDecision("suppress", "quality score below review threshold", False, constraints)
        if report.overall_score < self.min_publish_score:
            return QualityDecision("needs_review", "quality score below publish threshold", False, constraints)
        if "official_only" in flags:
            constraints.append("Mark official-only framing as an attributed official position.")
            return QualityDecision("needs_review", "official-only evidence", False, constraints)
        return QualityDecision("publishable", "quality gate passed", False, constraints)


class QualityAssessor:
    def __init__(self, cfg: Config) -> None:
        editorial = cfg.editorial_values or {}
        quality = editorial.get("quality", {}) if isinstance(editorial, dict) else {}
        self.extractor = ClaimExtractor(max_claims=int(quality.get("max_claims_per_cluster", 6)))
        self.matcher = EvidenceMatcher(
            min_support_confidence=float(quality.get("min_claim_support_confidence", 0.55))
        )
        self.reliability = SourceReliabilityScorer(cfg)
        self.source_regions = {source.name: source.region for source in cfg.sources}
        self.bias = BiasFrameAnalyzer(self.source_regions)
        self.gate = QualityGate(cfg)

    def assess_cluster(self, cluster: ArticleCluster) -> ClusterQualityReport:
        claims = self.extractor.extract(cluster)
        evidence = self.matcher.match(claims, cluster)
        fact_coverage = self._fact_coverage(claims, evidence)
        source_diversity = self._source_diversity(cluster)
        reliability_score = self.reliability.score_cluster(cluster)
        bias_risk, flags = self.bias.analyze(cluster)
        confirmed_claims, contested_claims = self._claim_rollup(claims, evidence)
        overall_score = _clamp(
            fact_coverage * 0.34
            + source_diversity * 0.22
            + reliability_score * 0.30
            + (1.0 - bias_risk) * 0.14
        )
        report = ClusterQualityReport(
            cluster_key=self._cluster_key(cluster),
            claims=claims,
            evidence=evidence,
            fact_coverage=fact_coverage,
            source_diversity=source_diversity,
            reliability_score=reliability_score,
            bias_risk=bias_risk,
            overall_score=overall_score,
            flags=flags,
            confirmed_claims=confirmed_claims,
            contested_claims=contested_claims,
            evidence_summary=self._evidence_summary(cluster, confirmed_claims, contested_claims, flags),
            created_at=datetime.now(tz=timezone.utc),
        )
        decision = self.gate.decide(report)
        report.decision = decision
        report.status = decision.status
        cluster.quality_report = report
        cluster.quality_decision = decision
        return report

    def assess_clusters(self, clusters: list[ArticleCluster]) -> list[ClusterQualityReport]:
        return [self.assess_cluster(cluster) for cluster in clusters]

    def postcheck_summary(self, summary: ClusterSummary) -> ClusterQualityReport:
        report = summary.cluster.quality_report or self.assess_cluster(summary.cluster)
        body = summary.summary.lower()
        if report.claims and not any(self._claim_mentions_summary(claim, body) for claim in report.claims):
            if "summary_claim_gap" not in report.flags:
                report.flags.append("summary_claim_gap")
            if report.status == "publishable":
                report.status = "needs_review"
                report.decision = QualityDecision(
                    status="needs_review",
                    reason="summary does not clearly map to extracted claims",
                    needs_more_evidence=False,
                    summary_constraints=report.decision.summary_constraints,
                )
        self._attach_to_summary(summary, report)
        return report

    def _attach_to_summary(self, summary: ClusterSummary, report: ClusterQualityReport) -> None:
        summary.quality_report = report
        summary.quality_status = report.status
        summary.quality_score = report.overall_score
        summary.quality_flags = list(report.flags)
        summary.confirmed_claims = list(report.confirmed_claims)
        summary.contested_claims = list(report.contested_claims)
        summary.evidence_summary = report.evidence_summary

    def _fact_coverage(self, claims: list[Claim], evidence: list[Evidence]) -> float:
        if not claims:
            return 0.0
        supported = 0
        for claim in claims:
            claim_id = claim.claim_id or ""
            if any(item.claim_id == claim_id and item.stance == "supports" for item in evidence):
                supported += 1
        return supported / len(claims)

    def _source_diversity(self, cluster: ArticleCluster) -> float:
        if not cluster.articles:
            return 0.0
        source_count = len({_source_key(article) for article in cluster.articles})
        regions = {
            article.origin_region or self.source_regions.get(article.source_name)
            for article in cluster.articles
            if article.origin_region or self.source_regions.get(article.source_name)
        }
        region_score = min(len(regions), 3) / 3 if regions else 0.0
        source_score = min(source_count, 4) / 4
        return _clamp(source_score * 0.65 + region_score * 0.35)

    def _claim_rollup(self, claims: list[Claim], evidence: list[Evidence]) -> tuple[list[str], list[str]]:
        confirmed: list[str] = []
        contested: list[str] = []
        for claim in claims:
            claim_id = claim.claim_id or ""
            supporting_sources = {
                item.source_name
                for item in evidence
                if item.claim_id == claim_id and item.stance == "supports"
            }
            refuting_sources = {
                item.source_name
                for item in evidence
                if item.claim_id == claim_id and item.stance == "refutes"
            }
            if supporting_sources and refuting_sources:
                contested.append(claim.text)
            elif len(supporting_sources) >= 2 or (len(supporting_sources) == 1 and claim.claim_type not in {"forecast", "causal"}):
                confirmed.append(claim.text)
        return confirmed[:6], contested[:6]

    def _evidence_summary(
        self,
        cluster: ArticleCluster,
        confirmed_claims: list[str],
        contested_claims: list[str],
        flags: list[str],
    ) -> str:
        source_count = len({_source_key(article) for article in cluster.articles})
        if contested_claims:
            return f"{source_count} sources assessed; contested claims require attribution."
        if confirmed_claims:
            return f"{source_count} sources assessed; {len(confirmed_claims)} claim(s) have source support."
        if "single_source" in flags:
            return "Single-source story; summary must be tightly attributed."
        return f"{source_count} sources assessed."

    def _cluster_key(self, cluster: ArticleCluster) -> str:
        if cluster.storyline_key:
            return cluster.storyline_key
        seed = "|".join(article.url for article in cluster.articles[:4]) or cluster.topic_category
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]

    def _claim_mentions_summary(self, claim: Claim, summary_body: str) -> bool:
        tokens = _claim_tokens(claim.text)
        if not tokens:
            return False
        summary_tokens = _claim_tokens(summary_body)
        return len(tokens & summary_tokens) / len(tokens) >= 0.20
