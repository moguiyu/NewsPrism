"""LLM-driven multi-dimensional impact evaluation — the selection brain.

One batched LLM call per publish scores every candidate event cluster on
real-human-impact dimensions (0–10 each). A cross-source signal dimension is
computed locally from source/region/tier facts. The calibrated composite of
both decides selection, the 今日正能量 lane, editorial status, and where the
active seeker spends search budget. No keyword lists anywhere.

Calibration weights come from the calibration_weights table (seeded from
config/editorial-values.yaml); the latest editorial_policy bullets are
injected into the prompt as persistent editorial memory.

Layer: service (imports types, config, repo)
"""
from __future__ import annotations

import hashlib
import json
import logging
import re

import litellm
from pydantic import BaseModel, Field

from newsprism.config import Config
from newsprism.repo import get_calibration_weights, get_latest_editorial_policy
from newsprism.service.categories import (
    DEFAULT_DISPLAY_CATEGORY,
    DISPLAY_CATEGORIES,
    normalize_display_category,
)
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.types import Article, ArticleCluster, ImpactAssessment

logger = logging.getLogger(__name__)

DIMENSIONS = (
    "scope",
    "severity",
    "novelty",
    "actor_influence",
    "decision_relevance",
    "feelgood",
)

# Seeds; live values come from the calibration_weights table.
DEFAULT_WEIGHTS: dict[str, float] = {
    "scope": 0.16,
    "severity": 0.16,
    "novelty": 0.12,
    "actor_influence": 0.14,
    "decision_relevance": 0.18,
    "feelgood": 0.0,   # uplift drives the positive lane, not main-lane importance
    "signal": 0.24,
}


def cluster_key(cluster: ArticleCluster) -> str:
    """Stable key for evaluation persistence; same scheme for link-after-insert."""
    seed = "|".join(article.url for article in cluster.articles[:4]) or cluster.topic_category
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


class ImpactItem(BaseModel):
    cluster_index: int
    scope: float = 0.0
    severity: float = 0.0
    novelty: float = 0.0
    actor_influence: float = 0.0
    decision_relevance: float = 0.0
    feelgood: float = 0.0
    rationale: str = ""
    display_category: str = DEFAULT_DISPLAY_CATEGORY
    short_topic_name: str | None = None
    topic_icon_key: str | None = None
    subject_regions: list[str] = Field(default_factory=list)
    target_region: str | None = None     # ISO alpha-2 of whose 内政 this is about
    is_home_affairs: bool = False        # True when the story falls within the 内政 boundary


class ImpactBatch(BaseModel):
    items: list[ImpactItem] = Field(default_factory=list)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _clamp_dim(value: float) -> float:
    return max(0.0, min(10.0, float(value)))


def _norm_regions(values: list[str] | None) -> list[str]:
    """Lowercase, trim, drop blanks/dupes, cap at 3 — the subject-country codes."""
    out: list[str] = []
    for value in values or []:
        code = str(value).strip().lower()
        if code and code not in out:
            out.append(code)
        if len(out) == 3:
            break
    return out


class ImpactAssessor:
    def __init__(
        self,
        cfg: Config,
        weights_loader=None,
        policy_loader=None,
    ) -> None:
        self.cfg = cfg
        self.model = cfg.litellm_model
        self.api_key = cfg.litellm_api_key
        self.base_url = cfg.litellm_base_url
        self.compat_kwargs = completion_compat_kwargs(self.model, self.base_url)
        self._weights_loader = weights_loader
        self._policy_loader = policy_loader

        editorial = cfg.editorial_values or {}
        impact_cfg = editorial.get("impact", {}) if isinstance(editorial, dict) else {}
        raw_weights = impact_cfg.get("weights", {}) if isinstance(impact_cfg, dict) else {}
        self.seed_weights = {
            key: float(raw_weights.get(key, default))
            for key, default in DEFAULT_WEIGHTS.items()
        }
        status_cfg = impact_cfg.get("status", {}) if isinstance(impact_cfg, dict) else {}
        self.suppress_floor = float(status_cfg.get("suppress_floor", 0.18))
        self.review_floor = float(status_cfg.get("review_floor", 0.34))
        self.single_source_severity_review = float(
            status_cfg.get("single_source_severity_review", 6.0)
        )
        self.batch_size = max(1, int(impact_cfg.get("batch_size", 40)))

        reliability = editorial.get("source_reliability", {}) if isinstance(editorial, dict) else {}
        self.tier_scores = reliability.get("tier_scores", {}) if isinstance(reliability, dict) else {}
        self.source_tiers = {source.name: source.tier for source in cfg.sources}
        self.source_regions = {source.name: source.region for source in cfg.sources}

        hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
        self.icon_allowlist = list(
            hot_cfg.get("icon_allowlist", ["globe", "war", "trade", "chip", "ai", "energy"])
        )

    # ─── PUBLIC API ──────────────────────────────────────────────────────────

    def rank_candidates(self, clusters: list[ArticleCluster], window: int) -> list[ArticleCluster]:
        """Return the top-`window` clusters by local cross-source signal (no LLM).

        Bounds how many clusters get LLM-scored. Signal rewards multi-source /
        multi-region / high-tier pickup — a non-keyword proxy for "this story
        has real coverage" — so the impact call still decides among a broad set.
        """
        if len(clusters) <= window:
            return list(clusters)
        scored = sorted(clusters, key=lambda c: self._signal(c)[0], reverse=True)
        return scored[:window]

    def assess_clusters(self, clusters: list[ArticleCluster]) -> list[ImpactAssessment]:
        """Evaluate all clusters; attach the assessment to each cluster."""
        if not clusters:
            return []

        items_by_index: dict[int, ImpactItem] = {}
        llm_ok = True
        for start in range(0, len(clusters), self.batch_size):
            chunk = clusters[start:start + self.batch_size]
            chunk_items = self._evaluate_chunk(chunk, start)
            if chunk_items is None:
                llm_ok = False
            else:
                items_by_index.update(chunk_items)

        weights = self.weights()
        assessments: list[ImpactAssessment] = []
        missing = 0
        for index, cluster in enumerate(clusters):
            item = items_by_index.get(index)
            if item is None:
                missing += 1
            assessment = self._build_assessment(cluster, item, weights)
            cluster.impact = assessment
            cluster.display_category = assessment.display_category
            assessments.append(assessment)

        status_counts: dict[str, int] = {}
        for assessment in assessments:
            status_counts[assessment.status] = status_counts.get(assessment.status, 0) + 1
        logger.info(
            "Impact evaluation: %d clusters scored (llm_ok=%s, signal-only fallback=%d) statuses=%s top_composite=%.3f",
            len(assessments),
            llm_ok,
            missing,
            status_counts,
            max((a.composite for a in assessments), default=0.0),
        )
        return assessments

    def weights(self) -> dict[str, float]:
        """Calibrated weights (table) over seeds (yaml), normalized to sum 1."""
        loader = self._weights_loader or get_calibration_weights
        try:
            calibrated = loader()
        except Exception as exc:
            logger.warning("Calibration weights unavailable, using seeds: %s", exc)
            calibrated = {}
        merged = dict(self.seed_weights)
        for key, value in calibrated.items():
            if key in merged:
                merged[key] = float(value)
        total = sum(merged.values())
        if total <= 0:
            return dict(DEFAULT_WEIGHTS)
        return {key: value / total for key, value in merged.items()}

    def recompute_local(self, cluster: ArticleCluster) -> None:
        """Refresh signal/composite/status after cluster membership changed (e.g. seeker)."""
        if cluster.impact is None:
            return
        weights = self.weights()
        cluster.impact = self._build_assessment(
            cluster,
            self._item_from_assessment(cluster.impact),
            weights,
        )
        cluster.display_category = cluster.impact.display_category

    # ─── LLM CALL ────────────────────────────────────────────────────────────

    def _evaluate_chunk(
        self,
        chunk: list[ArticleCluster],
        offset: int,
    ) -> dict[int, ImpactItem] | None:
        prompt = self._build_prompt(chunk)
        content = ""
        for attempt, suffix in enumerate(("", "\n\n最后要求：只输出紧凑 JSON，不要解释，不要 Markdown。")):
            try:
                content = self._completion(prompt + suffix, max_tokens=min(16000, 600 + len(chunk) * 220))
            except Exception as exc:
                logger.warning("Impact LLM call failed (attempt %d): %s", attempt + 1, exc)
                continue
            parsed = self._parse_batch(content)
            if parsed is not None:
                return {
                    offset + item.cluster_index - 1: item
                    for item in parsed.items
                    if 1 <= item.cluster_index <= len(chunk)
                }
        salvaged = self._salvage_items(content, len(chunk))
        if salvaged:
            logger.warning("Impact evaluation salvaged %d/%d items from malformed output", len(salvaged), len(chunk))
            return {offset + item.cluster_index - 1: item for item in salvaged}
        logger.error("Impact evaluation failed for chunk of %d clusters — signal-only fallback", len(chunk))
        return None

    def _completion(self, prompt: str, max_tokens: int) -> str:
        response = litellm.completion(
            model=self.model,
            api_key=self.api_key,
            api_base=self.base_url,
            messages=[
                {"role": "system", "content": self._system_prompt()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            **self.compat_kwargs,
        )
        return response.choices[0].message.content or ""

    def _system_prompt(self) -> str:
        base = (
            "你是一家国际新闻编辑部的执行主编。你的职责是判断每条新闻对人类的真实影响，"
            "而不是按题材、关键词或来源数量打分。评分必须基于事件本身的后果。只输出 JSON。"
        )
        policy = self._policy_text()
        if policy:
            base += "\n\n编辑部当前政策备忘（必须遵守）：\n" + policy
        return base

    def _policy_text(self) -> str:
        loader = self._policy_loader or get_latest_editorial_policy
        try:
            return (loader() or "").strip()
        except Exception:
            return ""

    def _build_prompt(self, chunk: list[ArticleCluster]) -> str:
        rows = []
        for index, cluster in enumerate(chunk, 1):
            lead = cluster.articles[0] if cluster.articles else None
            titles = [
                f"{article.source_name}: {article.title}"
                for article in cluster.articles[:3]
            ]
            regions = sorted(
                {
                    article.origin_region or self.source_regions.get(article.source_name)
                    for article in cluster.articles
                }
                - {None}
            )
            rows.append(
                {
                    "cluster_index": index,
                    "titles": titles,
                    "snippet": (lead.content[:240] if lead else ""),
                    "source_count": len(cluster.sources),
                    "regions": regions,
                }
            )
        categories = ", ".join(DISPLAY_CATEGORIES)
        icons = ", ".join(self.icon_allowlist)
        return (
            f"对以下 {len(chunk)} 个新闻事件簇逐一评估影响力。每个维度打 0-10 分（可用整数）：\n"
            "- scope 波及范围：受影响的人数与系统广度。0=个别人，5=一国主要人群，10=全球大部分人\n"
            "- severity 严重与不可逆：后果深度与不可逆程度。0=琐事，5=重大但可恢复，10=不可逆转折（开战/宪政危机/重大技术或医学突破）\n"
            "- novelty 新颖性：相对既有常态的新信息量。0=例行重复或日常波动，10=前所未有\n"
            "- actor_influence 行为体影响力：涉事国家、机构、公司的体量与系统重要性。0=无名个体，10=超级大国或系统重要机构\n"
            "- decision_relevance 决策相关：是否改变一个关注全球大势的读者应有的判断或决策\n"
            "- feelgood 治愈轻松：让人开心、可爱、好笑、暖心或振奋的程度。严肃、负面、冲突类新闻一律 0\n"
            "其他字段：\n"
            f"- display_category：只能从这 6 个中选一个：{categories}\n"
            "- short_topic_name：4-10 个中文字符的短专题名\n"
            f"- topic_icon_key：只能从这些键中选一个：{icons}\n"
            "- rationale：不超过 30 个中文字符，说明影响判断的核心依据\n"
            "- subject_regions：该事件主要涉及的国家/地区，用小写 ISO 代码数组（最多 3 个），如 [\"il\",\"ir\"]；与新闻来源国不同，指事件本身发生/影响的国家\n"
            "- target_region：如果事件主要涉及一个国家的内政（国内治理），填写该国的小写 ISO 代码；如果是外交、贸易、战争、国际组织或科技/文化事件，填 null\n"
            "- is_home_affairs：布尔值。true 表示这是某国的内政（选举、国内政策、法律、人权国内实施、社会保障、国内治安、抗议）；false 表示外交、战争、贸易、国际组织、科技、文化或无法确定\n"
            "要求：\n"
            "1. 按事件实际后果打分，不要因为来源多就抬高分数（来源信号由系统单独计算）。\n"
            "2. 例行市场波动、产品促销、明星八卦等低影响内容应得到明显低分。\n"
            "3. cluster_index 必须与输入一致（从 1 开始），每个簇恰好输出一项。\n"
            "只输出 JSON：{\"items\":[{\"cluster_index\":1,\"scope\":7,\"severity\":6,\"novelty\":5,"
            "\"actor_influence\":8,\"decision_relevance\":7,\"feelgood\":0,"
            "\"rationale\":\"...\",\"display_category\":\"World\",\"short_topic_name\":\"...\","
            "\"topic_icon_key\":\"globe\",\"subject_regions\":[\"il\"],"
            "\"target_region\":\"il\",\"is_home_affairs\":true}]}\n\n"
            f"事件簇：\n{json.dumps(rows, ensure_ascii=False)}"
        )

    def _parse_batch(self, content: str) -> ImpactBatch | None:
        text = (content or "").strip()
        if not text:
            return None
        try:
            return ImpactBatch.model_validate_json(text)
        except Exception:
            start, end = text.find("{"), text.rfind("}")
            if start != -1 and end > start:
                try:
                    return ImpactBatch.model_validate_json(text[start:end + 1])
                except Exception:
                    return None
        return None

    def _salvage_items(self, content: str, chunk_len: int) -> list[ImpactItem]:
        if not content.strip():
            return []
        pattern = re.compile(
            r'\{\s*"cluster_index"\s*:\s*(?P<idx>\d+)(?P<body>.*?)(?:\}|\Z)',
            re.DOTALL,
        )
        salvaged: list[ImpactItem] = []
        seen: set[int] = set()
        for match in pattern.finditer(content):
            idx = int(match.group("idx"))
            if idx in seen or not (1 <= idx <= chunk_len):
                continue
            body = match.group("body")
            dims: dict[str, float] = {}
            for dim in DIMENSIONS:
                value = re.search(rf'"{dim}"\s*:\s*([0-9]*\.?[0-9]+)', body)
                if value:
                    dims[dim] = float(value.group(1))
            if not dims:
                continue
            seen.add(idx)
            category = re.search(r'"display_category"\s*:\s*"([^"]*)"', body)
            rationale = re.search(r'"rationale"\s*:\s*"([^"]*)"', body)
            short_name = re.search(r'"short_topic_name"\s*:\s*"([^"]*)"', body)
            icon = re.search(r'"topic_icon_key"\s*:\s*"([^"]*)"', body)
            regions_match = re.search(r'"subject_regions"\s*:\s*\[(.*?)\]', body, re.DOTALL)
            subject_regions = (
                [r.strip().strip('"').strip() for r in regions_match.group(1).split(",")]
                if regions_match else []
            )
            target_region_match = re.search(r'"target_region"\s*:\s*"([^"]*)"', body)
            is_ha_match = re.search(r'"is_home_affairs"\s*:\s*(true|false)', body)
            target_region = (
                target_region_match.group(1).strip()
                if target_region_match and target_region_match.group(1).strip() != "null"
                else None
            )
            is_home_affairs = (
                is_ha_match.group(1) == "true" if is_ha_match else False
            )
            salvaged.append(
                ImpactItem(
                    cluster_index=idx,
                    **{dim: dims.get(dim, 0.0) for dim in DIMENSIONS},
                    rationale=rationale.group(1) if rationale else "",
                    display_category=category.group(1) if category else DEFAULT_DISPLAY_CATEGORY,
                    short_topic_name=short_name.group(1) if short_name else None,
                    topic_icon_key=icon.group(1) if icon else None,
                    subject_regions=subject_regions,
                    target_region=target_region,
                    is_home_affairs=is_home_affairs,
                )
            )
        return salvaged

    # ─── LOCAL MATH ──────────────────────────────────────────────────────────

    def _article_tier(self, article: Article) -> str:
        tier = self.source_tiers.get(article.source_name)
        if not tier and article.source_kind != "news":
            tier = article.source_kind
        if not tier and article.is_searched:
            tier = "active_search"
        return tier or "unknown"

    def _signal(self, cluster: ArticleCluster) -> tuple[float, list[str]]:
        articles = cluster.articles
        if not articles:
            return 0.0, ["empty_cluster"]
        organic = [article for article in articles if not article.is_searched]
        flags: list[str] = []

        sources = {article.source_name for article in articles}
        organic_sources = {article.source_name for article in organic}
        regions = {
            article.origin_region or self.source_regions.get(article.source_name)
            for article in articles
        } - {None}
        tier_values = [
            float(self.tier_scores.get(self._article_tier(article), self.tier_scores.get("unknown", 0.45)))
            for article in articles
        ]
        mean_tier = sum(tier_values) / len(tier_values)

        if len(organic_sources) <= 1:
            flags.append("single_source")
        if len(regions) <= 1 and len(articles) >= 2:
            flags.append("single_region")
        official_count = sum(
            1 for article in articles if article.is_official_source or article.source_kind != "news"
        )
        if official_count and official_count == len(articles):
            flags.append("official_only")
        elif official_count:
            flags.append("official_source_present")

        score = (
            0.45 * min(len(sources), 4) / 4
            + 0.30 * min(len(regions), 3) / 3
            + 0.25 * mean_tier
        )
        return _clamp01(score), flags

    def _composite(self, dims: dict[str, float], signal: float, weights: dict[str, float]) -> float:
        score = weights.get("signal", 0.0) * signal
        for dim in DIMENSIONS:
            score += weights.get(dim, 0.0) * (dims.get(dim, 0.0) / 10.0)
        return _clamp01(score)

    def _status(
        self,
        dims: dict[str, float],
        composite: float,
        flags: list[str],
        evaluated_by_llm: bool,
    ) -> tuple[str, list[str]]:
        constraints = [
            "只陈述来源文章明确支持的事实。",
            "单一来源或有争议的说法必须显式归因到来源。",
        ]
        if "official_only" in flags:
            constraints.append("官方来源的表述必须标注为官方立场，不要当作独立确认的事实。")
        if evaluated_by_llm and composite < self.suppress_floor:
            return "suppress", constraints
        if "single_source" in flags and dims.get("severity", 0.0) >= self.single_source_severity_review:
            return "seek_more_evidence", constraints
        if "official_only" in flags:
            return "needs_review", constraints
        if evaluated_by_llm and composite < self.review_floor:
            return "needs_review", constraints
        return "publishable", constraints

    def _build_assessment(
        self,
        cluster: ArticleCluster,
        item: ImpactItem | None,
        weights: dict[str, float],
    ) -> ImpactAssessment:
        signal, flags = self._signal(cluster)
        if item is None:
            dims = {dim: 0.0 for dim in DIMENSIONS}
            composite = signal  # deterministic, keyword-free degradation
            evaluated = False
            rationale = ""
            display_category = DEFAULT_DISPLAY_CATEGORY
            short_topic_name = None
            topic_icon_key = self.icon_allowlist[0] if self.icon_allowlist else None
            subject_regions = []
            target_region = None
            is_home_affairs = False
        else:
            dims = {dim: _clamp_dim(getattr(item, dim)) for dim in DIMENSIONS}
            composite = self._composite(dims, signal, weights)
            evaluated = True
            rationale = re.sub(r"\s+", " ", item.rationale or "").strip()[:60]
            display_category = normalize_display_category(item.display_category)
            short_topic_name = (item.short_topic_name or "").strip() or None
            topic_icon_key = (
                item.topic_icon_key
                if item.topic_icon_key in self.icon_allowlist
                else (self.icon_allowlist[0] if self.icon_allowlist else None)
            )
            subject_regions = _norm_regions(item.subject_regions)
            target_region = item.target_region
            is_home_affairs = bool(item.is_home_affairs)

        status, constraints = self._status(dims, composite, flags, evaluated)
        return ImpactAssessment(
            cluster_key=cluster_key(cluster),
            dims=dims,
            rationale=rationale,
            display_category=display_category,
            short_topic_name=short_topic_name,
            topic_icon_key=topic_icon_key,
            subject_regions=subject_regions,
            target_region=target_region,
            is_home_affairs=is_home_affairs,
            signal=signal,
            composite=composite,
            status=status,
            flags=flags,
            summary_constraints=constraints,
            evaluated_by_llm=evaluated,
            model=self.model if evaluated else None,
        )

    def _item_from_assessment(self, assessment: ImpactAssessment) -> ImpactItem | None:
        if not assessment.evaluated_by_llm:
            return None
        return ImpactItem(
            cluster_index=1,
            **{dim: assessment.dim(dim) for dim in DIMENSIONS},
            rationale=assessment.rationale,
            display_category=assessment.display_category or DEFAULT_DISPLAY_CATEGORY,
            short_topic_name=assessment.short_topic_name,
            topic_icon_key=assessment.topic_icon_key,
            subject_regions=assessment.subject_regions,
        )
