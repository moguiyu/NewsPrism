"""AI summarizer — generates cluster summaries with multi-source perspectives.

For each cluster:
- If multi-source: summary + per-source perspective bullets
- If single-source: clean summary paragraph only

Uses LiteLLM so any OpenAI-compatible provider works (DeepSeek recommended).

Layer: service (imports types, config; never imports repo or runtime)
"""
from __future__ import annotations
import json
import logging
import re
from pathlib import Path

import litellm
from pydantic import BaseModel, Field

from newsprism.config import Config
from newsprism.service.language import looks_like_chinese_text
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.types import ArticleCluster, ClusterSummary, PerspectiveGroup

logger = logging.getLogger(__name__)

litellm.set_verbose = False


def _extract_headline(summary_text: str) -> str:
    for line in summary_text.splitlines():
        match = re.match(r"\*\*(.+?)\*\*", line.strip())
        if match:
            return match.group(1)
    return ""


def _body_only(summary_text: str) -> str:
    lines = summary_text.splitlines()
    body_lines: list[str] = []
    headline_consumed = False
    for line in lines:
        stripped = line.strip()
        if not headline_consumed and re.match(r"\*\*(.+?)\*\*", stripped):
            headline_consumed = True
            continue
        if re.match(r"[•·\-\*]\s*【.+?】", stripped):
            continue
        body_lines.append(line)
    while body_lines and not body_lines[0].strip():
        body_lines.pop(0)
    while body_lines and not body_lines[-1].strip():
        body_lines.pop()
    return "\n".join(body_lines)


class PerspectiveItem(BaseModel):
    source: str = Field(description="The source name, exactly as provided.")
    perspective: str = Field(description="The unique perspective or angle from this source, in one sentence.")


class PerspectiveGroupItem(BaseModel):
    sources: list[str] = Field(
        default_factory=list,
        description="Exact source names grouped under the same perspective.",
    )
    perspective: str = Field(
        description="The shared distinctive perspective for this group of sources, in one sentence.",
    )


class StructuredSummary(BaseModel):
    headline: str = Field(description="A one-sentence headline summarizing the core event.")
    body: str = Field(description="2-4 sentences of objective, factual summary.")
    short_topic_name: str | None = Field(
        default=None,
        description="A concise 4-10 Chinese character topic label suitable for a navigation tab.",
    )
    topic_icon_key: str | None = Field(
        default=None,
        description="One hotspot icon key chosen from the provided allowlist.",
    )
    perspective_groups: list[PerspectiveGroupItem] = Field(
        default_factory=list,
        description="Distinct perspective groups. Each source must appear exactly once across groups.",
    )
    perspectives: list[PerspectiveItem] = Field(
        default_factory=list,
        description="Deprecated fallback: one perspective per source. Empty if unused."
    )


class BatchSummaryItem(BaseModel):
    index: int = Field(description="Zero-based index of the cluster in the batch.")
    headline: str
    body: str
    short_topic_name: str | None = None
    topic_icon_key: str | None = None
    perspective_groups: list[PerspectiveGroupItem] = Field(default_factory=list)


class BatchSummaryResponse(BaseModel):
    clusters: list[BatchSummaryItem]


class SummaryTranslation(BaseModel):
    headline: str = Field(description="English headline translated from the Chinese digest headline.")
    body: str = Field(description="English body translated from the Chinese digest body.")
    short_topic_name: str | None = Field(
        default=None,
        description="A concise English topic label suitable for navigation tabs.",
    )
    perspective_groups: list[PerspectiveGroupItem] = Field(
        default_factory=list,
        description="Perspective groups translated to English while preserving the exact source grouping.",
    )


class PositiveSummaryTranslation(BaseModel):
    headline_zh: str = Field(description="Simplified Chinese headline translated from the source-language positive story.")
    body_zh: str = Field(description="Simplified Chinese body translated from the source-language positive story.")
    headline_en: str = Field(description="English headline translated from the source-language positive story.")
    body_en: str = Field(description="English body translated from the source-language positive story.")


class LabelTranslation(BaseModel):
    translation: str = Field(description="Concise English translation for the provided Chinese label.")


class PositiveEnergyItem(BaseModel):
    cluster_index: int = Field(description="1-based cluster index from the provided candidate list.")
    good_fit: bool = Field(
        default=False,
        description="Whether the story is a strict fit for 今日正能量: cheerful, funny, happy, cute, heartwarming, or clearly uplifting.",
    )
    positive: bool = Field(description="Whether the story is generally positive or uplifting.")
    fun: bool = Field(description="Whether the story is generally fun, light, delightful, or entertaining.")
    low_conflict: bool = Field(description="Whether the story is low-conflict and not dominated by harm, war, disaster, scandal, or market stress.")
    confidence: float = Field(description="Confidence between 0 and 1.")
    reason: str = Field(description="Short Chinese reason for inclusion or exclusion.")


class PositiveEnergyBatch(BaseModel):
    items: list[PositiveEnergyItem] = Field(
        default_factory=list,
        description="One classification item per candidate story.",
    )




class StorylineRelationItem(BaseModel):
    left_index: int = Field(description="Left cluster index from the provided candidate pair list.")
    right_index: int = Field(description="Right cluster index from the provided candidate pair list.")
    relation: str = Field(
        description="One of: same_core_storyline, same_direct_spillover_storyline, not_related.",
    )
    confidence: float = Field(description="Confidence between 0 and 1.")


class StorylineRelationBatch(BaseModel):
    relations: list[StorylineRelationItem] = Field(
        default_factory=list,
        description="One relation decision per candidate pair.",
    )


class Summarizer:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.model = cfg.litellm_model
        self.api_key = cfg.litellm_api_key
        self.base_url = cfg.litellm_base_url
        self.temperature = cfg.summarizer.get("temperature", 0.3)
        self.max_tokens = cfg.summarizer.get("max_tokens", 1200)
        self.completion_compat_kwargs = completion_compat_kwargs(self.model, self.base_url)
        self.hot_topics_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
        self.topic_icon_allowlist = self.hot_topics_cfg.get(
            "icon_allowlist",
            ["globe", "war", "trade", "chip", "ai", "energy"],
        )

        style_file = Path(cfg.summarizer.get("style_guide_file", "config/style-guide.md"))
        self.style_guide = style_file.read_text(encoding="utf-8") if style_file.exists() else ""

    def summarize_all(self, clusters: list[ArticleCluster]) -> list[ClusterSummary]:
        results: list[ClusterSummary] = []
        for cluster in clusters:
            try:
                result = self._summarize_cluster(cluster)
                results.append(result)
            except Exception as exc:
                logger.error("Summarization failed for cluster '%s': %s", cluster.topic_category, exc)
        return results

    def summarize_all_batch(self, clusters: list[ArticleCluster]) -> list[ClusterSummary]:
        """Summarise all clusters in a single LLM call. Falls back to summarize_all on failure."""
        if not clusters:
            return []
        try:
            return self._batch_summarize(clusters)
        except Exception as exc:
            logger.error("Batch summarisation failed (%s) — falling back to per-cluster", exc)
            return self.summarize_all(clusters)

    def _batch_summarize(self, clusters: list[ArticleCluster]) -> list[ClusterSummary]:
        """Build one prompt for all clusters and parse BatchSummaryResponse."""
        cluster_blocks: list[str] = []
        for i, cluster in enumerate(clusters):
            articles_block = self._format_articles(cluster)
            quality_block = self._quality_prompt_block(cluster)
            sources_joined = "、".join(cluster.sources)
            block_parts = [f"== 集群 {i} | {cluster.topic_category} | 来源：{sources_joined} =="]
            if quality_block:
                block_parts.append(quality_block)
            block_parts.append(articles_block)
            cluster_blocks.append("\n".join(block_parts))

        separator = "\n\n---\n\n"
        clusters_text = separator.join(cluster_blocks)

        prompt = (
            f"为以下 {len(clusters)} 个新闻事件集群分别生成摘要。\n\n"
            "输出 JSON 格式：\n"
            "{\"clusters\": [{\"index\": 0, \"headline\": \"...\", \"body\": \"...\", "
            "\"short_topic_name\": \"...\", \"topic_icon_key\": \"...\", "
            "\"perspective_groups\": [{\"sources\": [\"来源A\", \"来源B\"], \"perspective\": \"一句话共享视角\"}, ...]}, ...]}\n\n"
            "每个集群的规则：\n"
            "- headline: 一句话点明核心事件\n"
            "- body: 2–4句客观事实总结，不要在 body 里列举来源视角\n"
            "- short_topic_name: 4-10个中文字符的短专题名，适合作为导航tab标签，不要包含“热点专题-”前缀\n"
            f"- topic_icon_key: 只能从以下列表中选择一个最贴切的键名：{', '.join(self.topic_icon_allowlist)}\n"
            "- perspective_groups: 去重后的视角分组数组。"
            "每个元素为 {\"sources\": [\"来源A\"], \"perspective\": \"一句话视角\"}\n"
            "额外要求：\n"
            "1. 所有来源必须且只能在 perspective_groups 中出现一次。\n"
            "2. 当多个来源视角实质相同，即使来自不同国家，也应合并到同一 group；只有明显不同的立场、强调点或国家视角才拆成不同 group。\n"
            "3. 单一来源的集群 perspective_groups 为 []。\n"
            "4. index 字段必须与输入顺序一致（从 0 开始）。\n"
            "5. 只输出 JSON，不要解释。\n\n"
            "---\n\n"
            f"{clusters_text}"
        )

        response = litellm.completion(
            model=self.model,
            api_key=self.api_key,
            api_base=self.base_url,
            messages=[
                {"role": "system", "content": self.style_guide},
                {"role": "user", "content": prompt},
            ],
            temperature=self.temperature,
            max_tokens=min(len(clusters) * 800, 16000),
            response_format={"type": "json_object"},
            **self.completion_compat_kwargs,
        )

        content = response.choices[0].message.content or ""
        batch_result = BatchSummaryResponse.model_validate_json(content)

        items_by_index = {item.index: item for item in batch_result.clusters}

        results: list[ClusterSummary] = []
        for i, cluster in enumerate(clusters):
            item = items_by_index.get(i)
            if item is None:
                logger.warning(
                    "Batch response missing index %d ('%s'); falling back to per-cluster call",
                    i,
                    cluster.topic_category,
                )
                try:
                    results.append(self._summarize_cluster(cluster))
                except Exception as exc:
                    logger.error(
                        "Per-cluster fallback also failed for '%s': %s",
                        cluster.topic_category,
                        exc,
                    )
                continue

            headline_clean = item.headline.strip().strip("*")
            summary_text = f"**{headline_clean}**\n\n{item.body}"
            grouped_perspectives = self._normalize_perspective_groups(
                cluster,
                item.perspective_groups,
                [],
            )
            perspectives = {
                source_name: group.perspective
                for group in grouped_perspectives
                for source_name in group.sources
            }
            parsed_short = item.short_topic_name
            parsed_icon = item.topic_icon_key

            results.append(
                ClusterSummary(
                    cluster=cluster,
                    summary=summary_text,
                    perspectives=perspectives,
                    grouped_perspectives=grouped_perspectives,
                    short_topic_name=parsed_short,
                    topic_icon_key=parsed_icon,
                    storyline_key=cluster.storyline_key,
                    storyline_name=cluster.storyline_name,
                    storyline_role=cluster.storyline_role,
                    storyline_confidence=cluster.storyline_confidence,
                    storyline_state=cluster.storyline_state,
                    storyline_timeline=list(cluster.storyline_timeline),
                    storyline_membership_status=cluster.storyline_membership_status,
                    storyline_anchor_labels=list(cluster.storyline_anchor_labels),
                    macro_topic_key=cluster.macro_topic_key,
                    macro_topic_name=cluster.macro_topic_name,
                    macro_topic_icon_key=cluster.macro_topic_icon_key,
                    macro_topic_member_count=cluster.macro_topic_member_count,
                    quality_report=cluster.quality_report,
                    quality_status=cluster.quality_report.status if cluster.quality_report else "unknown",
                    quality_score=cluster.quality_report.overall_score if cluster.quality_report else 0.0,
                    quality_flags=list(cluster.quality_report.flags) if cluster.quality_report else [],
                    confirmed_claims=list(cluster.quality_report.confirmed_claims) if cluster.quality_report else [],
                    contested_claims=list(cluster.quality_report.contested_claims) if cluster.quality_report else [],
                    evidence_summary=cluster.quality_report.evidence_summary if cluster.quality_report else "",
                )
            )

        return results

    def translate_report_content(
        self,
        summaries: list[ClusterSummary],
        hot_topics: list[dict[str, object]] | None = None,
        focus_storylines: list[dict[str, object]] | None = None,
    ) -> bool:
        if not summaries:
            return False

        hot_topics = hot_topics or []
        focus_storylines = focus_storylines or []
        label_cache: dict[str, str] = {}

        try:
            for summary in summaries:
                self._translate_cluster_summary(summary)

            for summary in summaries:
                if summary.storyline_name:
                    summary.storyline_name_en = label_cache.setdefault(
                        summary.storyline_name,
                        self._translate_short_label(summary.storyline_name),
                    )
                if summary.macro_topic_name:
                    summary.macro_topic_name_en = label_cache.setdefault(
                        summary.macro_topic_name,
                        self._translate_short_label(summary.macro_topic_name),
                    )

            for family in hot_topics:
                family_name = str(family.get("macro_topic_name") or family.get("storyline_name") or "").strip()
                if not family_name:
                    continue
                translation = label_cache.setdefault(family_name, self._translate_short_label(family_name))
                family["macro_topic_name_en"] = translation
                family["storyline_name_en"] = translation

            for family in focus_storylines:
                family_name = str(family.get("storyline_name") or "").strip()
                if not family_name:
                    continue
                translation = label_cache.setdefault(family_name, self._translate_short_label(family_name))
                family["storyline_name_en"] = translation

            return True
        except Exception as exc:
            logger.warning("English translation failed; rendering Chinese-only report: %s", exc)
            self._clear_translated_report_content(summaries, hot_topics, focus_storylines)
            return False

    def normalize_positive_energy_summaries(
        self,
        summaries: list[ClusterSummary],
        include_english: bool = False,
    ) -> list[ClusterSummary]:
        """Ensure positive-energy primary fields are Chinese; drop untranslatable source-language items."""
        normalized: list[ClusterSummary] = []
        for summary in summaries:
            if self._summary_has_chinese_primary(summary):
                if include_english and not summary.summary_en:
                    try:
                        self._translate_cluster_summary(summary)
                    except Exception as exc:
                        logger.warning(
                            "Positive-energy English translation failed; keeping Chinese-only item '%s': %s",
                            _extract_headline(summary.summary) or summary.cluster.topic_category,
                            exc,
                        )
                normalized.append(summary)
                continue

            try:
                self._translate_positive_energy_summary(summary, include_english=include_english)
            except Exception as exc:
                logger.warning(
                    "Positive-energy story omitted because Chinese normalization failed: source=%s headline=%s error=%s",
                    summary.cluster.sources[0] if summary.cluster.sources else "",
                    _extract_headline(summary.summary) or summary.cluster.topic_category,
                    exc,
                )
                continue
            normalized.append(summary)
        return normalized


    def classify_storyline_relations(
        self,
        pair_candidates: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        if not pair_candidates:
            return []

        batch_size = max(1, int(self.hot_topics_cfg.get("storyline_relation_batch_size", 8)))
        relations: list[dict[str, object]] = []
        for start in range(0, len(pair_candidates), batch_size):
            batch = pair_candidates[start:start + batch_size]
            prompt = self._build_storyline_relation_prompt(batch)
            parsed = self._request_storyline_relations(
                prompt=prompt,
                max_tokens=min(self.max_tokens, 1600),
                stage_label=f"storyline relation batch {start + 1}-{start + len(batch)}",
            )
            by_pair = {
                (item.left_index, item.right_index): item
                for item in (parsed.relations if parsed is not None else [])
            }
            for candidate in batch:
                pair = (int(candidate["left_index"]), int(candidate["right_index"]))
                item = by_pair.get(pair)
                if item is None:
                    continue
                relation = item.relation.strip()
                if relation not in {
                    "same_core_storyline",
                    "same_direct_spillover_storyline",
                    "not_related",
                }:
                    relation = "not_related"
                relations.append(
                    {
                        "left_index": pair[0],
                        "right_index": pair[1],
                        "relation": relation,
                        "confidence": max(0.0, min(1.0, float(item.confidence))),
                    }
                )
        return relations

    def classify_positive_energy(self, summaries: list[ClusterSummary]) -> list[dict[str, object]]:
        """Classify main-lane summaries for the 今日正能量 section."""
        if not summaries:
            return []

        prompt = self._build_positive_energy_prompt(summaries)
        try:
            content = self._json_completion(
                system_prompt=(
                    "You classify daily news digests for a light positive highlights section. "
                    "Return compact JSON only."
                ),
                user_prompt=prompt,
                max_tokens=min(self.max_tokens, max(500, len(summaries) * 130)),
                temperature=0.1,
            )
            parsed = self._parse_positive_energy_content(content)
            if parsed is None:
                logger.warning("Retrying positive-energy classification with compact JSON prompt after parse failure")
                retry_content = self._json_completion(
                    system_prompt=(
                        "You classify daily news digests for a light positive highlights section. "
                        "Return compact JSON only."
                    ),
                    user_prompt=f"{prompt}\n\n最后要求：只输出紧凑 JSON，不要解释，不要 Markdown。",
                    max_tokens=min(self.max_tokens, max(500, len(summaries) * 130)),
                    temperature=0.1,
                )
                parsed = self._parse_positive_energy_content(retry_content)
            if parsed is None:
                logger.error("Failed to parse positive-energy classification output")
                return []
        except Exception as exc:
            logger.warning("Positive-energy classification failed; section will be omitted: %s", exc)
            return []

        valid_indices = set(range(1, len(summaries) + 1))
        normalized: list[dict[str, object]] = []
        seen: set[int] = set()
        for item in parsed.items:
            if item.cluster_index not in valid_indices or item.cluster_index in seen:
                continue
            seen.add(item.cluster_index)
            normalized.append(
                {
                    "cluster_index": item.cluster_index,
                    "good_fit": bool(item.good_fit),
                    "positive": bool(item.positive),
                    "fun": bool(item.fun),
                    "low_conflict": bool(item.low_conflict),
                    "confidence": max(0.0, min(1.0, float(item.confidence))),
                    "reason": item.reason.strip(),
                }
            )
        return normalized

    def name_storyline(self, anchor_clusters: list[ArticleCluster]) -> str | None:
        if not anchor_clusters:
            return None
        anchor_lines = []
        for idx, cluster in enumerate(anchor_clusters[:4], 1):
            lead_title = cluster.articles[0].title if cluster.articles else cluster.topic_category
            anchor_lines.append(
                f"[{idx}] topic={cluster.topic_category}\n"
                f"headline={lead_title}"
            )
        prompt = (
            "下面是一组属于同一主线事件的核心锚点新闻。\n"
            "请为这条 storyline 生成一个 4-10 个中文字符的短名称，用于热点 tab。\n"
            "要求：\n"
            "1. 名称必须稳定、概括主线，不要使用完整长标题。\n"
            "2. 不要加“热点专题-”前缀。\n"
            "3. 只输出 JSON：{\"storyline_name\":\"...\"}\n\n"
            "核心锚点：\n"
            + "\n\n".join(anchor_lines)
        )
        try:
            content = self._macro_topic_completion(prompt, min(self.max_tokens, 300))
            extracted = self._extract_json_object(content) or content
            match = re.search(r'"storyline_name"\s*:\s*"([^"]+)"', extracted)
            if not match:
                return None
            candidate = self._normalize_macro_topic_name(match.group(1), anchor_clusters[0])
            return candidate or None
        except Exception as exc:
            logger.warning("Storyline naming failed; falling back to deterministic name: %s", exc)
            return None

    def _normalize_macro_topic_name(self, value: str | None, cluster: ArticleCluster) -> str:
        candidate = re.sub(r"\s+", "", (value or "").strip())
        candidate = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", candidate)
        candidate = candidate[:10].strip(" -:：，,、。.；;")
        if candidate:
            return candidate
        fallback = cluster.articles[0].title if cluster.articles else cluster.topic_category
        fallback = re.sub(r"\s+", "", fallback)[:10].strip(" -:：，,、。.；;")
        return fallback or "焦点话题"

    def _request_storyline_relations(
        self,
        prompt: str,
        max_tokens: int,
        stage_label: str,
    ) -> StorylineRelationBatch | None:
        content = self._macro_topic_completion(prompt, max_tokens)
        parsed = self._parse_storyline_relation_content(content)
        if parsed is not None:
            return parsed

        logger.warning("Retrying %s with compact JSON prompt after parse failure", stage_label)
        retry_prompt = (
            f"{prompt}\n\n"
            "最后要求：只输出紧凑 JSON，不要解释，不要 Markdown，不要换行装饰。"
        )
        retry_content = self._macro_topic_completion(retry_prompt, max_tokens)
        parsed = self._parse_storyline_relation_content(retry_content)
        if parsed is not None:
            return parsed

        salvaged = self._salvage_storyline_relations(retry_content or content)
        if salvaged:
            logger.warning(
                "Salvaged %d relation assignments from malformed %s output",
                len(salvaged),
                stage_label,
            )
            return StorylineRelationBatch(relations=salvaged)

        logger.error("Failed to parse %s output after retry", stage_label)
        return None

    def _macro_topic_completion(self, prompt: str, max_tokens: int) -> str:
        response = litellm.completion(
            model=self.model,
            api_key=self.api_key,
            api_base=self.base_url,
            messages=[
                {"role": "system", "content": self.style_guide},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            **self.completion_compat_kwargs,
        )
        return response.choices[0].message.content or ""


    def _parse_storyline_relation_content(self, content: str) -> StorylineRelationBatch | None:
        if not content.strip():
            return None
        try:
            return StorylineRelationBatch.model_validate_json(content)
        except Exception:
            extracted = self._extract_json_object(content)
            if extracted and extracted != content:
                try:
                    return StorylineRelationBatch.model_validate_json(extracted)
                except Exception:
                    return None
        return None

    def _parse_positive_energy_content(self, content: str) -> PositiveEnergyBatch | None:
        if not content.strip():
            return None
        try:
            return PositiveEnergyBatch.model_validate_json(content)
        except Exception:
            extracted = self._extract_json_object(content)
            if extracted and extracted != content:
                try:
                    return PositiveEnergyBatch.model_validate_json(extracted)
                except Exception:
                    return None
        return None

    def _parse_positive_summary_translation_content(self, content: str) -> PositiveSummaryTranslation | None:
        if not content.strip():
            return None
        try:
            return PositiveSummaryTranslation.model_validate_json(content)
        except Exception:
            extracted = self._extract_json_object(content)
            if extracted and extracted != content:
                try:
                    return PositiveSummaryTranslation.model_validate_json(extracted)
                except Exception:
                    return None
        return None

    def _extract_json_object(self, content: str) -> str | None:
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        return content[start:end + 1]


    def _salvage_storyline_relations(self, content: str) -> list[StorylineRelationItem]:
        if not content.strip():
            return []
        pattern = re.compile(
            r'\{\s*"left_index"\s*:\s*(?P<left_index>\d+)'
            r'.*?"right_index"\s*:\s*(?P<right_index>\d+)'
            r'.*?"relation"\s*:\s*"(?P<relation>[^"]*)"'
            r'.*?"confidence"\s*:\s*(?P<confidence>[0-9]*\.?[0-9]+)',
            re.DOTALL,
        )
        salvaged: list[StorylineRelationItem] = []
        seen: set[tuple[int, int]] = set()
        valid_relations = {
            "same_core_storyline",
            "same_direct_spillover_storyline",
            "not_related",
        }
        for match in pattern.finditer(content):
            pair = (int(match.group("left_index")), int(match.group("right_index")))
            if pair in seen:
                continue
            seen.add(pair)
            relation = match.group("relation").strip()
            if relation not in valid_relations:
                relation = "not_related"
            salvaged.append(
                StorylineRelationItem(
                    left_index=pair[0],
                    right_index=pair[1],
                    relation=relation,
                    confidence=max(0.0, min(1.0, float(match.group("confidence")))),
                )
            )
        return salvaged


    def _build_positive_energy_prompt(self, summaries: list[ClusterSummary]) -> str:
        candidates = []
        for index, summary in enumerate(summaries, 1):
            headline = _extract_headline(summary.summary) or summary.cluster.topic_category
            body = _body_only(summary.summary)
            candidates.append(
                {
                    "cluster_index": index,
                    "topic_category": summary.cluster.topic_category,
                    "headline": headline,
                    "body": body[:500],
                    "sources": list(summary.cluster.sources),
                    "article_titles": [article.title for article in summary.cluster.articles[:4]],
                }
            )
        return (
            "请从每日新闻主线中判断哪些适合放入“今日正能量”轻量板块。\n"
            "选择标准：\n"
            "1. good_fit=true 只给真正让读者感到开心、可爱、好笑、暖心、治愈、轻松愉快或明确振奋的新闻。\n"
            "2. positive=true 表示故事整体积极、温暖、鼓舞或有明确好消息；仅仅“没有坏消息”不算。\n"
            "3. fun=true 表示故事轻松、有趣、文化娱乐、体育、科学发现、生活方式或让读者会心一笑。\n"
            "4. low_conflict=true 表示不是战争、灾害、伤亡、严重政治冲突、犯罪、制裁、诉讼、市场暴跌或丑闻主导。\n"
            "5. 中性、严肃、程序性、政策、商业、地缘政治、诉讼、事故、灾害、犯罪、市场压力类新闻，即使 low_conflict，也必须 good_fit=false。\n"
            "6. 可以全部 false；不要为了凑数强行标 good_fit 或 positive。\n"
            "7. reason 用不超过 24 个中文字符说明原因。\n"
            "只输出 JSON：{\"items\":[{\"cluster_index\":1,\"good_fit\":true,\"positive\":true,\"fun\":false,\"low_conflict\":true,\"confidence\":0.83,\"reason\":\"...\"}]}\n\n"
            f"候选新闻：\n{json.dumps(candidates, ensure_ascii=False, indent=2)}"
        )

    def _summarize_cluster(self, cluster: ArticleCluster) -> ClusterSummary:
        articles_block = self._format_articles(cluster)
        prompt = self._build_prompt(cluster, articles_block)

        response = litellm.completion(
            model=self.model,
            api_key=self.api_key,
            api_base=self.base_url,
            messages=[
                {"role": "system", "content": self.style_guide},
                {"role": "user", "content": prompt},
            ],
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            response_format={"type": "json_object"},
            **self.completion_compat_kwargs,
        )

        try:
            # Parse the returned JSON string into our Pydantic model
            content = response.choices[0].message.content or ""
            parsed = StructuredSummary.model_validate_json(content)

            headline_clean = parsed.headline.strip().strip("*")
            summary_text = f"**{headline_clean}**\n\n{parsed.body}"
            grouped_perspectives = self._normalize_perspective_groups(
                cluster,
                parsed.perspective_groups,
                parsed.perspectives,
            )
            perspectives = {
                source_name: group.perspective
                for group in grouped_perspectives
                for source_name in group.sources
            }
        except Exception as e:
            logger.error("Failed to parse structured output for '%s': %s", cluster.topic_category, e)
            # Fallback for unexpected failures:
            summary_text = response.choices[0].message.content or ""
            perspectives = {}
            grouped_perspectives = []
            parsed = StructuredSummary(headline="", body="")

        logger.debug("Summarized cluster '%s': %d chars", cluster.topic_category, len(summary_text))
        return ClusterSummary(
            cluster=cluster,
            summary=summary_text,
            perspectives=perspectives,
            grouped_perspectives=grouped_perspectives,
            short_topic_name=parsed.short_topic_name,
            topic_icon_key=parsed.topic_icon_key,
            storyline_key=cluster.storyline_key,
            storyline_name=cluster.storyline_name,
            storyline_role=cluster.storyline_role,
            storyline_confidence=cluster.storyline_confidence,
            storyline_state=cluster.storyline_state,
            storyline_timeline=list(cluster.storyline_timeline),
            storyline_membership_status=cluster.storyline_membership_status,
            storyline_anchor_labels=list(cluster.storyline_anchor_labels),
            macro_topic_key=cluster.macro_topic_key,
            macro_topic_name=cluster.macro_topic_name,
            macro_topic_icon_key=cluster.macro_topic_icon_key,
            macro_topic_member_count=cluster.macro_topic_member_count,
            quality_report=cluster.quality_report,
            quality_status=cluster.quality_report.status if cluster.quality_report else "unknown",
            quality_score=cluster.quality_report.overall_score if cluster.quality_report else 0.0,
            quality_flags=list(cluster.quality_report.flags) if cluster.quality_report else [],
            confirmed_claims=list(cluster.quality_report.confirmed_claims) if cluster.quality_report else [],
            contested_claims=list(cluster.quality_report.contested_claims) if cluster.quality_report else [],
            evidence_summary=cluster.quality_report.evidence_summary if cluster.quality_report else "",
        )

    def _translate_cluster_summary(self, summary: ClusterSummary) -> None:
        headline = _extract_headline(summary.summary)
        body = _body_only(summary.summary)
        if not headline or not body:
            raise ValueError(f"Missing structured Chinese summary for '{summary.cluster.topic_category}'")

        perspective_groups = [
            {
                "sources": list(group.sources),
                "perspective": group.perspective,
            }
            for group in summary.grouped_perspectives
        ]
        prompt_payload = {
            "headline": headline,
            "body": body,
            "short_topic_name": summary.short_topic_name or "",
            "perspective_groups": perspective_groups,
        }
        prompt = (
            "Translate the following Chinese news digest JSON into English.\n"
            "Rules:\n"
            "1. Preserve facts exactly; do not add or remove information.\n"
            "2. Keep the exact same JSON shape.\n"
            "3. Keep every source name in perspective_groups exactly unchanged.\n"
            "4. Preserve the exact source grouping and ordering in perspective_groups.\n"
            "5. short_topic_name should be concise natural English suitable for a tab label.\n"
            "6. Return compact JSON only.\n\n"
            f"{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
        )
        content = self._json_completion(
            system_prompt="You are a precise translator for structured news digests.",
            user_prompt=prompt,
            max_tokens=min(self.max_tokens, 1400),
            temperature=0.1,
        )
        parsed = SummaryTranslation.model_validate_json(content)

        headline_clean = parsed.headline.strip().strip("*")
        body_clean = parsed.body.strip()
        if not headline_clean or not body_clean:
            raise ValueError(f"Incomplete English translation for '{summary.cluster.topic_category}'")

        translated_groups = self._align_translated_perspective_groups(
            summary,
            parsed.perspective_groups,
        )

        summary.summary_en = f"**{headline_clean}**\n\n{body_clean}"
        summary.grouped_perspectives_en = translated_groups
        if parsed.short_topic_name and parsed.short_topic_name.strip():
            summary.short_topic_name_en = self._clean_short_label(parsed.short_topic_name)
        elif summary.short_topic_name:
            summary.short_topic_name_en = self._translate_short_label(summary.short_topic_name)

    def _summary_has_chinese_primary(self, summary: ClusterSummary) -> bool:
        headline = _extract_headline(summary.summary)
        body = _body_only(summary.summary)
        return looks_like_chinese_text(f"{headline}\n{body}")

    def _translate_positive_energy_summary(self, summary: ClusterSummary, include_english: bool) -> None:
        headline = _extract_headline(summary.summary)
        if not headline and summary.cluster.articles:
            headline = summary.cluster.articles[0].title
        body = _body_only(summary.summary)
        if not body and summary.cluster.articles:
            body = summary.cluster.articles[0].content
        source_language = self._source_language_for_summary(summary)
        prompt_payload = {
            "source_language": source_language,
            "headline": headline,
            "body": body[:1200],
            "source": summary.cluster.sources[0] if summary.cluster.sources else "",
        }
        prompt = (
            "Normalize this 今日正能量 positive-highlight story for a bilingual Chinese report.\n"
            "Return JSON with exactly these keys: headline_zh, body_zh, headline_en, body_en.\n"
            "Rules:\n"
            "1. headline_zh and body_zh must be Simplified Chinese.\n"
            "2. headline_en and body_en must be natural English.\n"
            "3. Preserve facts exactly; do not add context, claims, dates, names, or numbers.\n"
            "4. Keep body_zh to 1-2 short sentences and under 120 Chinese characters.\n"
            "5. Keep body_en to 1-2 short sentences.\n"
            "6. Return compact JSON only.\n\n"
            f"{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
        )
        content = self._json_completion(
            system_prompt="You translate short positive news highlights without adding facts.",
            user_prompt=prompt,
            max_tokens=min(self.max_tokens, 700),
            temperature=0.1,
        )
        parsed = self._parse_positive_summary_translation_content(content)
        if parsed is None:
            raise ValueError("Could not parse positive summary translation JSON")

        headline_zh = self._clean_positive_text(parsed.headline_zh).strip("*")
        body_zh = self._clean_positive_text(parsed.body_zh)
        headline_en = self._clean_positive_text(parsed.headline_en).strip("*")
        body_en = self._clean_positive_text(parsed.body_en)
        if not headline_zh or not body_zh or not looks_like_chinese_text(f"{headline_zh}\n{body_zh}"):
            raise ValueError("Translated positive summary does not contain a Chinese primary digest")
        if include_english and (not headline_en or not body_en):
            raise ValueError("Translated positive summary is missing English fields")

        summary.summary = f"**{headline_zh}**\n\n{body_zh}"
        summary.summary_en = f"**{headline_en}**\n\n{body_en}" if include_english else None

    def _source_language_for_summary(self, summary: ClusterSummary) -> str:
        source_languages = {source.name: source.language for source in self.cfg.sources}
        for article in summary.cluster.articles:
            language = source_languages.get(article.source_name)
            if language:
                return language
        return "unknown"

    def _clean_positive_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _align_translated_perspective_groups(
        self,
        summary: ClusterSummary,
        parsed_groups: list[PerspectiveGroupItem],
    ) -> list[PerspectiveGroup]:
        """Keep source grouping stable even when the translator drifts.

        A single malformed perspective_groups translation should not disable the
        entire English report. The renderer can still use the translated
        headline/body, while perspective rows keep the original source groups.
        """
        if not summary.grouped_perspectives:
            return []

        if len(parsed_groups) != len(summary.grouped_perspectives):
            logger.warning(
                "Perspective group count changed during translation for '%s'; preserving original source grouping",
                summary.cluster.topic_category,
            )

        aligned: list[PerspectiveGroup] = []
        for index, zh_group in enumerate(summary.grouped_perspectives):
            translated_text = ""
            if index < len(parsed_groups):
                parsed_group = parsed_groups[index]
                if list(parsed_group.sources) != list(zh_group.sources):
                    logger.warning(
                        "Perspective grouping changed during translation for '%s'; preserving original sources",
                        summary.cluster.topic_category,
                    )
                translated_text = self._clean_perspective_text(parsed_group.perspective)

            aligned.append(
                PerspectiveGroup(
                    sources=list(zh_group.sources),
                    perspective=translated_text or self._fallback_perspective_text_en(),
                )
            )
        return aligned

    def _translate_short_label(self, label: str) -> str:
        normalized = self._clean_short_label(label)
        if not normalized:
            raise ValueError("Cannot translate empty label")
        if re.fullmatch(r"[A-Za-z0-9&/\- +]+", normalized):
            return normalized

        prompt = (
            "Translate this short Chinese news topic label into concise natural English.\n"
            "Rules:\n"
            "1. Keep it short, usually 2-5 words.\n"
            "2. Make it suitable for a navigation tab.\n"
            "3. Do not add explanations or punctuation decoration.\n"
            "4. Return JSON only: {\"translation\": \"...\"}\n\n"
            f"label: {normalized}"
        )
        content = self._json_completion(
            system_prompt="You translate short news labels into concise English.",
            user_prompt=prompt,
            max_tokens=120,
            temperature=0.1,
        )
        parsed = LabelTranslation.model_validate_json(content)
        translation = self._clean_short_label(parsed.translation)
        if not translation:
            raise ValueError(f"Empty translated label for '{normalized}'")
        return translation

    def _clear_translated_report_content(
        self,
        summaries: list[ClusterSummary],
        hot_topics: list[dict[str, object]],
        focus_storylines: list[dict[str, object]],
    ) -> None:
        for summary in summaries:
            summary.summary_en = None
            summary.grouped_perspectives_en = []
            summary.short_topic_name_en = None
            summary.storyline_name_en = None
            summary.macro_topic_name_en = None
        for family in hot_topics:
            family.pop("macro_topic_name_en", None)
            family.pop("storyline_name_en", None)
        for family in focus_storylines:
            family.pop("storyline_name_en", None)

    def _clean_short_label(self, text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip()).strip(" -:：，,、。.；;")

    def _json_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float = 0.1,
    ) -> str:
        response = litellm.completion(
            model=self.model,
            api_key=self.api_key,
            api_base=self.base_url,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            **self.completion_compat_kwargs,
        )
        return response.choices[0].message.content or ""

    def _normalize_perspective_groups(
        self,
        cluster: ArticleCluster,
        raw_groups: list[PerspectiveGroupItem],
        legacy_items: list[PerspectiveItem],
    ) -> list[PerspectiveGroup]:
        if not cluster.is_multi_source:
            return []

        valid_sources = list(dict.fromkeys(cluster.sources))
        valid_source_set = set(valid_sources)
        legacy_by_source: dict[str, str] = {}

        for item in legacy_items:
            source = item.source.strip()
            perspective = self._clean_perspective_text(item.perspective)
            if source in valid_source_set and perspective and source not in legacy_by_source:
                legacy_by_source[source] = perspective

        normalized: list[PerspectiveGroup] = []
        assigned_sources: set[str] = set()

        for group in raw_groups:
            clean_sources: list[str] = []
            seen_in_group: set[str] = set()
            for raw_source in group.sources:
                source = raw_source.strip()
                if (
                    source in valid_source_set
                    and source not in assigned_sources
                    and source not in seen_in_group
                ):
                    clean_sources.append(source)
                    seen_in_group.add(source)

            perspective = self._clean_perspective_text(group.perspective)
            if not clean_sources or not perspective:
                continue

            normalized.append(PerspectiveGroup(sources=clean_sources, perspective=perspective))
            assigned_sources.update(clean_sources)

        for source in valid_sources:
            if source in assigned_sources:
                continue
            fallback_perspective = legacy_by_source.get(source) or self._fallback_perspective_text()
            normalized.append(PerspectiveGroup(sources=[source], perspective=fallback_perspective))
            assigned_sources.add(source)

        return normalized

    def _clean_perspective_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _fallback_perspective_text(self) -> str:
        return "该来源报道与主摘要角度接近，未稳定提炼出可单列的差异化视角。"

    def _fallback_perspective_text_en(self) -> str:
        return "This source reports a similar angle to the main summary."

    def _format_articles(self, cluster: ArticleCluster) -> str:
        lines: list[str] = []
        for i, article in enumerate(cluster.articles, 1):
            lines.append(
                f"[{i}] 来源：{article.source_name}\n"
                f"标题：{article.title}\n"
                f"内容：{article.content[:3000]}\n"
                f"链接：{article.url}\n"
            )
        return "\n".join(lines)

    def _build_prompt(self, cluster: ArticleCluster, articles_block: str) -> str:
        source_list = "、".join(cluster.sources)
        is_multi = cluster.is_multi_source
        quality_block = self._quality_prompt_block(cluster)

        # Explicitly ask for JSON
        if is_multi:
            instruction = (
                f"以下是来自 {len(cluster.sources)} 个不同来源（{source_list}）关于同一话题（{cluster.topic_category}）的报道。\n"
                "请按照编辑风格要求，生成一段多视角摘要，并【必须输出纯 JSON 格式】，包含以下字段：\n"
                "- headline: 粗体标题（一句话点明核心事件）\n"
                "- body: 2–4句客观总结\n"
                "- short_topic_name: 4-10个中文字符的短专题名，适合作为导航tab标签，不要包含“热点专题-”前缀\n"
                f"- topic_icon_key: 只能从以下列表中选择一个最贴切的键名：{', '.join(self.topic_icon_allowlist)}\n"
                "- perspective_groups: 数组，包含去重后的 distinct perspective groups。每个元素为 {sources: ['来源A', '来源B'], perspective: '一句话共享视角'}\n"
                "额外要求：\n"
                "1. headline 和 body 只负责概括事件事实，不要在 body 里重复列举来源视角。\n"
                "2. 所有来源必须且只能在 perspective_groups 中出现一次。\n"
                "3. 当多个来源的视角实质相同，即使来自不同国家，也应合并到同一 group。\n"
                "4. 只有明显不同的立场、强调点或国家视角，才拆成不同 group。\n"
                "5. 只输出 JSON，不要解释。"
            )
        else:
            instruction = (
                f"以下是来自 {source_list} 关于话题（{cluster.topic_category}）的报道。\n"
                "请按照编辑风格要求，生成一段事实摘要，并【必须输出纯 JSON 格式】，包含以下字段：\n"
                "- headline: 粗体标题（一句话点明核心事件）\n"
                "- body: 2–4句客观总结\n"
                "- short_topic_name: 4-10个中文字符的短专题名，适合作为导航tab标签，不要包含“热点专题-”前缀\n"
                f"- topic_icon_key: 只能从以下列表中选择一个最贴切的键名：{', '.join(self.topic_icon_allowlist)}\n"
                "- perspective_groups: 空数组 []\n"
                "额外要求：headline 和 body 只负责概括事件事实；只输出 JSON，不要解释。"
            )

        return f"{instruction}\n\n{quality_block}\n\n{articles_block}"

    def _quality_prompt_block(self, cluster: ArticleCluster) -> str:
        report = getattr(cluster, "quality_report", None)
        decision = getattr(cluster, "quality_decision", None)
        if report is None:
            return ""
        constraints = list(getattr(decision, "summary_constraints", []) or [])
        confirmed = list(getattr(report, "confirmed_claims", []) or [])[:6]
        contested = list(getattr(report, "contested_claims", []) or [])[:6]
        payload = {
            "quality_status": report.status,
            "quality_score": round(float(report.overall_score), 3),
            "evidence_summary": report.evidence_summary,
            "confirmed_claims": confirmed,
            "contested_claims": contested,
            "summary_constraints": constraints,
        }
        return (
            "质量评估约束：\n"
            "请严格依据以下质量评估结果写摘要；confirmed_claims 可以作为事实陈述，"
            "contested_claims 必须显式归因或避免定论。\n"
            f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
        )


    def _build_storyline_relation_prompt(self, pair_candidates: list[dict[str, object]]) -> str:
        pair_lines: list[str] = []
        for candidate in pair_candidates:
            left_cluster = candidate["left_cluster"]
            right_cluster = candidate["right_cluster"]
            left_title = left_cluster.articles[0].title if left_cluster.articles else left_cluster.topic_category
            right_title = right_cluster.articles[0].title if right_cluster.articles else right_cluster.topic_category
            left_history = candidate.get("left_history", {}) or {}
            right_history = candidate.get("right_history", {}) or {}
            pair_lines.append(
                f"[{int(candidate['left_index'])},{int(candidate['right_index'])}]\n"
                f"left_topic={left_cluster.topic_category}\n"
                f"left_title={left_title}\n"
                f"left_history_storyline={left_history.get('storyline_name', '')}\n"
                f"right_topic={right_cluster.topic_category}\n"
                f"right_title={right_title}\n"
                f"right_history_storyline={right_history.get('storyline_name', '')}\n"
                f"signal_overlap={candidate.get('signal_overlap', 0)}\n"
                f"semantic_similarity={float(candidate.get('similarity', 0.0)):.3f}"
            )

        return (
            "下面是一组已经完成事件级聚类的候选事件对。"
            "请判断每一对是否属于同一个更高层级的 storyline。\n\n"
            "关系定义：\n"
            "1. same_core_storyline: 两个条目属于同一个核心事件/政策/灾害/选举/危机主线。\n"
            "2. same_direct_spillover_storyline: 其中一个条目是另一个核心事件的直接外溢或直接后果，如航运、市场、监管、交通、外交即时反应。\n"
            "3. not_related: 仅有宽泛地域、行业、主题相似，或属于更远的二级外溢，不应归为同一 storyline。\n"
            "4. precision-first: 宁可判 not_related，也不要因为大区域相似或泛主题背景就硬合并。\n"
            "5. history_storyline 只是辅助线索，不能单独决定相关性。\n"
            "6. confidence 给出 0 到 1 之间的小数。\n"
            "7. 只输出 JSON，格式如下：\n"
            "{\n"
            '  "relations": [\n'
            '    {"left_index": 1, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.82}\n'
            "  ]\n"
            "}\n\n"
            "候选事件对：\n"
            + "\n\n".join(pair_lines)
        )
