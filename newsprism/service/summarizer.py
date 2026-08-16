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
import unicodedata
from pathlib import Path

import litellm
from pydantic import BaseModel, Field

from newsprism.config import Config
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.service.llm_telemetry import tracked_completion
from newsprism.service.perspectives import canonicalize_perspective_groups
from newsprism.types import ArticleCluster, ClusterSummary, PerspectiveGroup, is_real_article

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


class GroundedSummaryRewrite(BaseModel):
    headline: str = ""
    body: str = ""


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


class BatchTranslationItem(BaseModel):
    index: int = Field(description="Zero-based index of the summary in the batch.")
    headline: str = ""
    body: str = ""
    short_topic_name: str | None = None
    perspective_groups: list[PerspectiveGroupItem] = Field(default_factory=list)


class BatchTranslationResponse(BaseModel):
    items: list[BatchTranslationItem] = Field(default_factory=list)
    labels: dict[str, str] = Field(default_factory=dict)


class StorylineRelationItem(BaseModel):
    left_index: int = Field(description="Left cluster index from the provided candidate pair list.")
    right_index: int = Field(description="Right cluster index from the provided candidate pair list.")
    relation: str = Field(
        description=(
            "One of: same_core_storyline, same_direct_spillover_storyline, "
            "same_conflict_different_event, not_related."
        ),
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
        self.telemetry_enabled = getattr(cfg, "llm_telemetry_enabled", False)
        self.temperature = cfg.summarizer.get("temperature", 0.3)
        self.max_tokens = cfg.summarizer.get("max_tokens", 1200)
        self.article_content_chars = max(200, int(cfg.summarizer.get("article_content_chars", 1600)))
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

        tracked = tracked_completion(
            stage="summary_batch",
            enabled=self.telemetry_enabled,
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
            item_count=len(clusters),
            **self.completion_compat_kwargs,
        )

        content = tracked.choices[0].message.content or ""
        try:
            batch_result = BatchSummaryResponse.model_validate_json(content)
        except Exception:
            salvaged_items = self._salvage_batch_summary_items(content)
            if not salvaged_items:
                tracked.mark("malformed_json")
                raise
            logger.warning(
                "Batch summary JSON was malformed; salvaged %d/%d items",
                len(salvaged_items),
                len(clusters),
            )
            batch_result = BatchSummaryResponse(clusters=salvaged_items)

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
            summary = ClusterSummary(
                cluster=cluster,
                summary=summary_text,
                perspectives=perspectives,
                grouped_perspectives=grouped_perspectives,
                short_topic_name=item.short_topic_name,
                topic_icon_key=item.topic_icon_key,
                **self._cluster_metadata_kwargs(cluster),
            )
            self._enforce_numeric_grounding(summary)
            results.append(summary)

        return results

    def _cluster_metadata_kwargs(self, cluster: ArticleCluster) -> dict[str, object]:
        """Storyline/impact fields copied from the cluster onto its summary."""
        impact = getattr(cluster, "impact", None)
        regions = {
            article.origin_region
            for article in cluster.articles
            if article.origin_region and is_real_article(article)
        }
        evidence_summary = (
            f"{len(cluster.sources)} 个来源、{len(regions)} 个地区参与评估。"
            if impact is not None
            else ""
        )
        return {
            "storyline_key": cluster.storyline_key,
            "storyline_name": cluster.storyline_name,
            "storyline_role": cluster.storyline_role,
            "storyline_confidence": cluster.storyline_confidence,
            "storyline_state": cluster.storyline_state,
            "storyline_timeline": list(cluster.storyline_timeline),
            "storyline_membership_status": cluster.storyline_membership_status,
            "storyline_anchor_labels": list(cluster.storyline_anchor_labels),
            "macro_topic_key": cluster.macro_topic_key,
            "macro_topic_name": cluster.macro_topic_name,
            "macro_topic_icon_key": cluster.macro_topic_icon_key,
            "macro_topic_member_count": cluster.macro_topic_member_count,
            "impact": impact,
            "display_category": getattr(cluster, "display_category", None),
            "quality_status": impact.status if impact is not None else "unknown",
            "quality_score": impact.composite if impact is not None else 0.0,
            "quality_flags": list(impact.flags) if impact is not None else [],
            "evidence_summary": evidence_summary,
            "organic_unique_regions": getattr(cluster, "organic_unique_regions", 0),
            "organic_unique_sources": getattr(cluster, "organic_unique_sources", 0),
        }

    def translate_report_content(
        self,
        summaries: list[ClusterSummary],
        hot_topics: list[dict[str, object]] | None = None,
        focus_storylines: list[dict[str, object]] | None = None,
    ) -> bool:
        """Translate the whole report in one batched LLM call (chunked if large)."""
        if not summaries:
            return False

        hot_topics = hot_topics or []
        focus_storylines = focus_storylines or []

        labels: set[str] = set()
        for summary in summaries:
            if summary.storyline_name:
                labels.add(summary.storyline_name)
            if summary.macro_topic_name:
                labels.add(summary.macro_topic_name)
        for family in hot_topics + focus_storylines:
            family_name = str(family.get("macro_topic_name") or family.get("storyline_name") or "").strip()
            if family_name:
                labels.add(family_name)
            family_name_full = str(
                family.get("macro_topic_name_full") or family.get("storyline_name_full") or ""
            ).strip()
            if family_name_full:
                labels.add(family_name_full)

        try:
            label_map: dict[str, str] = {}
            chunk_size = 18
            for start in range(0, len(summaries), chunk_size):
                chunk = summaries[start:start + chunk_size]
                chunk_labels = sorted(labels) if start == 0 else []
                label_map.update(self._translate_summary_chunk(chunk, chunk_labels))

            for summary in summaries:
                if summary.storyline_name and summary.storyline_name in label_map:
                    summary.storyline_name_en = label_map[summary.storyline_name]
                if summary.macro_topic_name and summary.macro_topic_name in label_map:
                    summary.macro_topic_name_en = label_map[summary.macro_topic_name]
            for family in hot_topics:
                family_name = str(family.get("macro_topic_name") or family.get("storyline_name") or "").strip()
                if family_name and family_name in label_map:
                    family["macro_topic_name_en"] = label_map[family_name]
                    family["storyline_name_en"] = label_map[family_name]
                family_name_full = str(
                    family.get("macro_topic_name_full") or family.get("storyline_name_full") or ""
                ).strip()
                if family_name_full and family_name_full in label_map:
                    family["macro_topic_name_full_en"] = label_map[family_name_full]
                    family["storyline_name_full_en"] = label_map[family_name_full]
            for family in focus_storylines:
                family_name = str(family.get("storyline_name") or "").strip()
                if family_name and family_name in label_map:
                    family["storyline_name_en"] = label_map[family_name]
            return True
        except Exception as exc:
            logger.warning("English translation failed; rendering Chinese-only report: %s", exc)
            self._clear_translated_report_content(summaries, hot_topics, focus_storylines)
            return False

    def _translate_summary_chunk(
        self,
        summaries: list[ClusterSummary],
        labels: list[str],
    ) -> dict[str, str]:
        """Translate one chunk of summaries + shared labels; apply results in place."""
        payload_items = []
        for index, summary in enumerate(summaries):
            payload_items.append(
                {
                    "index": index,
                    "headline": _extract_headline(summary.summary),
                    "body": _body_only(summary.summary),
                    "short_topic_name": summary.short_topic_name or "",
                    "perspective_groups": [
                        {"sources": list(group.sources), "perspective": group.perspective}
                        for group in summary.grouped_perspectives
                    ],
                }
            )
        prompt = (
            "Translate this Chinese news digest JSON into English.\n"
            "Rules:\n"
            "1. Preserve facts exactly; do not add or remove information.\n"
            "2. Return the same items array with the same index values.\n"
            "3. Keep every source name in perspective_groups exactly unchanged; "
            "preserve grouping and ordering.\n"
            "4. short_topic_name: concise natural English suitable for a tab label.\n"
            "5. labels: translate each Chinese label to a concise English tab label "
            "(2-5 words), returned as {\"原文\": \"English\"}.\n"
            "6. Return compact JSON only: {\"items\": [...], \"labels\": {...}}.\n\n"
            f"{json.dumps({'items': payload_items, 'labels': labels}, ensure_ascii=False)}"
        )
        content = self._json_completion(
            system_prompt="You are a precise translator for structured news digests.",
            user_prompt=prompt,
            max_tokens=min(16000, 600 + len(summaries) * 420 + len(labels) * 20),
            temperature=0.1,
        )
        parsed = BatchTranslationResponse.model_validate_json(content)
        items_by_index = {item.index: item for item in parsed.items}
        for index, summary in enumerate(summaries):
            item = items_by_index.get(index)
            if item is None:
                logger.warning(
                    "Translation batch missing index %d ('%s'); keeping Chinese-only",
                    index,
                    summary.cluster.topic_category,
                )
                continue
            headline_clean = item.headline.strip().strip("*")
            body_clean = item.body.strip()
            if not headline_clean or not body_clean:
                continue
            summary.summary_en = f"**{headline_clean}**\n\n{body_clean}"
            summary.grouped_perspectives_en = self._align_translated_perspective_groups(
                summary,
                item.perspective_groups,
            )
            self._enforce_translated_numeric_grounding(summary)
            if item.short_topic_name and item.short_topic_name.strip():
                summary.short_topic_name_en = self._clean_short_label(item.short_topic_name)
        return {key: self._clean_short_label(value) for key, value in parsed.labels.items() if value}

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
                max_tokens=min(16000, 600 + len(batch) * 180),
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
                    "same_conflict_different_event",
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
            content = self._macro_topic_completion(prompt, min(self.max_tokens, 300), stage="storyline_name")
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

    def _macro_topic_completion(
        self,
        prompt: str,
        max_tokens: int,
        stage: str = "storyline_relation",
    ) -> str:
        tracked = tracked_completion(
            stage=stage,
            enabled=self.telemetry_enabled,
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
        return tracked.choices[0].message.content or ""


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

    def _extract_json_object(self, content: str) -> str | None:
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        return content[start:end + 1]

    def _salvage_batch_summary_items(self, content: str) -> list[BatchSummaryItem]:
        """Recover complete indexed items from a truncated batch-summary JSON."""
        text = (content or "").strip()
        if not text:
            return []
        items: list[BatchSummaryItem] = []
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
            if isinstance(obj, dict) and isinstance(obj.get("index"), int):
                try:
                    items.append(BatchSummaryItem.model_validate(obj))
                except Exception:
                    pass
            pos = start + max(1, end)
        return items

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
            "same_conflict_different_event",
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


    def _summarize_cluster(self, cluster: ArticleCluster) -> ClusterSummary:
        articles_block = self._format_articles(cluster)
        prompt = self._build_prompt(cluster, articles_block)

        tracked = tracked_completion(
            stage="summary_single",
            enabled=self.telemetry_enabled,
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
            item_count=1,
            **self.completion_compat_kwargs,
        )

        try:
            # Parse the returned JSON string into our Pydantic model
            content = tracked.choices[0].message.content or ""
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
        summary = ClusterSummary(
            cluster=cluster,
            summary=summary_text,
            perspectives=perspectives,
            grouped_perspectives=grouped_perspectives,
            short_topic_name=parsed.short_topic_name,
            topic_icon_key=parsed.topic_icon_key,
            **self._cluster_metadata_kwargs(cluster),
        )
        self._enforce_numeric_grounding(summary)
        return summary

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
            family.pop("macro_topic_name_full_en", None)
            family.pop("storyline_name_full_en", None)
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
        stage: str = "translation",
    ) -> str:
        tracked = tracked_completion(
            stage=stage,
            enabled=self.telemetry_enabled,
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
        return tracked.choices[0].message.content or ""

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

        return canonicalize_perspective_groups(normalized)

    def _clean_perspective_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _fallback_perspective_text(self) -> str:
        return "该来源报道与主摘要角度接近，未稳定提炼出可单列的差异化视角。"

    def _fallback_perspective_text_en(self) -> str:
        return "This source reports a similar angle to the main summary."

    # Keep the numeric token deliberately small and deterministic.  The
    # surrounding language is not part of the token: ``4人`` and ``4 dead``
    # should compare through their numeric value, while a source sentence can
    # still be removed as a unit when a value is not grounded.
    _NUMERIC_VALUE = r"\d[\d,]*(?:\.\d+)?"
    _NUMERIC_CLAIM_PATTERN = re.compile(
        rf"(?:[$€£¥￥]\s*)?{_NUMERIC_VALUE}"
        rf"(?:\s*(?:[-–—:比]\s*){_NUMERIC_VALUE})?"
        rf"(?:\s*(?:千|천|万|만|亿|억|兆|조)(?:\s*{_NUMERIC_VALUE})?)?"
        r"(?:\s*(?:%|％|万亿美元|亿美元|亿元|万元|美元|欧元|人民币|"
        r"票|项|例|病例|人|名|家|国|枚|架|艘|倍|岁|年|月|日|건|명))?"
    )

    _NUMERIC_CONTEXT_PATTERN = re.compile(
        r"(?:[$€£¥￥%％]|万亿美元|亿美元|亿元|万元|美元|欧元|人民币|"
        r"票|项|例|病例|人|名|家|国|枚|架|艘|倍|岁|年|月|日|건|명|"
        r"死亡|死者|遇难|伤亡|受伤|伤者|确诊|病例|"
        r"\b(?:dead|death(?:s)?|died|kill(?:ed|s)?|casualt(?:y|ies)|"
        r"injur(?:ed|y|ies)|people|persons|cases|patients|votes?|"
        r"years?|months?|days?|percent|points?|items?|ships?|aircraft)\b)",
        re.IGNORECASE,
    )

    _NUMERIC_SCALE_PATTERN = re.compile(
        rf"(?<!\d)(?P<major>{_NUMERIC_VALUE})"
        r"(?P<scale>千|천|万|만|亿|억|兆|조)"
        rf"(?P<minor>{_NUMERIC_VALUE})?"
    )

    _NUMERIC_SCALE_FACTORS = {
        "千": 1e3,
        "천": 1e3,
        "万": 1e4,
        "만": 1e4,
        "亿": 1e8,
        "억": 1e8,
        "兆": 1e12,
        "조": 1e12,
    }

    @classmethod
    def _numeric_claims(cls, text: str) -> list[str]:
        claims: list[str] = []
        text = text or ""
        for match in cls._NUMERIC_CLAIM_PATTERN.finditer(text):
            value = match.group(0).strip()
            digits = re.sub(r"\D", "", value)
            context_window = text[max(0, match.start() - 12): match.end() + 24]
            has_context = bool(
                cls._NUMERIC_CONTEXT_PATTERN.search(value)
                or cls._NUMERIC_CONTEXT_PATTERN.search(context_window)
                or re.search(r"[-–—:比]", value)
            )
            if len(digits) >= 2 or has_context:
                claims.append(value)
        return list(dict.fromkeys(claims))

    @staticmethod
    def _normalized_claim_text(text: str) -> str:
        return (
            unicodedata.normalize("NFKC", text or "")
            .casefold()
            .replace("–", "-")
            .replace("—", "-")
            .replace("％", "%")
            .replace(",", "")
            .replace(" ", "")
        )

    @classmethod
    def _numeric_values(cls, text: str) -> list[float]:
        """Return canonical numeric values, including CJK/Korean scale forms.

        ``3千500`` and ``3천500`` both mean 3,500.  Keeping this conversion
        separate from the textual claim list lets the existing currency logic
        remain exact while allowing cross-script news evidence to ground the
        same fact.
        """
        text = unicodedata.normalize("NFKC", text or "")
        values: list[float] = []
        covered_spans: list[tuple[int, int]] = []
        for match in cls._NUMERIC_SCALE_PATTERN.finditer(text):
            try:
                major = float(match.group("major").replace(",", ""))
                minor_text = match.group("minor")
                minor = float(minor_text.replace(",", "")) if minor_text else 0.0
                factor = cls._NUMERIC_SCALE_FACTORS[match.group("scale")]
            except (KeyError, ValueError):
                continue
            values.append(major * factor + minor)
            covered_spans.append(match.span())

        for match in re.finditer(r"(?<![\d\w])\d[\d,]*(?:\.\d+)?(?!\d)", text):
            if any(start <= match.start() and match.end() <= end for start, end in covered_spans):
                continue
            try:
                values.append(float(match.group(0).replace(",", "")))
            except ValueError:
                continue
        return values

    # Currency/scale equivalence for the numeric-grounding check. This lets a
    # Chinese claim like "114亿美元" match an English source that says
    # "$11.4 billion" — same magnitude, different currency notation — instead
    # of failing a literal substring comparison.
    _CURRENCY_SYMBOL_MAP = {
        "$": "USD",
        "€": "EUR",
        "£": "GBP",
        "¥": "CNY",
        "￥": "CNY",
    }

    _CURRENCY_WORD_MAP = {
        "usd": "USD",
        "dollar": "USD",
        "dollars": "USD",
        "eur": "EUR",
        "euro": "EUR",
        "euros": "EUR",
        "cny": "CNY",
        "rmb": "CNY",
        "yuan": "CNY",
        "gbp": "GBP",
        "pound": "GBP",
        "pounds": "GBP",
        "美元": "USD",
        "美金": "USD",
        "欧元": "EUR",
        "人民币": "CNY",
        "元": "CNY",
        "英镑": "GBP",
    }

    _SCALE_WORD_MAP = {
        "trillion": 1e12,
        "billion": 1e9,
        "million": 1e6,
        "thousand": 1e3,
        "bn": 1e9,
        "mn": 1e6,
        "万亿": 1e12,
        "亿": 1e8,
        "万": 1e4,
    }

    # Relative tolerance for treating two currency magnitudes from independent
    # sources as "the same fact" (e.g. AP says $11.4B, another outlet rounds
    # to $11.5B for the same underlying figure).
    _CURRENCY_MAGNITUDE_TOLERANCE = 0.08

    _MONEY_AMOUNT_PATTERN = re.compile(
        r"(?P<sym>[$€£¥￥])?\s*"
        r"(?P<num>\d[\d,]*(?:\.\d+)?)\s*"
        r"(?P<scale>trillion|billion|million|thousand|bn|mn|万亿|亿|万)?\s*"
        r"(?P<cur>USD|usd|dollars?|EUR|eur|euros?|CNY|cny|RMB|rmb|yuan|GBP|gbp|"
        r"pounds?|美元|美金|欧元|人民币|英镑|元)?",
        re.IGNORECASE,
    )

    @classmethod
    def _money_amounts(cls, text: str) -> list[tuple[float, str]]:
        """Extract (magnitude, currency-class) pairs from arbitrary text.

        Only substrings with an explicit currency signal (a symbol like `$`
        or a currency word like "美元"/"dollars") count — a bare number with
        just a scale word (e.g. "5亿人") is not treated as a currency amount.
        """
        amounts: list[tuple[float, str]] = []
        for match in cls._MONEY_AMOUNT_PATTERN.finditer(text or ""):
            sym = match.group("sym")
            cur = match.group("cur")
            currency = None
            if sym:
                currency = cls._CURRENCY_SYMBOL_MAP.get(sym)
            elif cur:
                currency = cls._CURRENCY_WORD_MAP.get(cur.lower()) or cls._CURRENCY_WORD_MAP.get(cur)
            if not currency:
                continue
            try:
                value = float(match.group("num").replace(",", ""))
            except ValueError:
                continue
            scale = match.group("scale")
            if scale:
                factor = cls._SCALE_WORD_MAP.get(scale.lower()) or cls._SCALE_WORD_MAP.get(scale)
                if factor:
                    value *= factor
            amounts.append((value, currency))
        return amounts

    @classmethod
    def _claim_supported_by_money_amounts(
        cls,
        claim: str,
        evidence_amounts: list[tuple[float, str]],
    ) -> bool:
        if not evidence_amounts:
            return False
        claim_amounts = cls._money_amounts(claim)
        if not claim_amounts:
            return False
        for value, currency in claim_amounts:
            for evidence_value, evidence_currency in evidence_amounts:
                if evidence_currency != currency:
                    continue
                denom = max(abs(evidence_value), abs(value)) or 1.0
                if abs(evidence_value - value) / denom <= cls._CURRENCY_MAGNITUDE_TOLERANCE:
                    return True
        return False

    @staticmethod
    def _bare_digits_in_evidence(claim: str, evidence: str) -> bool:
        """Check whether a claim's numeric value occurs in source evidence.

        This deliberately accepts one-digit claims when their surrounding
        claim has a unit/context (``4人`` vs ``4 dead``).  It also compares
        canonical values so Korean/CJK forms such as ``3천500``/``3千500``
        match ``3500`` rather than relying on a literal substring.
        """
        claim_values = Summarizer._numeric_values(claim)
        evidence_values = Summarizer._numeric_values(evidence)
        for claim_value in claim_values:
            for evidence_value in evidence_values:
                if abs(claim_value - evidence_value) <= max(1e-9, abs(claim_value) * 1e-9):
                    return True

        digits = re.sub(r"[^\d]", "", unicodedata.normalize("NFKC", claim))
        if not digits:
            return False
        normalized_evidence = unicodedata.normalize("NFKC", evidence or "")
        matches = list(re.finditer(rf"(?<!\d){re.escape(digits)}(?!\d)", normalized_evidence))
        if not matches:
            return False
        # One-digit claims need a nearby unit/context marker; otherwise a
        # date, version, or list index can accidentally ground a casualty or
        # count claim (for example ``4人`` against an unrelated ``2024``).
        if len(digits) == 1:
            return any(
                Summarizer._NUMERIC_CONTEXT_PATTERN.search(
                    normalized_evidence[max(0, match.start() - 12): match.end() + 24]
                )
                for match in matches
            )
        return True

    def _unsupported_numeric_claims(
        self,
        cluster: ArticleCluster,
        summary_text: str,
    ) -> list[str]:
        raw_evidence_text = "\n".join(
            f"{article.title}\n{article.content}"
            for article in cluster.articles
            if is_real_article(article)
        )
        evidence = self._normalized_claim_text(raw_evidence_text)
        evidence_money = self._money_amounts(raw_evidence_text)
        unsupported = [
            claim
            for claim in self._numeric_claims(summary_text)
            if self._normalized_claim_text(claim) not in evidence
            and not self._claim_supported_by_money_amounts(claim, evidence_money)
            and not self._bare_digits_in_evidence(claim, raw_evidence_text)
        ]
        vote_pattern = re.compile(r"\d[\d,]*\s*[-–—]\s*\d[\d,]*")
        source_vote_counts = {
            self._normalized_claim_text(match.group(0))
            for article in cluster.articles
            if is_real_article(article)
            for match in vote_pattern.finditer(f"{article.title}\n{article.content}")
        }
        if len(source_vote_counts) > 1:
            for claim in self._numeric_claims(summary_text):
                if vote_pattern.search(claim) and claim not in unsupported:
                    unsupported.append(claim)
        return unsupported

    def _rewrite_grounded_summary(
        self,
        summary: ClusterSummary,
        unsupported: list[str],
    ) -> str | None:
        evidence = self._format_articles(summary.cluster)
        prompt = (
            "Rewrite this news headline and body once so every number, date, percentage, vote count, "
            "and money value is directly supported by the supplied source text. Do not add new facts. "
            "If sources conflict, omit the value or attribute the disagreement. Return JSON only as "
            "{\"headline\":\"...\",\"body\":\"...\"}.\n\n"
            f"Unsupported values: {unsupported}\nCurrent summary:\n{summary.summary}\n\nSources:\n{evidence}"
        )
        try:
            tracked = tracked_completion(
                stage="summary_rewrite",
                enabled=self.telemetry_enabled,
                model=self.model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=500,
                response_format={"type": "json_object"},
                item_count=1,
                **self.completion_compat_kwargs,
            )
            parsed = GroundedSummaryRewrite.model_validate_json(
                tracked.choices[0].message.content or ""
            )
            if not parsed.headline.strip() or not parsed.body.strip():
                return None
            return f"**{parsed.headline.strip().strip('*')}**\n\n{parsed.body.strip()}"
        except Exception as exc:
            logger.warning("Numeric grounding rewrite failed for '%s': %s", summary.cluster.topic_category, exc)
            return None

    # These are output-only fallbacks.  They intentionally contain no numeric
    # token and never expose the old ``有关数字`` placeholder to readers.
    _UNSUPPORTED_VALUE_PLACEHOLDER = "有关数字"  # forbidden output marker; input detection only
    _SAFE_HEADLINE_FALLBACK = "相关报道：具体数字有待进一步核实"
    _SAFE_BODY_FALLBACK = "报道披露了相关进展，但具体数字仍待进一步核实。"
    _SAFE_HEADLINE_FALLBACK_EN = "Related report: specific figures remain unverified"
    _SAFE_BODY_FALLBACK_EN = "The report describes the development, while specific figures remain unverified."

    _NUMERIC_PLACEHOLDER_PATTERN = re.compile(
        r"有关数字|\bcertain\s+number\b",
        re.IGNORECASE,
    )
    _ORPHAN_NUMERIC_SENTENCE_PATTERN = re.compile(
        rf"^\s*(?:[$€£¥￥]\s*)?{_NUMERIC_VALUE}"
        rf"(?:\s*(?:千|천|万|만|亿|억|兆|조)(?:\s*{_NUMERIC_VALUE})?)?"
        r"(?:\s*(?:%|％|万亿美元|亿美元|亿元|万元|美元|欧元|人民币|"
        r"票|项|例|病例|人|名|家|国|枚|架|艘|倍|岁|年|月|日|건|명|"
        r"dead|deaths?|people|cases|injured?))?"
        r"\s*[.,。!?！？]\s*$",
        re.IGNORECASE,
    )
    _LEADING_NUMERIC_FRAGMENT_PATTERN = re.compile(
        rf"^\s*(?:[$€£¥￥]\s*)?{_NUMERIC_VALUE}"
        rf"(?:\s*(?:千|천|万|만|亿|억|兆|조)(?:\s*{_NUMERIC_VALUE})?)?"
        r"(?:\s*(?:%|％|万亿美元|亿美元|亿元|万元|美元|欧元|人民币|"
        r"票|项|例|病例|人|名|家|国|枚|架|艘|倍|岁|年|月|日|건|명|"
        r"dead|deaths?|people|cases|injured?))?"
        r"\s*[.,。!?！？]",
        re.IGNORECASE,
    )
    _MALFORMED_NUMERIC_REMNANT_PATTERN = re.compile(
        r"(?:超|超过|约|近|达|至少|至多|致|导致|造成)\s*"
        r"(?:例|病例|人|名|死亡|受伤|%|％|[，,。.!！？?])"
        r"|\b(?:kills?|killed|dead|deaths?|injured?)\s*[,.;:]",
        re.IGNORECASE,
    )

    @classmethod
    def _split_sentences(cls, text: str) -> list[str]:
        return [
            sentence.strip()
            for sentence in re.split(r"(?<=[。！？])\s*|(?<=[.!?])(?:\s+|$)", text or "")
            if sentence.strip()
        ]

    @classmethod
    def _numeric_safety_violations(cls, text: str) -> list[str]:
        """Find output shapes that must never reach a publication boundary."""
        violations: list[str] = []
        if cls._NUMERIC_PLACEHOLDER_PATTERN.search(text or ""):
            violations.append("numeric_placeholder")

        body = _body_only(text or "")
        if cls._LEADING_NUMERIC_FRAGMENT_PATTERN.search(body):
            violations.append("leading_numeric_fragment")
        for sentence in cls._split_sentences(body):
            if cls._ORPHAN_NUMERIC_SENTENCE_PATTERN.fullmatch(sentence):
                violations.append("orphan_numeric_fragment")

        if re.match(r"^\s*[，,、。.!！？?]", body):
            violations.append("malformed_sentence_start")
        if cls._MALFORMED_NUMERIC_REMNANT_PATTERN.search(text or ""):
            violations.append("malformed_numeric_remnant")
        return list(dict.fromkeys(violations))

    @classmethod
    def _strip_unsupported_values(cls, text: str, unsupported: list[str]) -> str:
        result = text
        for value in sorted(unsupported, key=len, reverse=True):
            if value and value in result:
                result = result.replace(value, "")
        result = cls._NUMERIC_PLACEHOLDER_PATTERN.sub("", result)
        result = re.sub(r"\s+([，,。.!！？?%％])", r"\1", result)
        result = re.sub(r"([，,、])\s*[，,、]+", r"\1", result)
        return re.sub(r"\s+", " ", result).strip(" *，,、。.!！？?;；:：")

    @classmethod
    def _safe_headline_without_unsupported_values(
        cls,
        headline: str,
        unsupported: list[str],
        language: str = "zh",
    ) -> str:
        if language == "en":
            return cls._SAFE_HEADLINE_FALLBACK_EN

        candidate = cls._strip_unsupported_values(headline, unsupported)
        if not candidate:
            return cls._SAFE_HEADLINE_FALLBACK

        # Removing a quantity from phrases such as “致4人死亡” or “超3500例”
        # can leave a grammatical-looking but semantically broken remnant.
        if cls._MALFORMED_NUMERIC_REMNANT_PATTERN.search(candidate):
            for marker in ("导致", "造成", "致", "超过", "超", "约", "近", "达"):
                marker_index = candidate.find(marker)
                if marker_index > 1:
                    candidate = candidate[:marker_index].rstrip(" ，,、:：")
                    break

        candidate = candidate.strip(" *，,、。.!！？?;；:：")
        if len(candidate) < 2 or cls._NUMERIC_PLACEHOLDER_PATTERN.search(candidate):
            return cls._SAFE_HEADLINE_FALLBACK
        return f"{candidate}，具体数字有待进一步核实"

    @classmethod
    def _safe_body_without_unsupported_values(
        cls,
        body: str,
        unsupported: list[str],
        language: str = "zh",
    ) -> str:
        kept: list[str] = []
        for sentence in cls._split_sentences(body):
            if any(value and value in sentence for value in unsupported):
                continue
            if cls._NUMERIC_PLACEHOLDER_PATTERN.search(sentence):
                continue
            if cls._ORPHAN_NUMERIC_SENTENCE_PATTERN.fullmatch(sentence):
                continue
            kept.append(sentence)

        separator = " " if language == "en" else ""
        grounded_body = separator.join(kept).strip()
        if not grounded_body:
            return cls._SAFE_BODY_FALLBACK_EN if language == "en" else cls._SAFE_BODY_FALLBACK
        return grounded_body

    @classmethod
    def _safe_numeric_fallback(
        cls,
        text: str,
        unsupported: list[str],
        language: str = "zh",
    ) -> str:
        headline = _extract_headline(text)
        body = _body_only(text)
        safe_headline = cls._safe_headline_without_unsupported_values(
            headline,
            unsupported,
            language=language,
        )
        safe_body = cls._safe_body_without_unsupported_values(
            body,
            unsupported,
            language=language,
        )
        candidate = f"**{safe_headline}**\n\n{safe_body}"
        if cls._numeric_safety_violations(candidate):
            fallback_headline = (
                cls._SAFE_HEADLINE_FALLBACK_EN
                if language == "en"
                else cls._SAFE_HEADLINE_FALLBACK
            )
            fallback_body = (
                cls._SAFE_BODY_FALLBACK_EN
                if language == "en"
                else cls._SAFE_BODY_FALLBACK
            )
            return f"**{fallback_headline}**\n\n{fallback_body}"
        return candidate

    def _remove_unsupported_numeric_sentences(
        self,
        summary: ClusterSummary,
        unsupported: list[str],
    ) -> str:
        return self._safe_numeric_fallback(summary.summary, unsupported, language="zh")

    def _enforce_numeric_grounding(self, summary: ClusterSummary) -> None:
        unsupported = self._unsupported_numeric_claims(summary.cluster, summary.summary)
        violations = self._numeric_safety_violations(summary.summary)
        if not unsupported and not violations:
            return
        rewritten = self._rewrite_grounded_summary(summary, unsupported) if unsupported else None
        if (
            rewritten
            and not self._unsupported_numeric_claims(summary.cluster, rewritten)
            and not self._numeric_safety_violations(rewritten)
        ):
            summary.summary = rewritten
            summary.quality_flags.append("numeric_grounding_rewritten")
            return
        summary.summary = self._remove_unsupported_numeric_sentences(summary, unsupported)
        summary.quality_status = "needs_review"
        if "unsupported_numeric_claim" not in summary.quality_flags:
            if unsupported:
                summary.quality_flags.append("unsupported_numeric_claim")
        if violations and "numeric_safety_failed" not in summary.quality_flags:
            summary.quality_flags.append("numeric_safety_failed")
        summary.contested_claims.extend(
            claim for claim in unsupported if claim not in summary.contested_claims
        )

    def _enforce_translated_numeric_grounding(self, summary: ClusterSummary) -> None:
        """Apply the same numeric and malformed-output gate to English text."""
        if not summary.summary_en:
            return
        translated_text = summary.summary_en
        unsupported = self._unsupported_numeric_claims(summary.cluster, translated_text)
        violations = self._numeric_safety_violations(translated_text)
        perspective_text = "\n".join(
            group.perspective for group in summary.grouped_perspectives_en
        )
        perspective_violations = self._numeric_safety_violations(perspective_text)
        if not unsupported and not violations and not perspective_violations:
            return

        summary.summary_en = self._safe_numeric_fallback(
            translated_text,
            unsupported,
            language="en",
        )
        if perspective_violations:
            for group in summary.grouped_perspectives_en:
                if self._numeric_safety_violations(group.perspective):
                    group.perspective = self._fallback_perspective_text_en()
        summary.quality_status = "needs_review"
        if unsupported and "unsupported_numeric_claim" not in summary.quality_flags:
            summary.quality_flags.append("unsupported_numeric_claim")
            summary.contested_claims.extend(
                claim for claim in unsupported if claim not in summary.contested_claims
            )
        if "numeric_safety_failed" not in summary.quality_flags:
            summary.quality_flags.append("numeric_safety_failed")

    def _format_articles(self, cluster: ArticleCluster) -> str:
        lines: list[str] = []
        article_index = 0
        for article in cluster.articles:
            if not is_real_article(article):
                continue
            article_index += 1
            lines.append(
                f"[{article_index}] 来源：{article.source_name}\n"
                f"标题：{article.title}\n"
                f"内容：{article.content[:self.article_content_chars]}\n"
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
        impact = getattr(cluster, "impact", None)
        if impact is None:
            return ""
        payload = {
            "quality_status": impact.status,
            "impact_score": round(float(impact.composite), 3),
            "source_signal": round(float(impact.signal), 3),
            "flags": list(impact.flags),
            "summary_constraints": list(impact.summary_constraints),
        }
        return (
            "编辑约束：\n"
            "请严格遵守 summary_constraints；flags 提示来源结构风险（如单一来源、仅官方来源），"
            "对应内容必须显式归因，不要写成已被独立证实的事实。\n"
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
            "3. same_conflict_different_event: 两个条目属于同一持续中的多日重大冲突/危机/长期对峙"
            "（例如俄乌战争、美伊对峙、以巴冲突、中美贸易战、朝鲜半岛局势）的不同日常事件。"
            "它们共享一个 storyline 但角色为 spillover；仅适用于这种已被定义为持续事件的多日冲突，"
            "不适用于普通主题相似或一次性事件。两个条目必须各自明确涉及同一组冲突方；"
            "仅共享一个国家、领导人、战争、无人机、制裁等泛词时必须判 not_related。"
            "例如美以伊事件与俄乌事件不是同一冲突，即使都提到美国、俄罗斯或空袭。\n"
            "4. not_related: 仅有宽泛地域、行业、主题相似，或属于更远的二级外溢，不应归为同一 storyline。\n"
            "5. precision-first: 对于 ordinary 主题相似宁可判 not_related，也不要因为大区域相似或泛主题背景就硬合并。"
            "但对于第 3 类（同一持续冲突的不同日常事件），应主动识别并归入 same_conflict_different_event。\n"
            "6. history_storyline 只是辅助线索，不能单独决定相关性。\n"
            "7. confidence 给出 0 到 1 之间的小数。\n"
            "8. 只输出 JSON，格式如下：\n"
            "{\n"
            '  "relations": [\n'
            '    {"left_index": 1, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.82}\n'
            "  ]\n"
            "}\n\n"
            "候选事件对：\n"
            + "\n\n".join(pair_lines)
        )
