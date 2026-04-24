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


class LabelTranslation(BaseModel):
    translation: str = Field(description="Concise English translation for the provided Chinese label.")


class MacroTopicAssignment(BaseModel):
    cluster_index: int = Field(description="1-based cluster index from the provided candidate list.")
    macro_topic_key: str = Field(description="Stable ASCII key shared by related clusters.")
    macro_topic_name: str = Field(description="Short Chinese macro-topic family name, 4-10 characters.")
    topic_icon_key: str | None = Field(
        default=None,
        description="One hotspot icon key chosen from the provided allowlist.",
    )


class MacroTopicGrouping(BaseModel):
    assignments: list[MacroTopicAssignment] = Field(
        default_factory=list,
        description="One assignment per candidate cluster.",
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

    def classify_macro_topics(self, clusters: list[ArticleCluster]) -> list[dict[str, str | int | None]]:
        if not clusters:
            return []
        return self._classify_macro_topic_batches(
            clusters=clusters,
            batch_size=int(self.hot_topics_cfg.get("grouping_batch_size", 12)),
            build_prompt=lambda batch, _assignments, _history: self._build_macro_topic_prompt(batch),
            max_tokens=min(self.max_tokens, 1600),
            stage_label="macro-topic grouping",
        )

    def refine_macro_topics_with_history(
        self,
        clusters: list[ArticleCluster],
        initial_assignments: list[dict[str, str | int | None]],
        history_by_family: dict[str, list[dict[str, str | int]]],
    ) -> list[dict[str, str | int | None]]:
        if not clusters:
            return []
        if not any(history_by_family.values()):
            return initial_assignments
        return self._classify_macro_topic_batches(
            clusters=clusters,
            batch_size=int(self.hot_topics_cfg.get("refinement_batch_size", 10)),
            build_prompt=self._build_macro_topic_refinement_prompt,
            max_tokens=min(self.max_tokens, 2200),
            stage_label="history-refined macro-topic grouping",
            initial_assignments=initial_assignments,
            history_by_family=history_by_family,
        )

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

    def _classify_macro_topic_batches(
        self,
        clusters: list[ArticleCluster],
        batch_size: int,
        build_prompt,
        max_tokens: int,
        stage_label: str,
        initial_assignments: list[dict[str, str | int | None]] | None = None,
        history_by_family: dict[str, list[dict[str, str | int]]] | None = None,
    ) -> list[dict[str, str | int | None]]:
        batch_size = max(1, batch_size)
        all_assignments: list[dict[str, str | int | None]] = []
        for start in range(0, len(clusters), batch_size):
            batch_clusters = clusters[start:start + batch_size]
            local_initial = self._slice_assignments(initial_assignments or [], start, len(batch_clusters))
            local_history = self._slice_history(history_by_family or {}, local_initial)
            prompt = build_prompt(batch_clusters, local_initial, local_history)
            parsed = self._request_macro_topic_grouping(
                prompt=prompt,
                clusters=batch_clusters,
                max_tokens=max_tokens,
                stage_label=f"{stage_label} batch {start + 1}-{start + len(batch_clusters)}",
            )
            normalized = self._normalize_macro_topic_assignments(
                batch_clusters,
                parsed.assignments if parsed is not None else [],
                fallback_assignments=local_initial,
            )
            for assignment in normalized:
                assignment["cluster_index"] = int(assignment["cluster_index"]) + start
                all_assignments.append(assignment)
        return all_assignments

    def _request_macro_topic_grouping(
        self,
        prompt: str,
        clusters: list[ArticleCluster],
        max_tokens: int,
        stage_label: str,
    ) -> MacroTopicGrouping | None:
        content = self._macro_topic_completion(prompt, max_tokens)
        parsed = self._parse_macro_topic_grouping_content(content)
        if parsed is not None:
            return parsed

        logger.warning("Retrying %s with compact JSON prompt after parse failure", stage_label)
        retry_prompt = (
            f"{prompt}\n\n"
            "最后要求：只输出紧凑 JSON，不要解释，不要 Markdown，不要换行装饰。"
        )
        retry_content = self._macro_topic_completion(retry_prompt, max_tokens)
        parsed = self._parse_macro_topic_grouping_content(retry_content)
        if parsed is not None:
            return parsed

        salvaged = self._salvage_macro_topic_assignments(retry_content or content)
        if salvaged:
            logger.warning(
                "Salvaged %d/%d assignments from malformed %s output",
                len(salvaged),
                len(clusters),
                stage_label,
            )
            return MacroTopicGrouping(assignments=salvaged)

        logger.error("Failed to parse %s output after retry", stage_label)
        return None

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

    def _parse_macro_topic_grouping_content(self, content: str) -> MacroTopicGrouping | None:
        if not content.strip():
            return None
        try:
            return MacroTopicGrouping.model_validate_json(content)
        except Exception:
            extracted = self._extract_json_object(content)
            if extracted and extracted != content:
                try:
                    return MacroTopicGrouping.model_validate_json(extracted)
                except Exception:
                    return None
        return None

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

    def _salvage_macro_topic_assignments(self, content: str) -> list[MacroTopicAssignment]:
        if not content.strip():
            return []
        pattern = re.compile(
            r'\{\s*"cluster_index"\s*:\s*(?P<cluster_index>\d+)'
            r'.*?"macro_topic_key"\s*:\s*"(?P<macro_topic_key>[^"]*)"'
            r'.*?"macro_topic_name"\s*:\s*"(?P<macro_topic_name>[^"]*)"'
            r'(?:.*?"topic_icon_key"\s*:\s*"(?P<topic_icon_key>[^"]*)")?',
            re.DOTALL,
        )
        salvaged: list[MacroTopicAssignment] = []
        seen: set[int] = set()
        for match in pattern.finditer(content):
            cluster_index = int(match.group("cluster_index"))
            if cluster_index in seen:
                continue
            seen.add(cluster_index)
            salvaged.append(
                MacroTopicAssignment(
                    cluster_index=cluster_index,
                    macro_topic_key=match.group("macro_topic_key"),
                    macro_topic_name=match.group("macro_topic_name"),
                    topic_icon_key=match.group("topic_icon_key"),
                )
            )
        return salvaged

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

    def _slice_assignments(
        self,
        assignments: list[dict[str, str | int | None]],
        start: int,
        batch_len: int,
    ) -> list[dict[str, str | int | None]]:
        sliced: list[dict[str, str | int | None]] = []
        lower = start + 1
        upper = start + batch_len
        for assignment in assignments:
            cluster_index = assignment.get("cluster_index")
            if not isinstance(cluster_index, int) or not (lower <= cluster_index <= upper):
                continue
            local_assignment = dict(assignment)
            local_assignment["cluster_index"] = cluster_index - start
            sliced.append(local_assignment)
        return sliced

    def _slice_history(
        self,
        history_by_family: dict[str, list[dict[str, str | int]]],
        assignments: list[dict[str, str | int | None]],
    ) -> dict[str, list[dict[str, str | int]]]:
        family_keys = {
            str(assignment.get("macro_topic_key"))
            for assignment in assignments
            if assignment.get("macro_topic_key")
        }
        return {
            key: history_by_family.get(key, [])
            for key in family_keys
        }

    def _normalize_macro_topic_assignments(
        self,
        clusters: list[ArticleCluster],
        assignments: list[MacroTopicAssignment],
        fallback_assignments: list[dict[str, str | int | None]] | None = None,
    ) -> list[dict[str, str | int | None]]:
        fallback_by_index = {
            int(assignment.get("cluster_index", 0)): assignment
            for assignment in (fallback_assignments or [])
            if isinstance(assignment.get("cluster_index"), int)
        }
        assignments_by_index = {
            assignment.cluster_index: assignment for assignment in assignments
        }
        normalized: list[dict[str, str | int | None]] = []
        for index, cluster in enumerate(clusters, 1):
            assignment = assignments_by_index.get(index)
            if assignment is None:
                fallback = fallback_by_index.get(index)
                if fallback is None:
                    normalized.append(self._fallback_macro_assignment(cluster, index))
                    continue
                normalized.append(
                    {
                        "cluster_index": index,
                        "macro_topic_key": self._normalize_macro_topic_key(
                            fallback.get("macro_topic_key") if isinstance(fallback, dict) else None,
                            index,
                        ),
                        "macro_topic_name": self._normalize_macro_topic_name(
                            fallback.get("macro_topic_name") if isinstance(fallback, dict) else None,
                            cluster,
                        ),
                        "topic_icon_key": self._normalize_icon_key(
                            fallback.get("topic_icon_key") if isinstance(fallback, dict) else None,
                        ),
                    }
                )
                continue
            normalized.append(
                {
                    "cluster_index": index,
                    "macro_topic_key": self._normalize_macro_topic_key(assignment.macro_topic_key, index),
                    "macro_topic_name": self._normalize_macro_topic_name(assignment.macro_topic_name, cluster),
                    "topic_icon_key": self._normalize_icon_key(assignment.topic_icon_key),
                }
            )
        return normalized

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
            storyline_membership_status=cluster.storyline_membership_status,
            storyline_anchor_labels=list(cluster.storyline_anchor_labels),
            macro_topic_key=cluster.macro_topic_key,
            macro_topic_name=cluster.macro_topic_name,
            macro_topic_icon_key=cluster.macro_topic_icon_key,
            macro_topic_member_count=cluster.macro_topic_member_count,
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

        translated_groups = self._normalize_perspective_groups(
            summary.cluster,
            parsed.perspective_groups,
            [],
        )
        if len(translated_groups) != len(summary.grouped_perspectives):
            raise ValueError(
                f"Perspective group count changed during translation for '{summary.cluster.topic_category}'"
            )
        for zh_group, en_group in zip(summary.grouped_perspectives, translated_groups):
            if list(zh_group.sources) != list(en_group.sources):
                raise ValueError(
                    f"Perspective grouping changed during translation for '{summary.cluster.topic_category}'"
                )

        summary.summary_en = f"**{headline_clean}**\n\n{body_clean}"
        summary.grouped_perspectives_en = translated_groups
        if parsed.short_topic_name and parsed.short_topic_name.strip():
            summary.short_topic_name_en = self._clean_short_label(parsed.short_topic_name)
        elif summary.short_topic_name:
            summary.short_topic_name_en = self._translate_short_label(summary.short_topic_name)

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

        return f"{instruction}\n\n{articles_block}"

    def _build_macro_topic_prompt(self, clusters: list[ArticleCluster]) -> str:
        cluster_lines: list[str] = []
        for index, cluster in enumerate(clusters, 1):
            lead_title = cluster.articles[0].title if cluster.articles else cluster.topic_category
            source_preview = "、".join(cluster.sources[:4])
            cluster_lines.append(
                f"[{index}] topic_category={cluster.topic_category}\n"
                f"headline={lead_title}\n"
                f"sources={source_preview}\n"
                f"article_count={len(cluster.articles)}"
            )

        allowlist = ", ".join(self.topic_icon_allowlist)
        return (
            "下面是一组已经完成事件级聚类的候选新闻条目。"
            "你的任务不是总结单条新闻，而是判断哪些条目属于同一个更高层级的宏观热点家族。\n\n"
            "规则：\n"
            "1. 只有在多个条目明显属于同一主线事件/冲突/危机时，才给它们相同的 macro_topic_key 与 macro_topic_name。\n"
            "2. 单条新闻即使来源很多，也不要因为视角多就提升为宏观主题家族。\n"
            "3. macro_topic_name 必须是 4-10 个中文字符，不要包含“热点专题”前缀。\n"
            "4. macro_topic_key 必须是 ASCII 小写短键，可用连字符。\n"
            f"5. topic_icon_key 只能从以下列表中选择：{allowlist}\n"
            "6. 如果某条新闻不属于任何更大的共享家族，给它一个唯一的宏观主题键和对应短名。\n"
            "7. 输出必须覆盖每个 cluster_index，且只能输出 JSON。\n\n"
            "输出格式：\n"
            "{\n"
            '  "assignments": [\n'
            '    {"cluster_index": 1, "macro_topic_key": "...", "macro_topic_name": "...", "topic_icon_key": "..."}\n'
            "  ]\n"
            "}\n\n"
            "候选条目：\n"
            + "\n\n".join(cluster_lines)
        )

    def _build_macro_topic_refinement_prompt(
        self,
        clusters: list[ArticleCluster],
        initial_assignments: list[dict[str, str | int | None]],
        history_by_family: dict[str, list[dict[str, str | int]]],
    ) -> str:
        initial_by_index = {
            int(assignment.get("cluster_index", 0)): assignment
            for assignment in initial_assignments
            if isinstance(assignment.get("cluster_index"), int)
        }

        cluster_lines: list[str] = []
        for index, cluster in enumerate(clusters, 1):
            lead_title = cluster.articles[0].title if cluster.articles else cluster.topic_category
            source_preview = "、".join(cluster.sources[:4])
            initial = initial_by_index.get(index, {})
            family_key = str(initial.get("macro_topic_key") or f"single-{index}")
            family_name = str(initial.get("macro_topic_name") or cluster.topic_category)
            history_lines = []
            for item in history_by_family.get(family_key, []):
                report_date = item.get("report_date", "")
                topic_category = item.get("topic_category", "")
                summary = item.get("summary", "")
                history_lines.append(
                    f"- {report_date} | {topic_category} | {summary}"
                )
            history_block = "\n".join(history_lines) if history_lines else "- 无相关历史记忆"
            cluster_lines.append(
                f"[{index}] initial_family_key={family_key}\n"
                f"initial_family_name={family_name}\n"
                f"topic_category={cluster.topic_category}\n"
                f"headline={lead_title}\n"
                f"sources={source_preview}\n"
                f"article_count={len(cluster.articles)}\n"
                f"related_history:\n{history_block}"
            )

        allowlist = ", ".join(self.topic_icon_allowlist)
        return (
            "下面是一组当前日期的候选新闻条目，以及从过去5天检索出的相关历史簇摘要。"
            "请根据当前条目本身和这些历史记忆，重新判断哪些条目属于同一个宏观热点家族。"
            "历史记忆只用于帮助你识别跨天延续的大主题，不能因为历史很多就把今天无关的条目硬合并。\n\n"
            "规则：\n"
            "1. 优先依据今天的条目是否属于同一主线事件/危机/战争来分组。\n"
            "2. 历史记忆只用于纠正今天的碎片化分组，帮助识别同一超级话题。\n"
            "3. 只有核心事件及其直接外溢影响可以归入同一家族；宽泛的地域相似或二级延伸不要硬合并。\n"
            "4. 单条新闻即使来源很多，也不要仅因视角多就提升为宏观主题家族。\n"
            "5. macro_topic_name 必须是 4-10 个中文字符，不要包含“热点专题”前缀。\n"
            "6. macro_topic_key 必须是 ASCII 小写短键，可用连字符。\n"
            f"7. topic_icon_key 只能从以下列表中选择：{allowlist}\n"
            "8. 输出必须覆盖每个 cluster_index，且只能输出 JSON。\n\n"
            "输出格式：\n"
            "{\n"
            '  "assignments": [\n'
            '    {"cluster_index": 1, "macro_topic_key": "...", "macro_topic_name": "...", "topic_icon_key": "..."}\n'
            "  ]\n"
            "}\n\n"
            "当前候选条目与历史记忆：\n"
            + "\n\n".join(cluster_lines)
        )

    def _fallback_macro_assignment(self, cluster: ArticleCluster, index: int) -> dict[str, str | int | None]:
        return {
            "cluster_index": index,
            "macro_topic_key": f"single-{index}",
            "macro_topic_name": self._normalize_macro_topic_name(cluster.topic_category, cluster),
            "topic_icon_key": self._normalize_icon_key(None),
        }

    def _normalize_macro_topic_key(self, value: str | None, index: int) -> str:
        compact = re.sub(r"[^a-z0-9\-]+", "-", (value or "").lower()).strip("-")
        return compact or f"single-{index}"

    def _normalize_macro_topic_name(self, value: str | None, cluster: ArticleCluster) -> str:
        candidate = re.sub(r"\s+", "", (value or "").strip())
        candidate = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", candidate)
        candidate = candidate[:10].strip(" -:：，,、。.；;")
        if candidate:
            return candidate
        fallback = cluster.articles[0].title if cluster.articles else cluster.topic_category
        fallback = re.sub(r"\s+", "", fallback)[:10].strip(" -:：，,、。.；;")
        return fallback or "焦点话题"

    def _normalize_icon_key(self, value: str | None) -> str:
        if value in self.topic_icon_allowlist:
            return value
        return self.topic_icon_allowlist[0] if self.topic_icon_allowlist else "globe"

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
