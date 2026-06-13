"""Editorial display planning for NewsPrism reports.

Selection is impact-driven: clusters are ranked by the calibrated composite
from the impact evaluation, with display-category diversity caps. Storyline
families become hot-topic tabs. Display-level duplicates are detected with
embedding centroids and shared article URLs — no curated vocabulary.

Layer: service (imports config and types; never imports runtime or repo).
"""
from __future__ import annotations

import logging
import re
import urllib.parse
from collections import defaultdict

import numpy as np

from newsprism.config import Config
from newsprism.types import ArticleCluster, ClusterSummary, EditorialReportPlan

logger = logging.getLogger(__name__)

_DEFAULT_HOT_TOPIC_ICON_KEY = "globe"
_DISPLAY_DEDUP_SIMILARITY = 0.80


def _extract_markdown_headline(summary_text: str) -> str:
    for line in summary_text.splitlines():
        match = re.match(r"\*\*(.+?)\*\*", line.strip())
        if match:
            return match.group(1)
    return ""


def _composite(item: ArticleCluster | ClusterSummary) -> float:
    impact = getattr(item, "impact", None)
    if impact is None and isinstance(item, ClusterSummary):
        impact = getattr(item.cluster, "impact", None)
    return float(impact.composite) if impact is not None else 0.0


def _feelgood(summary: ClusterSummary) -> float:
    impact = getattr(summary, "impact", None) or getattr(summary.cluster, "impact", None)
    return impact.dim("feelgood") if impact is not None else 0.0


def _severity(summary: ClusterSummary) -> float:
    impact = getattr(summary, "impact", None) or getattr(summary.cluster, "impact", None)
    return impact.dim("severity") if impact is not None else 0.0


def _display_category(item: ArticleCluster | ClusterSummary) -> str:
    category = getattr(item, "display_category", None)
    if not category and isinstance(item, ClusterSummary):
        category = getattr(item.cluster, "display_category", None)
    return category or ""


def _summary_centroid(summary: ClusterSummary) -> np.ndarray | None:
    embeddings = [
        np.array(article.embedding, dtype=float)
        for article in summary.cluster.articles
        if article.embedding is not None
    ]
    if not embeddings:
        return None
    centroid = np.mean(embeddings, axis=0)
    norm = np.linalg.norm(centroid)
    if norm == 0:
        return None
    return centroid / norm


def _summary_urls(summary: ClusterSummary) -> set[str]:
    return {article.url for article in summary.cluster.articles}


def _source_overlap(left: ClusterSummary, right: ClusterSummary) -> int:
    return len(set(left.cluster.sources) & set(right.cluster.sources))


def _is_hot_member(role: str | None, membership_status: str | None) -> bool:
    if role not in {"core", "spillover"}:
        return False
    if membership_status == "excluded_to_main":
        return False
    return True


def _summary_is_hot_member(summary: ClusterSummary) -> bool:
    return _is_hot_member(
        summary.storyline_role or getattr(summary.cluster, "storyline_role", "none"),
        summary.storyline_membership_status
        or getattr(summary.cluster, "storyline_membership_status", "none"),
    )


def _cluster_is_hot_member(cluster: ArticleCluster) -> bool:
    return _is_hot_member(
        getattr(cluster, "storyline_role", "none"),
        getattr(cluster, "storyline_membership_status", "none"),
    )


def _normalize_storyline_name(name: str | None, summary: ClusterSummary | None, max_chars: int) -> str:
    candidate = re.sub(r"\s+", "", (name or "").strip())
    candidate = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", candidate).strip()
    candidate = candidate[:max_chars].strip(" -:：，,、。.；;")
    if candidate:
        return candidate
    if summary is not None:
        headline = _extract_markdown_headline(summary.summary) or summary.cluster.topic_category or "全球焦点"
        headline = re.sub(r"\s+", "", headline)[:max_chars].strip(" -:：，,、。.；;")
        if headline:
            return headline
    return "全球焦点"


class EditorialPlanner:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def base_plan(self, kept_summaries: list[ClusterSummary]) -> EditorialReportPlan:
        hot_topics, focus_storylines, regular_summaries = select_hot_topic_families(kept_summaries, self.cfg)
        return EditorialReportPlan(
            hot_topics=hot_topics,
            focus_storylines=focus_storylines,
            regular_summaries=regular_summaries,
            positive_summaries=[],
        )

    def finalize(
        self,
        base_plan: EditorialReportPlan,
        positive_summaries: list[ClusterSummary] | None = None,
    ) -> EditorialReportPlan:
        positive = positive_summaries or []
        regular_summaries = split_positive_energy_lane(base_plan.regular_summaries, positive)
        hot_topics, focus_storylines, regular_summaries, positive = resolve_display_duplicates(
            base_plan.hot_topics,
            base_plan.focus_storylines,
            regular_summaries,
            positive,
        )
        return EditorialReportPlan(
            hot_topics=hot_topics,
            focus_storylines=focus_storylines,
            regular_summaries=regular_summaries,
            positive_summaries=positive,
        )

    def plan(
        self,
        kept_summaries: list[ClusterSummary],
        positive_summaries: list[ClusterSummary] | None = None,
    ) -> EditorialReportPlan:
        return self.finalize(self.base_plan(kept_summaries), positive_summaries)


# ─── CLUSTER-LEVEL SELECTION (pre-summary) ────────────────────────────────────

def _diverse_by_category(
    items: list,
    limit: int,
    max_per_category: int,
) -> list:
    """Greedy top-N by composite with a per-display-category cap; spill if short."""
    selected: list = []
    per_category: dict[str, int] = defaultdict(int)
    overflow: list = []
    for item in items:
        if len(selected) >= limit:
            break
        category = _display_category(item)
        if category and per_category[category] >= max_per_category:
            overflow.append(item)
            continue
        selected.append(item)
        if category:
            per_category[category] += 1
    for item in overflow:
        if len(selected) >= limit:
            break
        selected.append(item)
    return selected


def select_report_clusters(
    clusters: list[ArticleCluster],
    cfg: Config,
) -> tuple[list[ArticleCluster], list[ArticleCluster]]:
    """Split candidates into hot-topic family members and impact-ranked main clusters."""
    hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
    impact_cfg = (cfg.editorial_values or {}).get("impact", {})
    positive_cfg = cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}
    diversity_cfg = impact_cfg.get("diversity", {}) if isinstance(impact_cfg, dict) else {}
    main_limit = cfg.clustering.get("max_clusters_per_report", 20)
    max_per_category = max(1, int(diversity_cfg.get("max_per_category", 8)))
    positive_extra_limit = max(0, int(positive_cfg.get("max_items", 5)))
    min_feelgood = float(impact_cfg.get("positive", {}).get("min_feelgood", 7.0))

    ranked = sorted(clusters, key=_composite, reverse=True)

    hot_clusters: list[ArticleCluster] = []
    main_pool: list[ArticleCluster] = []
    if hot_cfg.get("enabled", False):
        max_topic_tabs = int(hot_cfg.get("max_topic_tabs", 3))
        min_items_per_topic = int(hot_cfg.get("min_items_per_topic", 5))
        families: dict[str, list[ArticleCluster]] = defaultdict(list)
        for cluster in ranked:
            if cluster.storyline_key and _cluster_is_hot_member(cluster):
                families[cluster.storyline_key].append(cluster)
        eligible = [
            (key, members)
            for key, members in families.items()
            if len(members) >= min_items_per_topic
            and any(cluster.storyline_role == "core" for cluster in members)
        ]
        eligible.sort(key=lambda item: (-len(item[1]), -max(_composite(c) for c in item[1])))
        hot_keys = {key for key, _members in eligible[:max_topic_tabs]}
        for cluster in ranked:
            if cluster.storyline_key in hot_keys and _cluster_is_hot_member(cluster):
                cluster.is_hot_topic = True
                cluster.macro_topic_member_count = len(families[cluster.storyline_key])
                hot_clusters.append(cluster)
            else:
                main_pool.append(cluster)
    else:
        main_pool = list(ranked)

    main_clusters = _diverse_by_category(main_pool, main_limit, max_per_category)

    # Positive extras: high-feelgood clusters clipped by the main cap still get
    # summarized so the 今日正能量 lane can use them.
    selected_ids = {id(cluster) for cluster in hot_clusters + main_clusters}
    extras = 0
    for cluster in main_pool:
        if extras >= positive_extra_limit:
            break
        if id(cluster) in selected_ids:
            continue
        impact = getattr(cluster, "impact", None)
        if impact is None or impact.dim("feelgood") < min_feelgood or impact.status == "suppress":
            continue
        main_clusters.append(cluster)
        selected_ids.add(id(cluster))
        extras += 1

    return hot_clusters, main_clusters


# ─── SUMMARY-LEVEL PLANNING (post-freshness) ──────────────────────────────────

def _rank_main_summaries(summaries: list[ClusterSummary], cfg: Config, limit: int) -> list[ClusterSummary]:
    impact_cfg = (cfg.editorial_values or {}).get("impact", {})
    diversity_cfg = impact_cfg.get("diversity", {}) if isinstance(impact_cfg, dict) else {}
    max_per_category = max(1, int(diversity_cfg.get("max_per_category", 8)))
    ranked = sorted(summaries, key=_composite, reverse=True)
    if limit <= 0:
        return []
    return _diverse_by_category(ranked, limit, max_per_category)


def select_hot_topic_families(
    summaries: list[ClusterSummary],
    cfg: Config,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[ClusterSummary]]:
    hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
    max_name_chars = hot_cfg.get("tab_name_max_chars", 10)
    max_topic_tabs = hot_cfg.get("max_topic_tabs", 3)
    min_items_per_topic = hot_cfg.get("min_items_per_topic", 5)
    main_limit = cfg.clustering.get("max_clusters_per_report", 20)

    for summary in summaries:
        summary.is_hot_topic = getattr(summary.cluster, "is_hot_topic", False)
        summary.storyline_key = getattr(summary.cluster, "storyline_key", None)
        summary.storyline_name = getattr(summary.cluster, "storyline_name", None)
        summary.storyline_role = getattr(summary.cluster, "storyline_role", "none")
        summary.storyline_confidence = getattr(summary.cluster, "storyline_confidence", 0.0)
        summary.storyline_membership_status = getattr(summary.cluster, "storyline_membership_status", "none")
        summary.storyline_anchor_labels = list(getattr(summary.cluster, "storyline_anchor_labels", []))
        summary.macro_topic_key = getattr(summary.cluster, "macro_topic_key", None)
        summary.macro_topic_name = getattr(summary.cluster, "macro_topic_name", None)
        summary.macro_topic_icon_key = getattr(summary.cluster, "macro_topic_icon_key", None)
        summary.macro_topic_member_count = getattr(summary.cluster, "macro_topic_member_count", 0)
        summary.impact = getattr(summary.cluster, "impact", None)
        summary.display_category = getattr(summary.cluster, "display_category", None)

    grouped: dict[str, list[ClusterSummary]] = defaultdict(list)
    group_order: dict[str, int] = {}
    group_icons: dict[str, str] = {}
    standalone: list[ClusterSummary] = []
    for index, summary in enumerate(summaries):
        if summary.freshness_state == "stale":
            continue
        key = summary.macro_topic_key or summary.storyline_key
        if key and _summary_is_hot_member(summary):
            grouped[key].append(summary)
            group_order.setdefault(key, index)
            group_icons.setdefault(key, summary.macro_topic_icon_key or _DEFAULT_HOT_TOPIC_ICON_KEY)
        else:
            standalone.append(summary)

    hot_keys = [
        key
        for key, members in grouped.items()
        if len(members) >= min_items_per_topic
        and any(
            (member.storyline_role or getattr(member.cluster, "storyline_role", "none")) == "core"
            for member in members
        )
    ]
    hot_keys.sort(key=lambda key: (-len(grouped[key]), group_order.get(key, 0)))
    hot_keys = hot_keys[:max_topic_tabs]

    hot_topics: list[dict[str, object]] = []
    for position, key in enumerate(hot_keys, 1):
        members = sorted(grouped[key], key=_composite, reverse=True)
        family_name = _normalize_storyline_name(
            members[0].storyline_name or members[0].macro_topic_name,
            members[0],
            max_name_chars,
        )
        hot_topics.append(
            {
                "dom_id": f"hot-topic-{position}",
                "macro_topic_key": key,
                "macro_topic_name": family_name,
                "storyline_key": key,
                "storyline_name": family_name,
                "topic_icon_key": group_icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
                "anchor_labels": list(members[0].storyline_anchor_labels) if members else [],
                "member_count": len(members),
                "summaries": members,
            }
        )

    focus_storylines: list[dict[str, object]] = []
    main_candidates = list(standalone)
    assigned_keys = set(hot_keys)
    for key, members in grouped.items():
        if key in assigned_keys:
            continue
        members = sorted(members, key=_composite, reverse=True)
        if 2 <= len(members) < min_items_per_topic and any(
            (member.storyline_role or getattr(member.cluster, "storyline_role", "none")) == "core"
            for member in members
        ):
            for summary in members:
                summary.is_hot_topic = False
                summary.cluster.is_hot_topic = False
            focus_storylines.append(
                {
                    "storyline_key": key,
                    "storyline_name": _normalize_storyline_name(
                        members[0].storyline_name or members[0].macro_topic_name,
                        members[0],
                        max_name_chars,
                    ),
                    "topic_icon_key": group_icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
                    "member_count": len(members),
                    "summaries": members,
                }
            )
            continue
        for summary in members:
            summary.is_hot_topic = False
            summary.cluster.is_hot_topic = False
            main_candidates.append(summary)

    focus_storylines.sort(
        key=lambda family: (-int(family["member_count"]), group_order.get(str(family["storyline_key"]), 0)),
    )
    main_summaries = _rank_main_summaries(main_candidates, cfg, main_limit)
    return hot_topics, focus_storylines, main_summaries


# ─── 今日正能量 ────────────────────────────────────────────────────────────────

def _summary_primary_domain(summary: ClusterSummary) -> str:
    for article in summary.cluster.articles:
        domain = urllib.parse.urlparse(article.url).netloc.lower().removeprefix("www.").strip(".")
        if domain:
            return domain
    if summary.cluster.sources:
        return summary.cluster.sources[0].lower()
    return ""


def select_positive_summaries(
    kept_summaries: list[ClusterSummary],
    cfg: Config,
) -> list[ClusterSummary]:
    """Pick the 今日正能量 lane from the impact evaluation's feelgood dimension."""
    positive_cfg = cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}
    if not bool(positive_cfg.get("enabled", True)):
        return []
    impact_cfg = (cfg.editorial_values or {}).get("impact", {})
    positive_rules = impact_cfg.get("positive", {}) if isinstance(impact_cfg, dict) else {}
    min_feelgood = float(positive_rules.get("min_feelgood", 7.0))
    max_severity = float(positive_rules.get("max_severity", 4.0))
    max_items = max(0, int(positive_cfg.get("max_items", 5)))

    candidates = [
        summary
        for summary in kept_summaries
        if _feelgood(summary) >= min_feelgood
        and _severity(summary) <= max_severity
        and (getattr(summary, "quality_status", "publishable") != "suppress")
    ]
    candidates.sort(key=lambda summary: (-_feelgood(summary), -_composite(summary)))

    selected: list[ClusterSummary] = []
    domains: set[str] = set()
    for summary in candidates:
        if len(selected) >= max_items:
            break
        domain = _summary_primary_domain(summary)
        if domain and domain in domains:
            continue
        impact = getattr(summary, "impact", None) or getattr(summary.cluster, "impact", None)
        feelgood = _feelgood(summary)
        reason = impact.rationale if impact else ""
        summary.feelgood_score = feelgood
        summary.feelgood_reason = reason
        # Renderer/template contract (carried from the former feelgood scorer):
        summary.positive_energy_score = round(feelgood / 10.0, 4)  # type: ignore[attr-defined]
        summary.positive_energy_reason = reason  # type: ignore[attr-defined]
        summary.positive_energy_category = (
            getattr(summary, "display_category", None)
            or getattr(summary.cluster, "display_category", None)
            or summary.cluster.topic_category
        )  # type: ignore[attr-defined]
        summary.positive_energy_source = (
            summary.cluster.sources[0] if summary.cluster.sources else ""
        )  # type: ignore[attr-defined]
        selected.append(summary)
        if domain:
            domains.add(domain)

    logger.info(
        "Positive energy selection: candidates=%d selected=%d (min_feelgood=%.1f) headlines=%s",
        len(candidates),
        len(selected),
        min_feelgood,
        [_extract_markdown_headline(summary.summary) or summary.cluster.topic_category for summary in selected],
    )
    return selected


def split_positive_energy_lane(
    main_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
) -> list[ClusterSummary]:
    positive_ids = {id(summary) for summary in positive_summaries}
    return [summary for summary in main_summaries if id(summary) not in positive_ids]


# ─── DISPLAY DEDUP (embedding-based) ──────────────────────────────────────────

def _prefer_summary(left: ClusterSummary, right: ClusterSummary) -> ClusterSummary:
    left_rank = (
        _composite(left),
        len(left.grouped_perspectives),
        len(left.cluster.sources),
        1 if left.freshness_state == "new" else 0,
    )
    right_rank = (
        _composite(right),
        len(right.grouped_perspectives),
        len(right.cluster.sources),
        1 if right.freshness_state == "new" else 0,
    )
    return left if left_rank >= right_rank else right


def _merge_duplicate_summary(target: ClusterSummary, duplicate: ClusterSummary, reason: str, confidence: float) -> None:
    seen_urls = {article.url for article in target.cluster.articles}
    for article in duplicate.cluster.articles:
        if article.url in seen_urls:
            continue
        target.cluster.articles.append(article)
        seen_urls.add(article.url)
        if article.source_name not in target.cluster.sources:
            target.cluster.sources.append(article.source_name)

    for source, perspective in duplicate.perspectives.items():
        if source not in target.perspectives and perspective.strip():
            target.perspectives[source] = perspective

    grouped_by_text = {
        re.sub(r"\s+", " ", group.perspective).strip(): group
        for group in target.grouped_perspectives
        if group.perspective.strip()
    }
    for group in duplicate.grouped_perspectives:
        key = re.sub(r"\s+", " ", group.perspective).strip()
        if not key:
            continue
        existing = grouped_by_text.get(key)
        if existing is None:
            target.grouped_perspectives.append(group)
            grouped_by_text[key] = group
            continue
        for source in group.sources:
            if source not in existing.sources:
                existing.sources.append(source)

    target.duplicate_action = "merged"  # type: ignore[attr-defined]
    target.duplicate_reason = reason  # type: ignore[attr-defined]
    target.duplicate_confidence = round(confidence, 4)  # type: ignore[attr-defined]
    duplicate.duplicate_action = "suppressed"  # type: ignore[attr-defined]
    duplicate.duplicate_reason = reason  # type: ignore[attr-defined]
    duplicate.duplicate_confidence = round(confidence, 4)


def _display_duplicate(
    left: ClusterSummary,
    right: ClusterSummary,
    centroids: dict[int, np.ndarray | None],
) -> tuple[bool, str, float]:
    if _summary_urls(left) & _summary_urls(right):
        return True, "shared_article_url", 0.95
    left_centroid = centroids.get(id(left))
    right_centroid = centroids.get(id(right))
    if left_centroid is None or right_centroid is None:
        return False, "", 0.0
    similarity = float(np.dot(left_centroid, right_centroid))
    if similarity >= _DISPLAY_DEDUP_SIMILARITY:
        return True, "embedding_similarity", similarity
    return False, "", similarity


def _flatten_display_summaries(
    hot_topics: list[dict[str, object]],
    focus_storylines: list[dict[str, object]],
    regular_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
) -> list[ClusterSummary]:
    summaries: list[ClusterSummary] = []
    for family in hot_topics + focus_storylines:
        family_summaries = family.get("summaries", [])
        if isinstance(family_summaries, list):
            summaries.extend(summary for summary in family_summaries if isinstance(summary, ClusterSummary))
    summaries.extend(positive_summaries)
    summaries.extend(regular_summaries)
    return summaries


def _filter_family_summaries(families: list[dict[str, object]], keep_ids: set[int]) -> list[dict[str, object]]:
    filtered: list[dict[str, object]] = []
    for family in families:
        family_summaries = family.get("summaries", [])
        if not isinstance(family_summaries, list):
            continue
        kept = [
            summary
            for summary in family_summaries
            if isinstance(summary, ClusterSummary) and id(summary) in keep_ids
        ]
        if not kept:
            continue
        family["summaries"] = kept
        family["member_count"] = len(kept)
        filtered.append(family)
    return filtered


def resolve_display_duplicates(
    hot_topics: list[dict[str, object]],
    focus_storylines: list[dict[str, object]],
    regular_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[ClusterSummary], list[ClusterSummary]]:
    displayed = _flatten_display_summaries(hot_topics, focus_storylines, regular_summaries, positive_summaries)
    centroids: dict[int, np.ndarray | None] = {}
    for summary in displayed:
        centroids[id(summary)] = _summary_centroid(summary)
        summary.duplicate_action = "kept"  # type: ignore[attr-defined]
        summary.duplicate_reason = ""  # type: ignore[attr-defined]
        summary.duplicate_confidence = 0.0  # type: ignore[attr-defined]

    suppressed_ids: set[int] = set()
    for left_index, left in enumerate(displayed):
        if id(left) in suppressed_ids:
            continue
        for right in displayed[left_index + 1 :]:
            if id(right) in suppressed_ids:
                continue
            duplicate, reason, confidence = _display_duplicate(left, right, centroids)
            if not duplicate:
                continue
            preferred = _prefer_summary(left, right)
            suppressed = right if preferred is left else left
            _merge_duplicate_summary(preferred, suppressed, reason, confidence)
            suppressed_ids.add(id(suppressed))
            logger.info(
                "Display duplicate suppressed: kept='%s' suppressed='%s' reason=%s confidence=%.2f",
                _extract_markdown_headline(preferred.summary) or preferred.cluster.topic_category,
                _extract_markdown_headline(suppressed.summary) or suppressed.cluster.topic_category,
                reason,
                confidence,
            )
            if suppressed is left:
                break

    keep_ids = {id(summary) for summary in displayed} - suppressed_ids
    return (
        _filter_family_summaries(hot_topics, keep_ids),
        _filter_family_summaries(focus_storylines, keep_ids),
        [summary for summary in regular_summaries if id(summary) in keep_ids],
        [summary for summary in positive_summaries if id(summary) in keep_ids],
    )
