"""Daily pipeline orchestrator.

Collect phase (every 4h):  fetch → tag → dedup → store
Publish phase (08:00 CST): cluster → summarize → render HTML → push Telegram

Layer: runtime — the only layer that imports from all others.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import time
import urllib.parse
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from newsprism.config import Config
from newsprism.repo import (
    delete_clusters_for_date,
    get_articles_by_ids,
    get_clusters_for_date,
    get_recent_clusters,
    get_unclustered_articles,
    init_db,
    insert_article,
    insert_cluster,
    mark_articles_clustered,
    reset_articles_clustered,
    update_article_embedding,
)
from newsprism.runtime.publisher import TelegramPublisher
from newsprism.runtime.renderer import HtmlRenderer, _extract_headline
from newsprism.service.clusterer import Clusterer
from newsprism.service.collector import Collector
from newsprism.service.dedup import Deduplicator
from newsprism.service.filter import TopicTagger
from newsprism.service.freshness import FreshnessEvaluator
from newsprism.service.seeker import ActiveSeeker
from newsprism.service.storyline import EventClusterValidator, StorylineResolver
from newsprism.service.summarizer import Summarizer
from newsprism.types import ArticleCluster, Cluster, ClusterSummary, raw_to_articles

logger = logging.getLogger(__name__)

try:
    from newsprism.repo import get_article_id_by_url
except ImportError:
    get_article_id_by_url = None

try:
    from newsprism.repo import get_report_article_ids
except ImportError:
    def get_report_article_ids(report_date: str) -> list[int]:
        article_ids: list[int] = []
        for cluster in get_clusters_for_date(report_date):
            for article_id in getattr(cluster, "article_ids", []) or []:
                if article_id not in article_ids:
                    article_ids.append(article_id)
        return article_ids

_DEFAULT_HOT_TOPIC_ICON_KEY = "globe"
_DEFAULT_HOT_TOPIC_ALLOWLIST = ["globe", "war", "trade", "chip", "ai", "energy"]
_HEADLINEISH_NAME_TOKENS = (
    "表示",
    "宣布",
    "警告",
    "回应",
    "确认",
    "威胁",
    "载有",
    "运送",
    "计划",
    "要求",
    "呼吁",
    "称",
    "说",
    "突破",
)


def _normalize_storyline_name(name: str | None, summary: ClusterSummary | None, max_chars: int) -> str:
    candidate = re.sub(r"\s+", "", (name or "").strip())
    candidate = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", candidate).strip()
    candidate = candidate[:max_chars].strip(" -:：，,、。.；;")
    if candidate:
        return candidate
    if summary is not None:
        headline = _extract_headline(summary.summary) or summary.cluster.topic_category or "全球焦点"
        headline = re.sub(r"\s+", "", headline)
        headline = headline[:max_chars].strip(" -:：，,、。.；;")
        if headline:
            return headline
    return "全球焦点"


def _normalized_storyline_candidate(name: str | None, max_chars: int) -> str:
    candidate = re.sub(r"\s+", "", (name or "").strip())
    candidate = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", candidate).strip()
    return candidate[:max_chars].strip(" -:：，,、。.；;")


def _looks_like_headline_fragment(name: str) -> bool:
    if not name:
        return True
    if any(char in name for char in ":：，,。！？!?；;“”‘’\"'《》"):
        return True
    if len(name) >= 8 and any(token in name for token in _HEADLINEISH_NAME_TOKENS):
        return True
    return False


def _best_storyline_label(candidates: list[str | None], max_chars: int) -> str | None:
    counts: dict[str, int] = defaultdict(int)
    originals: dict[str, str] = {}
    for candidate in candidates:
        normalized = _normalized_storyline_candidate(candidate, max_chars)
        if not normalized or _looks_like_headline_fragment(normalized):
            continue
        counts[normalized] += 1
        originals.setdefault(normalized, normalized)
    if not counts:
        return None
    best = max(counts, key=lambda key: (counts[key], -len(key)))
    return originals[best]


def _normalize_icon_key(icon_key: str | None, allowlist: list[str]) -> str:
    if icon_key in allowlist:
        return icon_key
    return allowlist[0] if allowlist else _DEFAULT_HOT_TOPIC_ICON_KEY


def _cluster_storyline_headline(cluster: ArticleCluster) -> str:
    if cluster.articles:
        return cluster.articles[0].title
    return cluster.topic_category


def _storyline_group_key(cluster: ArticleCluster, index: int) -> str:
    return cluster.storyline_key or cluster.macro_topic_key or f"single-{index + 1}"


def _group_clusters_by_storyline(clusters: list[ArticleCluster]) -> dict[str, list[ArticleCluster]]:
    grouped: dict[str, list[ArticleCluster]] = defaultdict(list)
    for index, cluster in enumerate(clusters):
        grouped[_storyline_group_key(cluster, index)].append(cluster)
    return grouped


def _storyline_log_line(storyline_key: str, clusters: list[ArticleCluster]) -> str:
    storyline_name = clusters[0].storyline_name or clusters[0].macro_topic_name or clusters[0].topic_category
    roles = ",".join(sorted({cluster.storyline_role for cluster in clusters}))
    headlines = ", ".join(_cluster_storyline_headline(cluster) for cluster in clusters[:3])
    return f"{storyline_key}/{storyline_name}: {len(clusters)} role={roles} [{headlines}]"


def _log_storyline_stage(stage: str, clusters: list[ArticleCluster]) -> None:
    grouped = _group_clusters_by_storyline(clusters)
    if not grouped:
        logger.info("%s: no storyline families", stage)
        return
    lines = [
        _storyline_log_line(key, members)
        for key, members in sorted(
            grouped.items(),
            key=lambda item: min(
                getattr(cluster, "_storyline_candidate_index", index)
                for index, cluster in enumerate(item[1], 1)
            ),
        )
    ]
    logger.info("%s: %s", stage, " | ".join(lines))


def _reset_hot_topic_metadata(clusters: list[ArticleCluster]) -> None:
    for index, cluster in enumerate(clusters, 1):
        cluster.is_hot_topic = False
        cluster.macro_topic_member_count = 0
        if not cluster.storyline_key:
            cluster.storyline_key = f"single-{index}"
        if not cluster.storyline_name:
            cluster.storyline_name = cluster.macro_topic_name or cluster.topic_category
        if not cluster.storyline_membership_status:
            cluster.storyline_membership_status = "none"
        if cluster.storyline_anchor_labels is None:
            cluster.storyline_anchor_labels = []
        if not cluster.macro_topic_key:
            cluster.macro_topic_key = cluster.storyline_key
        if not cluster.macro_topic_name:
            cluster.macro_topic_name = cluster.storyline_name
        if not cluster.macro_topic_icon_key:
            cluster.macro_topic_icon_key = _DEFAULT_HOT_TOPIC_ICON_KEY


def _is_storyline_hot_member(role: str | None, membership_status: str | None) -> bool:
    if role not in {"core", "spillover"}:
        return False
    if membership_status == "excluded_to_main":
        return False
    if membership_status in {"core", "spillover"}:
        return True
    return membership_status in {None, "", "none"}


def _cluster_is_hot_member(cluster: ArticleCluster) -> bool:
    return _is_storyline_hot_member(
        getattr(cluster, "storyline_role", "none"),
        getattr(cluster, "storyline_membership_status", "none"),
    )


def _summary_is_hot_member(summary: ClusterSummary) -> bool:
    return _is_storyline_hot_member(
        summary.storyline_role or getattr(summary.cluster, "storyline_role", "none"),
        summary.storyline_membership_status or getattr(summary.cluster, "storyline_membership_status", "none"),
    )


def _summary_valid_perspective_count(summary: ClusterSummary) -> int:
    if summary.grouped_perspectives:
        return len(summary.grouped_perspectives)
    if summary.perspectives:
        return len(summary.perspectives)
    return 0


def _summary_organic_source_count(summary: ClusterSummary) -> int:
    return len({article.source_name for article in summary.cluster.articles if not article.is_searched})


def _normalized_anchor_label(label: str) -> str:
    return re.sub(r"\s+", "", label.strip().lower())


def _summary_anchor_labels(summary: ClusterSummary) -> set[str]:
    return {
        _normalized_anchor_label(label)
        for label in summary.storyline_anchor_labels
        if _normalized_anchor_label(label)
    }


def _normalized_event_text(value: str) -> str:
    return re.sub(r"\s+", "", value.lower())


def _event_token_set(value: str) -> set[str]:
    normalized = _normalized_event_text(value)
    english = set(re.findall(r"[a-z]{4,}", normalized))
    chinese = set(re.findall(r"[\u4e00-\u9fff]{2,4}", value))
    return english | chinese


def _article_title_overlap(left: ClusterSummary, right: ClusterSummary) -> int:
    left_titles = {
        _normalized_event_text(article.title)
        for article in left.cluster.articles
    }
    right_titles = {
        _normalized_event_text(article.title)
        for article in right.cluster.articles
    }
    return len(left_titles & right_titles)


def _source_overlap(left: ClusterSummary, right: ClusterSummary) -> int:
    return len(set(left.cluster.sources) & set(right.cluster.sources))


def _normalized_perspective_set(summary: ClusterSummary) -> set[str]:
    return {
        re.sub(r"\s+", " ", text).strip()
        for text in summary.perspectives.values()
        if text.strip()
    }


def _same_event_candidate(left: ClusterSummary, right: ClusterSummary) -> bool:
    if _summary_anchor_labels(left) & _summary_anchor_labels(right):
        return True
    shared_storyline = (
        bool(left.storyline_key)
        and bool(right.storyline_key)
        and left.storyline_key == right.storyline_key
    )
    if _article_title_overlap(left, right) >= 1 and _source_overlap(left, right) >= 1:
        return True
    left_tokens = _event_token_set(_extract_headline(left.summary) or "")
    right_tokens = _event_token_set(_extract_headline(right.summary) or "")
    if not left_tokens or not right_tokens:
        return False
    token_overlap = len(left_tokens & right_tokens)
    return token_overlap >= 2 and (_source_overlap(left, right) >= 1 or shared_storyline)


def _same_angle_candidate(left: ClusterSummary, right: ClusterSummary) -> bool:
    left_tokens = _event_token_set(_extract_headline(left.summary) or "")
    right_tokens = _event_token_set(_extract_headline(right.summary) or "")
    left_groups = _summary_valid_perspective_count(left)
    right_groups = _summary_valid_perspective_count(right)
    left_perspectives = _normalized_perspective_set(left)
    right_perspectives = _normalized_perspective_set(right)
    if left_perspectives and right_perspectives and left_perspectives == right_perspectives:
        return True
    if left_groups != right_groups or left_groups == 0:
        return False
    token_overlap = len(left_tokens & right_tokens)
    title_overlap = _article_title_overlap(left, right)
    source_overlap = _source_overlap(left, right)
    if title_overlap >= 1 and source_overlap >= 1 and token_overlap >= 2:
        return True
    if (
        left.storyline_key
        and right.storyline_key
        and left.storyline_key == right.storyline_key
        and title_overlap >= 1
        and token_overlap >= 3
    ):
        return True
    return False


def _prefer_summary(left: ClusterSummary, right: ClusterSummary) -> ClusterSummary:
    left_rank = (
        _summary_valid_perspective_count(left),
        _summary_organic_source_count(left),
    )
    right_rank = (
        _summary_valid_perspective_count(right),
        _summary_organic_source_count(right),
    )
    return left if left_rank > right_rank else right


def _deduplicate_main_lane(summaries: list[ClusterSummary]) -> list[ClusterSummary]:
    kept: list[ClusterSummary] = []
    for candidate in summaries:
        duplicate_index: int | None = None
        for index, existing in enumerate(kept):
            if not _same_event_candidate(candidate, existing):
                continue
            if not _same_angle_candidate(candidate, existing):
                continue
            duplicate_index = index
            break
        if duplicate_index is None:
            kept.append(candidate)
            continue
        preferred = _prefer_summary(candidate, kept[duplicate_index])
        kept[duplicate_index] = preferred
    return kept


def _summary_family_link(left: ClusterSummary, right: ClusterSummary) -> bool:
    if _summary_anchor_labels(left) & _summary_anchor_labels(right):
        return True
    return _same_event_candidate(left, right)


def _coherent_family_component(members: list[ClusterSummary]) -> list[ClusterSummary]:
    if not members:
        return []
    if not any(_summary_anchor_labels(member) for member in members):
        return members

    components: list[list[ClusterSummary]] = []
    seen: set[int] = set()
    for start_index, start_member in enumerate(members):
        if start_index in seen:
            continue
        stack = [start_index]
        component_indexes: list[int] = []
        while stack:
            index = stack.pop()
            if index in seen:
                continue
            seen.add(index)
            component_indexes.append(index)
            member = members[index]
            for neighbor_index, neighbor in enumerate(members):
                if neighbor_index in seen:
                    continue
                if _summary_family_link(member, neighbor):
                    stack.append(neighbor_index)
        components.append([members[index] for index in sorted(component_indexes)])

    components.sort(key=lambda group: (-len(group), members.index(group[0])))
    return components[0]


def _validated_family_name(members: list[ClusterSummary], max_chars: int) -> str:
    if not members:
        return "全球焦点"
    stable_name = _best_storyline_label(
        [
            member.storyline_name
            for member in members
        ]
        + [
            member.macro_topic_name
            for member in members
        ]
        + [
            member.short_topic_name
            for member in members
        ],
        max_chars,
    )
    if stable_name:
        anchor_name = _best_storyline_label(
            [label for member in members for label in member.storyline_anchor_labels],
            max_chars,
        )
        if anchor_name and len(anchor_name) <= len(stable_name):
            return anchor_name
        return stable_name

    anchor_name = _best_storyline_label(
        [label for member in members for label in member.storyline_anchor_labels],
        max_chars,
    )
    if anchor_name:
        return anchor_name

    return _normalize_storyline_name(
        members[0].storyline_name or members[0].macro_topic_name or members[0].short_topic_name,
        members[0],
        max_chars,
    )


def select_report_clusters(
    clusters: list[ArticleCluster],
    cfg: Config,
    _source_regions: dict[str, str] | None = None,
) -> tuple[list[ArticleCluster], list[ArticleCluster]]:
    hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
    main_limit = cfg.clustering.get("max_clusters_per_report", 20)

    _reset_hot_topic_metadata(clusters)
    if not hot_cfg.get("enabled", False):
        return [], clusters[:main_limit]

    max_topic_tabs = hot_cfg.get("max_topic_tabs", 3)
    min_items_per_topic = hot_cfg.get("min_items_per_topic", 5)

    families: dict[str, dict[str, object]] = {}
    for index, cluster in enumerate(clusters):
        key = _storyline_group_key(cluster, index)
        info = families.setdefault(
            key,
            {
                "clusters": [],
                "first_index": index,
                "storyline_name": cluster.storyline_name or cluster.macro_topic_name or cluster.topic_category,
                "macro_topic_icon_key": cluster.macro_topic_icon_key or _DEFAULT_HOT_TOPIC_ICON_KEY,
            },
        )
        info["clusters"].append(cluster)

    ranked_families = [
        family
        for family in families.values()
        if len([cluster for cluster in family["clusters"] if _cluster_is_hot_member(cluster)]) >= min_items_per_topic
        and any(
            cluster.storyline_role == "core"
            for cluster in family["clusters"]
            if _cluster_is_hot_member(cluster)
        )
    ]
    ranked_families.sort(
        key=lambda family: (
            -len([cluster for cluster in family["clusters"] if _cluster_is_hot_member(cluster)]),
            int(family["first_index"]),
        ),
    )
    selected_families = ranked_families[:max_topic_tabs]
    hot_keys = {
        cluster.storyline_key or cluster.macro_topic_key or _storyline_group_key(cluster, index)
        for family in selected_families
        for index, cluster in enumerate(family["clusters"])
        if _cluster_is_hot_member(cluster)
    }

    hot_clusters: list[ArticleCluster] = []
    main_clusters: list[ArticleCluster] = []
    family_sizes = {
        cluster.storyline_key or cluster.macro_topic_key: len(
            [member for member in family["clusters"] if _cluster_is_hot_member(member)]
        )
        for family in selected_families
        for cluster in family["clusters"]
        if _cluster_is_hot_member(cluster)
    }
    for cluster in clusters:
        if (cluster.storyline_key in hot_keys) and _cluster_is_hot_member(cluster):
            cluster.is_hot_topic = True
            cluster.macro_topic_member_count = family_sizes.get(cluster.storyline_key, 0)
            hot_clusters.append(cluster)
            continue
        main_clusters.append(cluster)
        if len(main_clusters) >= main_limit:
            break

    return hot_clusters, main_clusters


def select_hot_topic_families(
    summaries: list[ClusterSummary],
    cfg: Config,
    _source_regions: dict[str, str] | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[ClusterSummary]]:
    hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
    max_name_chars = hot_cfg.get("tab_name_max_chars", 10)
    max_topic_tabs = hot_cfg.get("max_topic_tabs", 3)
    min_items_per_topic = hot_cfg.get("min_items_per_topic", 5)
    allowlist = list(hot_cfg.get("icon_allowlist", _DEFAULT_HOT_TOPIC_ALLOWLIST)) or list(_DEFAULT_HOT_TOPIC_ALLOWLIST)
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

    hot_grouped: dict[str, list[ClusterSummary]] = defaultdict(list)
    storyline_grouped: dict[str, list[ClusterSummary]] = defaultdict(list)
    group_order: dict[str, int] = {}
    group_names: dict[str, str] = {}
    group_icons: dict[str, str] = {}
    standalone_candidates: list[ClusterSummary] = []

    for index, summary in enumerate(summaries):
        if summary.freshness_state == "stale":
            continue
        if summary.macro_topic_key and _summary_is_hot_member(summary):
            storyline_grouped[summary.macro_topic_key].append(summary)
            group_order.setdefault(summary.macro_topic_key, index)
            group_names.setdefault(
                summary.macro_topic_key,
                _normalize_storyline_name(summary.storyline_name or summary.macro_topic_name, summary, max_name_chars),
            )
            group_icons.setdefault(
                summary.macro_topic_key,
                _normalize_icon_key(summary.macro_topic_icon_key, allowlist),
            )
            if summary.is_hot_topic:
                hot_grouped[summary.macro_topic_key].append(summary)
            continue
        standalone_candidates.append(summary)

    hot_keys = [
        key
        for key, members in hot_grouped.items()
        if len(members) >= min_items_per_topic
        and any(
            _summary_is_hot_member(member)
            and (member.storyline_role or getattr(member.cluster, "storyline_role", "none")) == "core"
            for member in members
        )
    ]
    hot_keys.sort(key=lambda key: (-len(hot_grouped[key]), group_order.get(key, 0)))
    hot_keys = hot_keys[:max_topic_tabs]

    hot_topics: list[dict[str, object]] = []
    assigned_hot_keys: set[str] = set()
    for position, key in enumerate(hot_keys, 1):
        members = hot_grouped[key]
        validated_members = _coherent_family_component(members)
        if len(validated_members) < min_items_per_topic:
            logger.info(
                "Hot topic family %s suppressed after coherence validation: %d -> %d members",
                key,
                len(members),
                len(validated_members),
            )
            continue
        assigned_hot_keys.add(key)
        hot_topics.append(
            {
                "dom_id": f"hot-topic-{position}",
                "macro_topic_key": key,
                "macro_topic_name": _validated_family_name(validated_members, max_name_chars),
                "storyline_key": key,
                "storyline_name": _validated_family_name(validated_members, max_name_chars),
                "topic_icon_key": group_icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
                "anchor_labels": list(validated_members[0].storyline_anchor_labels) if validated_members else [],
                "member_count": len(validated_members),
                "summaries": validated_members,
            }
        )

    focus_storylines: list[dict[str, object]] = []
    assigned_focus_keys: set[str] = set()
    for key, members in storyline_grouped.items():
        if key in assigned_hot_keys:
            continue
        validated_members = _coherent_family_component(members)
        if len(validated_members) < 2 or len(validated_members) >= min_items_per_topic:
            continue
        if not any(
            (member.storyline_role or getattr(member.cluster, "storyline_role", "none")) == "core"
            for member in validated_members
        ):
            continue
        for summary in validated_members:
            summary.is_hot_topic = False
            summary.cluster.is_hot_topic = False
        assigned_focus_keys.add(key)
        focus_storylines.append(
            {
                "storyline_key": key,
                "storyline_name": _validated_family_name(validated_members, max_name_chars),
                "topic_icon_key": group_icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
                "member_count": len(validated_members),
                "summaries": validated_members,
            }
        )

    focus_storylines.sort(
        key=lambda family: (
            -int(family["member_count"]),
            group_order.get(str(family["storyline_key"]), 0),
        ),
    )

    main_candidates = list(standalone_candidates)
    for key, members in storyline_grouped.items():
        if key in assigned_hot_keys or key in assigned_focus_keys:
            for summary in members:
                if summary in next(
                    (
                        family["summaries"]
                        for family in hot_topics + focus_storylines
                        if str(family.get("storyline_key") or family.get("macro_topic_key")) == key
                    ),
                    [],
                ):
                    continue
                summary.is_hot_topic = False
                summary.cluster.is_hot_topic = False
                main_candidates.append(summary)
            continue
        for summary in members:
            summary.is_hot_topic = False
            summary.cluster.is_hot_topic = False
            main_candidates.append(summary)

    main_summaries = _deduplicate_main_lane(main_candidates)[:main_limit]
    return hot_topics, focus_storylines, main_summaries


def _summary_primary_domain(summary: ClusterSummary) -> str:
    for article in summary.cluster.articles:
        parsed = urllib.parse.urlparse(article.url)
        domain = parsed.netloc.lower().removeprefix("www.").strip(".")
        if domain:
            return domain
    if summary.cluster.sources:
        return summary.cluster.sources[0].lower()
    return ""


def select_positive_energy_summaries(
    summaries: list[ClusterSummary],
    classifications: list[dict[str, object]],
    cfg: Config,
) -> list[ClusterSummary]:
    positive_cfg = cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}
    if not bool(positive_cfg.get("enabled", True)):
        return []

    min_items = max(1, int(positive_cfg.get("min_items", 3)))
    max_items = max(min_items, int(positive_cfg.get("max_items", 5)))
    min_confidence = float(positive_cfg.get("min_confidence", 0.55))

    by_index = {
        int(item.get("cluster_index", 0)): item
        for item in classifications
        if isinstance(item.get("cluster_index"), int)
    }
    candidates: list[tuple[float, int, ClusterSummary, dict[str, object]]] = []
    for index, summary in enumerate(summaries, 1):
        item = by_index.get(index)
        if item is None:
            continue
        positive = bool(item.get("positive"))
        fun = bool(item.get("fun"))
        low_conflict = bool(item.get("low_conflict"))
        confidence = max(0.0, min(1.0, float(item.get("confidence", 0.0) or 0.0)))
        if not low_conflict or not (positive or fun) or confidence < min_confidence:
            continue
        score = confidence + (0.2 if positive else 0.0) + (0.15 if fun else 0.0)
        candidates.append((score, index, summary, item))

    if len(candidates) < min_items:
        return []

    candidates.sort(key=lambda row: (-row[0], row[1]))
    selected: list[tuple[float, int, ClusterSummary, dict[str, object]]] = []
    selected_ids: set[int] = set()
    domains: set[str] = set()

    for candidate in candidates:
        _, index, summary, _item = candidate
        domain = _summary_primary_domain(summary)
        if domain and domain in domains:
            continue
        selected.append(candidate)
        selected_ids.add(index)
        if domain:
            domains.add(domain)
        if len(selected) >= max_items:
            break

    if len(selected) < min_items:
        for candidate in candidates:
            _, index, _summary, _item = candidate
            if index in selected_ids:
                continue
            selected.append(candidate)
            selected_ids.add(index)
            if len(selected) >= max_items or len(selected) >= min_items:
                break

    if len(selected) < min_items:
        return []

    selected.sort(key=lambda row: row[1])
    results: list[ClusterSummary] = []
    for score, _index, summary, item in selected[:max_items]:
        summary.positive_energy_score = round(score, 4)  # type: ignore[attr-defined]
        summary.positive_energy_reason = str(item.get("reason") or "").strip()  # type: ignore[attr-defined]
        results.append(summary)
    return results


def _warn_on_storyline_near_miss(clusters: list[ArticleCluster], hot_keys: set[str], stage: str) -> None:
    grouped = _group_clusters_by_storyline(clusters)
    for key, members in grouped.items():
        if key in hot_keys or len(members) < 4:
            continue
        if not any(cluster.storyline_role == "core" for cluster in members):
            continue
        logger.warning(
            "%s: storyline near miss %s/%s with %d items; first headlines=%s",
            stage,
            key,
            members[0].storyline_name or members[0].macro_topic_name or members[0].topic_category,
            len(members),
            ", ".join(_cluster_storyline_headline(cluster) for cluster in members[:3]),
        )


def _warn_on_summary_storyline_near_miss(summaries: list[ClusterSummary], hot_keys: set[str], stage: str) -> None:
    grouped: dict[str, list[ArticleCluster]] = defaultdict(list)
    for summary in summaries:
        if summary.freshness_state == "stale":
            continue
        grouped[summary.storyline_key or summary.macro_topic_key or ""].append(summary.cluster)
    _warn_on_storyline_near_miss(
        [cluster for members in grouped.values() for cluster in members],
        hot_keys,
        stage,
    )


class Scheduler:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        init_db()
        self._pipeline_lock = asyncio.Lock()
        self.schedule_timezone = ZoneInfo(cfg.schedule.get("timezone", "Europe/Warsaw"))
        self.output_dir = Path(cfg.output.get("html_dir", "output"))
        self.staging_dir = self._resolve_output_path(cfg.output.get("staging_dir"), default="staging")
        self.publish_complete_flag = self.staging_dir / cfg.output.get("publish_complete_flag", ".publish_complete")
        self.push_retry_cfg = cfg.schedule.get("push_retry", {})
        self.push_retry_enabled = bool(self.push_retry_cfg.get("enabled", True))
        self.push_retry_max_attempts = int(self.push_retry_cfg.get("max_attempts", 3))
        self.push_retry_interval_minutes = int(self.push_retry_cfg.get("retry_interval_minutes", 5))
        self._apscheduler: AsyncIOScheduler | None = None

        self.collector = Collector(cfg)
        self.tagger = TopicTagger(cfg)
        self.deduplicator = Deduplicator(cfg)
        self.clusterer = Clusterer(cfg)
        self.cluster_validator = EventClusterValidator(cfg)
        self.seeker = ActiveSeeker(cfg)
        self.summarizer = Summarizer(cfg)
        self.freshness_evaluator = FreshnessEvaluator(cfg)
        self.storyline_resolver = StorylineResolver(
            cfg,
            summarizer=self.summarizer,
            similarity_fn=self.freshness_evaluator.score_text_to_historical_cluster,
        )
        self.publisher = TelegramPublisher(cfg)
        self.renderer = HtmlRenderer(
            output_dir=cfg.output.get("html_dir", "output"),
            source_regions={s.name: s.region for s in cfg.sources},
        )
        self.renderer.day_navigation_cfg = cfg.output.get("day_navigation", {}) if isinstance(cfg.output, dict) else {}

    def _resolve_output_path(self, configured: str | None, default: str) -> Path:
        path = Path(configured or default)
        if path.is_absolute():
            return path
        root_parts = self.output_dir.parts
        if root_parts and path.parts[: len(root_parts)] == root_parts:
            return path
        return self.output_dir / path

    @property
    def _staging_subdir(self) -> Path:
        try:
            return self.staging_dir.relative_to(self.output_dir)
        except ValueError as exc:
            raise ValueError("output.staging_dir must be inside output.html_dir") from exc

    def _staging_report_dir(self, report_date: date) -> Path:
        return self.staging_dir / report_date.isoformat()

    def _write_publish_complete(self, report_date: date, total_story_count: int) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "report_date": report_date.isoformat(),
            "total_story_count": total_story_count,
            "created_at": datetime.now(tz=self.schedule_timezone).isoformat(),
        }
        self.publish_complete_flag.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        self.publish_complete_flag.chmod(0o644)

    def _read_publish_complete(self) -> dict[str, object] | None:
        if not self.publish_complete_flag.exists():
            return None
        raw = self.publish_complete_flag.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"report_date": raw}
        if not isinstance(payload, dict):
            return None
        return payload

    def _clear_publish_complete(self) -> None:
        if self.publish_complete_flag.exists():
            self.publish_complete_flag.unlink()

    def _is_publish_complete(self, report_date: date) -> bool:
        payload = self._read_publish_complete()
        if not payload:
            return False
        return payload.get("report_date") == report_date.isoformat()

    def _load_staged_render_payload(self, report_date: date) -> dict[str, object]:
        data_path = self._staging_report_dir(report_date) / "data.json"
        return json.loads(data_path.read_text(encoding="utf-8"))

    def _promote_staged_report(self, report_date: date) -> Path:
        staged_dir = self._staging_report_dir(report_date)
        final_dir = self.output_dir / report_date.isoformat()
        if not staged_dir.exists():
            raise FileNotFoundError(f"staged report directory missing: {staged_dir}")
        if final_dir.exists() or final_dir.is_symlink():
            if final_dir.is_symlink() or final_dir.is_file():
                final_dir.unlink()
            else:
                shutil.rmtree(final_dir)
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staged_dir), str(final_dir))
        return final_dir

    def _promote_latest_symlink(self, report_date: date, total_story_count: int) -> None:
        if total_story_count <= 0:
            logger.info(
                "Push promotion: staged report has zero stories for %s — preserving existing latest symlink",
                report_date.isoformat(),
            )
            return
        latest = self.output_dir / "latest"
        if latest.is_symlink() or latest.is_file():
            latest.unlink()
        elif latest.exists():
            shutil.rmtree(latest)
        try:
            latest.symlink_to(report_date.isoformat())
        except OSError:
            logger.warning("Push promotion: failed to update latest symlink", exc_info=True)

    def _schedule_push_retry(self, report_date: date, attempt: int) -> bool:
        if not self.push_retry_enabled or self._apscheduler is None:
            return False
        if attempt >= self.push_retry_max_attempts:
            return False
        retry_attempt = attempt + 1
        run_at = datetime.now(tz=self.schedule_timezone) + timedelta(minutes=self.push_retry_interval_minutes)
        job_id = f"push_retry_{report_date.isoformat()}_{retry_attempt}"
        self._apscheduler.add_job(
            partial(self.push, report_date=report_date, attempt=retry_attempt),
            DateTrigger(run_date=run_at, timezone=self.schedule_timezone),
            id=job_id,
            replace_existing=True,
        )
        logger.warning(
            "Push retry scheduled: report_date=%s attempt=%d run_at=%s",
            report_date.isoformat(),
            retry_attempt,
            run_at.isoformat(),
        )
        return True

    def _cleanup_old_staging(self) -> None:
        if not self.staging_dir.exists():
            return
        today_str = date.today().isoformat()
        for child in self.staging_dir.iterdir():
            if child == self.publish_complete_flag:
                continue
            if child.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", child.name) and child.name != today_str:
                shutil.rmtree(child, ignore_errors=True)
        payload = self._read_publish_complete()
        if payload and payload.get("report_date") != today_str:
            self._clear_publish_complete()

    # ─── PHASES ──────────────────────────────────────────────────────────────

    async def collect(self, mode: str = "full") -> None:
        """Phase 1: Collect → tag → dedup → persist."""
        phase_name = "COLLECT_DELTA" if mode == "delta" else "COLLECT"
        async with self._pipeline_lock:
            started = time.perf_counter()
            logger.info("=== %s phase started ===", phase_name)

            raw_articles = await self.collector.collect_all(mode=mode)
            db_articles = raw_to_articles(raw_articles)

            tagged = self.tagger.tag_all(db_articles)
            deduped = self.deduplicator.deduplicate(tagged)

            saved = 0
            for article in deduped:
                article_id = insert_article(article)
                if article_id is not None:
                    article.id = article_id
                    if article.embedding:
                        update_article_embedding(article_id, article.embedding)
                    saved += 1

            logger.info(
                "=== %s done: %d new articles saved (raw=%d deduped=%d duration_s=%.2f) ===",
                phase_name,
                saved,
                len(raw_articles),
                len(deduped),
                time.perf_counter() - started,
            )

    async def publish(
        self,
        report_date: date | None = None,
        articles_override: list | None = None,
        push_after_render: bool = True,
    ) -> None:
        """Phase 2: Cluster → summarize → render report, then optionally push Telegram."""
        async with self._pipeline_lock:
            started = time.perf_counter()
            phase_name = "PUBLISH_STAGE" if not push_after_render else "PUBLISH"
            logger.info("=== %s phase started ===", phase_name)
            today = report_date or date.today()

            if articles_override is None:
                max_age_hours = self.cfg.clustering.get("time_window_hours", 48)
                articles = get_unclustered_articles(max_age_hours=max_age_hours)
                logger.info(
                    "Publish input: %d unclustered articles found within %d hours",
                    len(articles),
                    max_age_hours,
                )
            else:
                articles = sorted(
                    articles_override,
                    key=lambda article: article.published_at,
                    reverse=True,
                )
                logger.info(
                    "Publish input override: %d replay articles for report_date=%s",
                    len(articles),
                    today.isoformat(),
                )
            if not articles:
                logger.warning("No unclustered articles found — skipping %s", phase_name.lower())
                return

            clusters = self.clusterer.cluster(articles)
            if not clusters:
                logger.warning("No clusters formed — skipping %s", phase_name.lower())
                return
            logger.info("Event cluster stage: %d clusters formed", len(clusters))

            clusters = self.cluster_validator.validate(clusters)
            logger.info("Event cluster validation stage: %d clusters after validation", len(clusters))

            hot_cfg = self.cfg.output.get("hot_topics", {}) if isinstance(self.cfg.output, dict) else {}
            candidate_window = hot_cfg.get(
                "candidate_window",
                self.cfg.clustering.get("max_clusters_per_report", 20) * 2,
            )
            candidate_window = max(candidate_window, self.cfg.clustering.get("max_clusters_per_report", 20))
            candidate_clusters = clusters[:candidate_window]
            logger.info(
                "Hot topic candidate window: %d of %d clusters retained before enrichment",
                len(candidate_clusters),
                len(clusters),
            )
            for index, cluster in enumerate(candidate_clusters):
                cluster._storyline_candidate_index = index  # type: ignore[attr-defined]

            if hot_cfg.get("enabled", False):
                history_window_days = hot_cfg.get("history_window_days", 5)
                historical_hot_topic_memory = get_recent_clusters(
                    days=history_window_days,
                    anchor_date=today.isoformat(),
                )
                logger.info(
                    "Storyline history stage: %d prior clusters from %d day window",
                    len(historical_hot_topic_memory),
                    history_window_days,
                )
                self.storyline_resolver.resolve(
                    candidate_clusters,
                    historical_hot_topic_memory,
                    today,
                )
                _log_storyline_stage("Storyline stage: resolved candidate families", candidate_clusters)
            else:
                _reset_hot_topic_metadata(candidate_clusters)

            hot_clusters, main_clusters = select_report_clusters(candidate_clusters, self.cfg)
            selected_clusters = hot_clusters + main_clusters
            hot_storyline_keys = {cluster.storyline_key or "" for cluster in hot_clusters}
            _warn_on_storyline_near_miss(candidate_clusters, hot_storyline_keys, "Storyline stage")
            _log_storyline_stage("Storyline stage: final candidate families before enrichment", selected_clusters)

            logger.info(
                "Selected %d hotspot candidate items and %d main candidates for enrichment/summarization (from %d total clusters; candidate window=%d)",
                len(hot_clusters),
                len(main_clusters),
                len(clusters),
                len(candidate_clusters),
            )

            # Phase 2.5: Actively seek missing perspectives using Tavily
            selected_clusters = self.seeker.enhance_clusters(selected_clusters)
            for cluster in selected_clusters:
                for article in cluster.articles:
                    if article.id is not None:
                        continue
                    article.id = insert_article(article)
                    if article.id is None and callable(get_article_id_by_url):
                        article.id = get_article_id_by_url(article.url)

            summaries = self.summarizer.summarize_all(selected_clusters)

            # Phase 2.6: Evaluate freshness against historical clusters
            historical = get_recent_clusters(
                days=self.cfg.dedup.get("window_days", 3),
                anchor_date=today.isoformat(),
            )
            logger.info("Freshness check: %d historical clusters from past %d days",
                        len(historical), self.cfg.dedup.get("window_days", 3))

            # Evaluate each cluster's freshness
            freshness_results = self.freshness_evaluator.classify_all(
                [(cs.cluster, cs.summary) for cs in summaries],
                historical,
            )

            # Filter out stale clusters and store freshness metadata
            kept_summaries: list[ClusterSummary] = []
            stats = {"new": 0, "developing": 0, "stale": 0}

            for cs, (cluster, summary, freshness) in zip(summaries, freshness_results):
                stats[freshness.state] += 1

                if freshness.state == "stale":
                    logger.info("Skipping stale cluster: %s", cs.summary[:60])
                    continue

                # Attach freshness metadata to the ClusterSummary for rendering
                cs.freshness_state = freshness.state
                cs.continues_cluster_id = freshness.continues_cluster_id

                # Store cluster with freshness metadata
                cluster_record = Cluster(
                    topic_category=cs.cluster.topic_category,
                    article_ids=[a.id for a in cs.cluster.articles if a.id],
                    summary=cs.summary,
                    perspectives=cs.perspectives,
                    report_date=today.isoformat(),
                    freshness_state=freshness.state,
                    continues_cluster_id=freshness.continues_cluster_id,
                    storyline_key=cs.cluster.storyline_key,
                    storyline_name=cs.cluster.storyline_name,
                    storyline_role=cs.cluster.storyline_role,
                    storyline_confidence=cs.cluster.storyline_confidence,
                )
                insert_cluster(cluster_record)
                mark_articles_clustered([a.id for a in cs.cluster.articles if a.id])

                kept_summaries.append(cs)

            logger.info(
                "Freshness results: %d new, %d developing, %d stale (filtered)",
                stats["new"], stats["developing"], stats["stale"],
            )

            hot_topics, focus_storylines, main_summaries = select_hot_topic_families(kept_summaries, self.cfg)
            positive_summaries: list[ClusterSummary] = []
            positive_cfg = self.cfg.output.get("positive_energy", {}) if isinstance(self.cfg.output, dict) else {}
            if bool(positive_cfg.get("enabled", True)) and main_summaries:
                positive_classifications = self.summarizer.classify_positive_energy(main_summaries)
                positive_summaries = select_positive_energy_summaries(
                    main_summaries,
                    positive_classifications,
                    self.cfg,
                )
            english_cfg = self.cfg.output.get("english", {}) if isinstance(self.cfg.output, dict) else {}
            if bool(english_cfg.get("enabled", False)):
                self.summarizer.translate_report_content(
                    kept_summaries,
                    hot_topics=hot_topics,
                    focus_storylines=focus_storylines,
                )
            focus_storyline_story_count = sum(
                len(family.get("summaries", []))
                for family in focus_storylines
                if isinstance(family.get("summaries"), list)
            )
            hot_topic_story_count = sum(
                len(family.get("summaries", []))
                for family in hot_topics
                if isinstance(family.get("summaries"), list)
            )
            total_story_count = len(main_summaries) + focus_storyline_story_count + hot_topic_story_count
            hot_storyline_keys = {
                str(family.get("macro_topic_key", ""))
                for family in hot_topics
                if isinstance(family.get("macro_topic_key"), str)
            }
            _warn_on_summary_storyline_near_miss(kept_summaries, hot_storyline_keys, "Storyline stage after freshness")
            _log_storyline_stage(
                "Storyline stage: final families after freshness",
                [summary.cluster for summary in kept_summaries],
            )

            logger.info(
                "Story display groups: %d hot topics, %d focus storylines, %d remaining for main report (cap=%d)",
                len(hot_topics),
                len(focus_storylines),
                len(main_summaries),
                self.cfg.clustering.get("max_clusters_per_report", 20),
            )
            logger.info(
                "Render input: %d kept stories after freshness (%d main report, %d focus storyline stories, %d hot topic stories)",
                total_story_count,
                len(main_summaries),
                focus_storyline_story_count,
                hot_topic_story_count,
            )

            html_path = self.renderer.render(
                main_summaries,
                today,
                hot_topics=hot_topics,
                focus_storylines=focus_storylines,
                positive_summaries=positive_summaries,
                report_subdir=self._staging_subdir if not push_after_render else None,
                update_latest=push_after_render,
            )
            if push_after_render:
                await self.publisher.publish(main_summaries, today)
                logger.info(
                    "Report latest promotion: %s",
                    "updated latest symlink" if total_story_count > 0 else "kept dated-only output; latest unchanged",
                )
            else:
                self._write_publish_complete(today, total_story_count)
                logger.info(
                    "Report staged for push: report=%s flag=%s latest=unchanged",
                    html_path,
                    self.publish_complete_flag,
                )

            logger.info(
                "=== %s done: %d clusters (%d stale filtered, %d hotspot tabs), report at %s (duration_s=%.2f) ===",
                phase_name,
                len(summaries),
                stats["stale"],
                len(hot_topics),
                html_path,
                time.perf_counter() - started,
            )

    async def push(self, report_date: date | None = None, attempt: int = 0) -> None:
        """Promote staged report output and send the Telegram digest."""
        started = time.perf_counter()
        today = report_date or date.today()
        staged_dir = self._staging_report_dir(today)

        if not self._is_publish_complete(today):
            logger.warning(
                "Push skipped: staged report not ready for %s (attempt=%d)",
                today.isoformat(),
                attempt,
            )
            if not self._schedule_push_retry(today, attempt):
                logger.error("Push failed: no completed staged report for %s", today.isoformat())
            return

        async with self._pipeline_lock:
            if not self._is_publish_complete(today):
                logger.warning(
                    "Push re-check failed: staged report no longer ready for %s (attempt=%d)",
                    today.isoformat(),
                    attempt,
                )
                if not self._schedule_push_retry(today, attempt):
                    logger.error("Push failed after re-check: staged report missing for %s", today.isoformat())
                return

            logger.info("=== PUSH phase started: report_date=%s attempt=%d ===", today.isoformat(), attempt)
            payload = self._load_staged_render_payload(today)
            total_story_count = int(payload.get("total_cluster_count", 0) or 0)
            data_path = staged_dir / "data.json"
            final_dir = self._promote_staged_report(today)
            self._promote_latest_symlink(today, total_story_count)
            await self.publisher.publish_rendered(final_dir / "data.json", today)
            self._clear_publish_complete()
            logger.info(
                "=== PUSH done: report=%s source=%s total_story_count=%d duration_s=%.2f ===",
                final_dir / "index.html",
                data_path,
                total_story_count,
                time.perf_counter() - started,
            )

    async def replay(self, report_date: date | None = None, dry_run: bool = False) -> None:
        """Reset one report date's article set and rerun publish from that exact set."""
        target_date = report_date or date.today()
        target_date_str = target_date.isoformat()
        logger.info("=== REPLAY started for report_date=%s dry_run=%s ===", target_date_str, dry_run)

        article_ids = get_report_article_ids(target_date_str)
        cluster_count = len(get_clusters_for_date(target_date_str))
        if not article_ids:
            logger.warning("Replay: no clusters found for report_date=%s; nothing to reset", target_date_str)
            return

        logger.info(
            "Replay target: report_date=%s cluster_rows=%d article_ids=%d",
            target_date_str,
            cluster_count,
            len(article_ids),
        )
        if dry_run:
            logger.info("Replay dry-run: no DB changes applied for report_date=%s", target_date_str)
            return

        deleted_clusters = delete_clusters_for_date(target_date_str)
        reset_articles = reset_articles_clustered(article_ids)
        replay_articles = get_articles_by_ids(article_ids)

        logger.info(
            "Replay reset applied: deleted_clusters=%d reset_articles=%d",
            deleted_clusters,
            reset_articles,
        )
        logger.info(
            "Replay publish start: report_date=%s article_count=%d",
            target_date_str,
            len(replay_articles),
        )
        await self.publish(report_date=target_date, articles_override=replay_articles, push_after_render=True)
        logger.info("=== REPLAY done for report_date=%s ===", target_date_str)

    async def run_once(self) -> None:
        """Full pipeline in one go (useful for testing / manual runs)."""
        logger.info("=== RUN_ONCE started ===")
        try:
            logger.info("RUN_ONCE boundary: before collect")
            await self.collect(mode="full")
            logger.info("RUN_ONCE boundary: after collect")
            logger.info("RUN_ONCE boundary: before publish")
            await self.publish(push_after_render=True)
            logger.info("RUN_ONCE boundary: after publish")
            logger.info("=== RUN_ONCE done ===")
        except Exception:
            logger.exception("RUN_ONCE failed")
            raise

    # ─── LONG-RUNNING SCHEDULER ──────────────────────────────────────────────

    def start(self) -> None:
        """Start APScheduler with configured cron times."""
        try:
            asyncio.run(self._run_scheduler())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")

    async def _run_scheduler(self) -> None:
        """Async scheduler loop — runs inside asyncio.run()."""
        tz = self.cfg.schedule.get("timezone", "Asia/Shanghai")
        sched = AsyncIOScheduler(timezone=tz)
        self._apscheduler = sched
        self._cleanup_old_staging()
        full_collect_cron = self.cfg.schedule.get(
            "full_collect_cron",
            self.cfg.schedule.get("collect_cron", "0 */4 * * *"),
        )
        delta_collect_cron = self.cfg.schedule.get("prepublish_collect_cron")
        publish_cron = self.cfg.schedule.get("publish_cron", "30 7 * * *")
        push_cron = self.cfg.schedule.get("push_cron", "0 8 * * *")

        sched.add_job(
            partial(self.collect, mode="full"),
            CronTrigger.from_crontab(full_collect_cron, timezone=tz),
            id="collect_full",
        )
        if delta_collect_cron:
            sched.add_job(
                partial(self.collect, mode="delta"),
                CronTrigger.from_crontab(delta_collect_cron, timezone=tz),
                id="collect_delta",
            )
        sched.add_job(
            partial(self.publish, push_after_render=False),
            CronTrigger.from_crontab(publish_cron, timezone=tz),
            id="publish_stage",
        )
        sched.add_job(
            self.push,
            CronTrigger.from_crontab(push_cron, timezone=tz),
            id="push_daily",
        )

        sched.start()
        logger.info(
            "Scheduler started. full_collect=%s delta_collect=%s publish_stage=%s push=%s tz=%s",
            full_collect_cron,
            delta_collect_cron,
            publish_cron,
            push_cron,
            tz,
        )

        # Block until cancelled (KeyboardInterrupt → asyncio.run cancels all tasks)
        await asyncio.Event().wait()
