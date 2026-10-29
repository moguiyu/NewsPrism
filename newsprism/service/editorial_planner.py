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
from newsprism.service.categories import normalize_display_category
from newsprism.service.evidence import evidence_counts
from newsprism.service.perspectives import canonicalize_perspective_groups
from newsprism.types import ArticleCluster, ClusterSummary, EditorialReportPlan, is_real_article

logger = logging.getLogger(__name__)

_DEFAULT_HOT_TOPIC_ICON_KEY = "globe"
# Cross-language coverage of the same event (multilingual mpnet) sits a notch
# below same-language pairs, so 0.80 missed e.g. a zh/en NBA-final pair at 0.778.
# 0.75 caught those but missed a zh/kr/ja US-CPI pair at 0.7477 (08-13), where
# the market-reaction cluster duplicated the data-release cluster. 0.73 catches
# it while staying well above the 0.62–0.64 related-but-distinct storyline
# range, so genuinely different stories are not merged.
_DISPLAY_DEDUP_SIMILARITY = 0.73
# Soft floor for a healthy main lane. Checked twice: once on the pre-dedup
# candidate pool (cheap warning while selection is still adjustable) and again
# on the post-dedup lane, because display dedup can collapse the lane below the
# floor after the first check has already passed.
_MAIN_LANE_SOFT_FLOOR = 10


def _extract_markdown_headline(summary_text: str) -> str:
    for line in summary_text.splitlines():
        match = re.match(r"\*\*(.+?)\*\*", line.strip())
        if match:
            return match.group(1)
    return ""


def _jev_adjusted_composite(item: ArticleCluster | ClusterSummary) -> float:
    """Composite with the beta Jev importance audit blended in.

    Production leaves ``jev_importance`` unset, so this is exactly the existing
    composite there. In the beta stack the audit nudges the ranking toward
    stories the decision model considers consequential.
    """
    base = _composite(item)
    impact = getattr(item, "impact", None)
    if impact is None and isinstance(item, ClusterSummary):
        impact = getattr(item.cluster, "impact", None)
    jev = getattr(impact, "jev_importance", None) if impact is not None else None
    if jev is None:
        return base
    return 0.78 * base + 0.22 * float(jev)


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
    return normalize_display_category(category)


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


def _storyline_key(summary: ClusterSummary) -> str | None:
    """Storyline family key carried by the cluster, if the resolver set one."""
    key = getattr(summary.cluster, "storyline_key", None)
    return str(key) if key else None


def _family_key(summary: ClusterSummary) -> str | None:
    """Family key for dedup protection, or None for a non-member.

    A cluster only joins a family when the resolver actually placed it in one
    (``core``/``spillover``). Merely sharing a ``storyline_key`` with a
    same-run family is not membership: ``_apply_singleton`` re-adopts a
    historical key for a cluster the resolver deliberately kept standalone, and
    treating that as family membership hid near-duplicates from display dedup.

    ``summary.storyline_role`` defaults to the literal string ``"none"`` (not
    ``None``), so it must be treated as absent — otherwise it masks the
    authoritative value copied onto the cluster and every summary looks like a
    non-member. Callers that bypass ``select_hot_topic_families`` (notably
    direct ``resolve_display_duplicates`` tests) rely on the cluster fallback.
    """
    role = summary.storyline_role
    if role in (None, "", "none"):
        role = getattr(summary.cluster, "storyline_role", None)
    status = summary.storyline_membership_status
    if status in (None, "", "none"):
        status = getattr(summary.cluster, "storyline_membership_status", None)
    if not _is_hot_member(role, status):
        return None
    return _storyline_key(summary)


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
    if re.match(r"^\d[\d,.]*[A-Za-z]", candidate):
        candidate = ""
    if candidate:
        return candidate
    if summary is not None:
        headline = _extract_markdown_headline(summary.summary) or summary.cluster.topic_category or "全球焦点"
        headline = re.sub(r"\s+", "", headline)[:max_chars].strip(" -:：，,、。.；;")
        if headline:
            return headline
    return "全球焦点"


class EditorialPlanner:
    def __init__(self, cfg: Config, jev_client=None) -> None:
        self.cfg = cfg
        self.jev_client = jev_client
        # Source name -> configured region, for the evidence region fallback.
        # Only searched articles carry ``origin_region``; organically collected
        # ones do not, so without this every card reported "0 个地区".
        self.source_regions = {
            source.name: source.region for source in (getattr(cfg, "sources", None) or [])
        }
        # Beta-only: one outlet arriving as both domain and brand must not be
        # counted twice. Empty in production, so counts there are unchanged.
        self.source_aliases = dict(getattr(cfg, "source_aliases", {}) or {})

    def base_plan(
        self,
        kept_summaries: list[ClusterSummary],
        report_date=None,
    ) -> EditorialReportPlan:
        hot_topics, _unused_focus_families, regular_summaries = select_hot_topic_families(
            kept_summaries,
            self.cfg,
            jev_client=getattr(self, "jev_client", None),
            report_date=report_date,
        )
        return EditorialReportPlan(
            hot_topics=hot_topics,
            focus_storylines=[],
            regular_summaries=regular_summaries,
            positive_summaries=[],
        )

    def finalize(
        self,
        base_plan: EditorialReportPlan,
        positive_summaries: list[ClusterSummary] | None = None,
        report_date=None,
    ) -> EditorialReportPlan:
        positive = positive_summaries or []
        # The 正能量 lane is exclusive: a cluster claimed for it is removed from
        # every other lane (regular *and* families). Without this a cluster that
        # is also a storyline/hot-topic member appears twice in the display set,
        # collides with itself in dedup, and is suppressed out of all lanes.
        positive_ids = {id(summary) for summary in positive}
        hot_topics = _strip_summaries_from_families(base_plan.hot_topics, positive_ids)
        focus_storylines = _strip_summaries_from_families(base_plan.focus_storylines, positive_ids)
        regular_summaries = split_positive_energy_lane(base_plan.regular_summaries, positive)
        output_cfg = getattr(self, "cfg", None)
        beta_mode = bool(
            output_cfg.output.get("beta_mode", False)
            if output_cfg is not None and isinstance(output_cfg.output, dict)
            else False
        )
        hot_topics, focus_storylines, regular_summaries, positive = resolve_display_duplicates(
            hot_topics,
            focus_storylines,
            regular_summaries,
            positive,
            jev_client=getattr(self, "jev_client", None),
            report_date=report_date,
            cross_family_same_event=beta_mode,
            source_regions=getattr(self, "source_regions", None),
            source_aliases=getattr(self, "source_aliases", None),
        )
        # A tab is admitted with >=2 members, but display dedup can remove
        # members afterwards. Never ship a one-member tab: the survivor is a
        # standalone story and belongs in the main lane.
        multi_member_tabs: list[dict[str, object]] = []
        demoted: list[ClusterSummary] = []
        for family in hot_topics:
            members = family.get("summaries", [])
            if not isinstance(members, list):
                continue
            if len(members) >= 2:
                multi_member_tabs.append(family)
            else:
                demoted.extend(summary for summary in members if isinstance(summary, ClusterSummary))
        if demoted:
            if hasattr(self, "cfg"):
                regular_summaries = _rank_main_summaries(
                    regular_summaries + demoted,
                    self.cfg,
                    len(regular_summaries) + len(demoted),
                )
            else:
                regular_summaries = sorted(
                    regular_summaries + demoted,
                    key=_jev_adjusted_composite,
                    reverse=True,
                )
        return EditorialReportPlan(
            hot_topics=multi_member_tabs,
            focus_storylines=focus_storylines,
            regular_summaries=regular_summaries,
            positive_summaries=positive,
        )

    def plan(
        self,
        kept_summaries: list[ClusterSummary],
        positive_summaries: list[ClusterSummary] | None = None,
        report_date=None,
    ) -> EditorialReportPlan:
        return self.finalize(
            self.base_plan(kept_summaries, report_date=report_date),
            positive_summaries,
            report_date=report_date,
        )


# ─── CLUSTER-LEVEL SELECTION (pre-summary) ────────────────────────────────────

def _diverse_by_category(
    items: list,
    limit: int,
    max_per_category: int,
    drop_reasons: dict[int, str] | None = None,
) -> list:
    """Greedy top-N by composite with a per-display-category cap; spill if short.

    ``drop_reasons`` records why each loser lost. "lane_quota" is too coarse to
    review: a story turned away because World had used its eight slots and one
    turned away because every slot went to a higher score call for different
    fixes. Keys are ``id(item)`` because candidates are not hashable.
    """
    selected: list = []
    per_category: dict[str, int] = defaultdict(int)
    overflow: list = []
    for item in items:
        if len(selected) >= limit:
            if drop_reasons is not None:
                drop_reasons[id(item)] = "main_limit"
            continue
        category = _display_category(item)
        if category and per_category[category] >= max_per_category:
            overflow.append(item)
            if drop_reasons is not None:
                drop_reasons[id(item)] = f"category_cap:{category}"
            continue
        selected.append(item)
        if category:
            per_category[category] += 1
    for item in overflow:
        if len(selected) >= limit:
            break
        selected.append(item)
        if drop_reasons is not None:
            drop_reasons.pop(id(item), None)
    return selected


def select_report_clusters(
    clusters: list[ArticleCluster],
    cfg: Config,
    drop_reasons: dict[int, str] | None = None,
) -> tuple[list[ArticleCluster], list[ArticleCluster]]:
    """Split candidates into hot-topic family members and impact-ranked main clusters.

    ``drop_reasons`` (optional) is filled with ``id(cluster) -> reason`` for every
    candidate that did not make the page, so the audit trail can say whether the
    main limit or a display-category cap ended a story.
    """
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
        max_items_per_topic = max(1, int(hot_cfg.get("max_items_per_topic", 10)))
        families: dict[str, list[ArticleCluster]] = defaultdict(list)
        for cluster in ranked:
            if cluster.storyline_key and _cluster_is_hot_member(cluster):
                families[cluster.storyline_key].append(cluster)
        # Tab admission at cluster level mirrors select_hot_topic_families:
        # every family with ≥2 members claims a tab. (Issue #2 rec #4)
        eligible = [
            (key, members)
            for key, members in families.items()
            if len(members) >= 2
        ]
        eligible.sort(
            key=lambda item: (
                -min(len(item[1]), max_items_per_topic),
                -max(_composite(c) for c in item[1]),
            )
        )
        hot_keys = {key for key, _members in eligible[:max_topic_tabs]}
        hot_claimed: dict[str, int] = defaultdict(int)
        for cluster in ranked:
            key = cluster.storyline_key
            if key in hot_keys and _cluster_is_hot_member(cluster):
                if hot_claimed[key] >= max_items_per_topic:
                    # D4: overflow beyond the per-topic cap is dropped, not
                    # moved into the main lane.
                    continue
                cluster.is_hot_topic = True
                cluster.macro_topic_member_count = min(
                    len(families[key]), max_items_per_topic
                )
                hot_clusters.append(cluster)
                hot_claimed[key] += 1
            else:
                main_pool.append(cluster)
    else:
        main_pool = list(ranked)

    main_clusters = _diverse_by_category(main_pool, main_limit, max_per_category, drop_reasons)

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
    ranked = sorted(summaries, key=_jev_adjusted_composite, reverse=True)
    if limit <= 0:
        return []
    return _diverse_by_category(ranked, limit, max_per_category)


def select_hot_topic_families(
    summaries: list[ClusterSummary],
    cfg: Config,
    jev_client=None,
    report_date=None,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[ClusterSummary]]:
    hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
    max_name_chars = hot_cfg.get("tab_name_max_chars", 10)
    # Full-length family label for in-body headers (overview + topic stage).
    # Navigation tabs stay capped at tab_name_max_chars; body copy shows the
    # complete storyline title. The cap only guards against pathological
    # titles, not editorial truncation.
    max_name_full_chars = int(hot_cfg.get("tab_name_full_max_chars", 60))
    max_topic_tabs = hot_cfg.get("max_topic_tabs", 3)
    min_items_per_topic = hot_cfg.get("min_items_per_topic", 5)
    max_items_per_topic = max(1, int(hot_cfg.get("max_items_per_topic", 10)))
    main_limit = int(hot_cfg.get("main_lane_target", 15))

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

    # Tab admission: every storyline family with ≥2 members claims a tab.
    # (Issue #2 rec #4: country-conflict families that previously spilled into
    # the main lane — Iran-US, RU-UA — now get their own tab.) Single-member
    # families stay in the main lane; they are genuinely standalone events.
    # The "has a core member" gate is dropped: when same_conflict_different_event
    # glue produces a spillover-only family, we still want it visible as a tab.
    hot_keys = [
        key
        for key, members in grouped.items()
        if len(members) >= 2
    ]
    family_scores: dict[str, tuple[float, float, float]] = {}
    if jev_client is not None and jev_client.stage_enabled("hot_topics"):
        jev_cfg = jev_client.stage_options("hot_topics")
        max_families = max(1, int(jev_cfg.get("max_families", 8)))
        for key in hot_keys[:max_families]:
            members = grouped[key]
            name = str(
                members[0].storyline_name
                or members[0].macro_topic_name
                or getattr(members[0].cluster, "topic_category", "")
            )
            scored = jev_client.score_hot_family(
                name=name,
                members=[
                    {
                        "headline": _extract_markdown_headline(m.summary) or m.cluster.topic_category,
                        "summary": m.summary,
                        "sources": [str(s) for s in m.cluster.sources],
                        "published": str(m.cluster.articles[0].published_at) if m.cluster.articles else "",
                    }
                    for m in members
                ],
                report_date=report_date,
                cluster_key=key,
            )
            if scored:
                family_scores[key] = scored
                logger.info(
                    "Jev hot-family score: key=%s name=%s importance=%.2f coherence=%.2f label_support=%.2f shadow=%s",
                    key,
                    name,
                    scored[0],
                    scored[1],
                    scored[2],
                    bool(getattr(jev_client, "shadow", True)),
                )
    if family_scores and not bool(getattr(jev_client, "shadow", True)):
        hot_keys.sort(
            key=lambda key: (
                -(
                    0.55 * family_scores.get(key, (0.0, 0.0, 0.0))[0]
                    + 0.30 * family_scores.get(key, (0.0, 0.0, 0.0))[1]
                    + 0.15 * family_scores.get(key, (0.0, 0.0, 0.0))[2]
                ),
                -min(len(grouped[key]), max_items_per_topic),
                group_order.get(key, 0),
            )
        )
    else:
        hot_keys.sort(
            key=lambda key: (
                -min(len(grouped[key]), max_items_per_topic),
                group_order.get(key, 0),
            )
        )
    hot_keys = hot_keys[:max_topic_tabs]

    hot_topics: list[dict[str, object]] = []
    for position, key in enumerate(hot_keys, 1):
        # D3/D4: cap each family at max_items_per_topic; overflow is dropped.
        members = sorted(grouped[key], key=_jev_adjusted_composite, reverse=True)[:max_items_per_topic]
        hot_topics.append(
            _build_hot_topic_family(
                key,
                members,
                position,
                group_icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
                max_name_chars,
                max_name_full_chars,
            )
        )

    main_candidates = list(standalone)
    assigned_keys = set(hot_keys)
    for key, members in grouped.items():
        if key in assigned_keys:
            continue
        members = sorted(members, key=_jev_adjusted_composite, reverse=True)
        for summary in members:
            summary.is_hot_topic = False
            summary.cluster.is_hot_topic = False
            main_candidates.append(summary)

    main_summaries = _rank_main_summaries(main_candidates, cfg, main_limit)
    if len(main_summaries) < 10:
        logger.warning(
            "Main-lane soft floor not reached: %d stories (< 10); no duplicate/backfill promotion",
            len(main_summaries),
        )
    return hot_topics, [], main_summaries


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
    if not selected:
        # The lane ran 33% empty over 2026-10-15..26 and each occurrence was
        # silent in the logs. Name the two ways it empties so the next empty day
        # is diagnosable from the log alone.
        near_miss = [
            summary
            for summary in kept_summaries
            if _feelgood(summary) > 0
            and _severity(summary) <= max_severity
            and _feelgood(summary) < min_feelgood
        ]
        near_miss.sort(key=lambda summary: -_feelgood(summary))
        highest = f"{_feelgood(near_miss[0]):.1f}" if near_miss else "none"
        logger.warning(
            "Positive energy lane is empty: %d kept summaries, %d carry any uplift "
            "(highest below the %.1f bar: %s). Check whether uplift stories were "
            "judged needs_review by the impact review floor.",
            len(kept_summaries),
            len(near_miss),
            min_feelgood,
            highest,
        )
    return selected


def split_positive_energy_lane(
    main_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
) -> list[ClusterSummary]:
    positive_ids = {id(summary) for summary in positive_summaries}
    return [summary for summary in main_summaries if id(summary) not in positive_ids]


def _build_hot_topic_family(
    key: str,
    members: list[ClusterSummary],
    position: int,
    icon_key: str,
    max_name_chars: int,
    max_name_full_chars: int,
) -> dict[str, object]:
    """Assemble one hot-topic tab dict.

    Extracted so a family rebuilt during re-admission (see
    :func:`readmit_hot_topic_families`) is byte-identical to the one the first
    pass would have produced.
    """
    lead = members[0] if members else None
    source_name = (lead.storyline_name or lead.macro_topic_name) if lead else None
    family_name = _normalize_storyline_name(source_name, lead, max_name_chars)
    family_name_full = _normalize_storyline_name(source_name, lead, max_name_full_chars)
    return {
        "dom_id": f"hot-topic-{position}",
        "macro_topic_key": key,
        "macro_topic_name": family_name,
        "macro_topic_name_full": family_name_full,
        "storyline_key": key,
        "storyline_name": family_name,
        "storyline_name_full": family_name_full,
        "topic_icon_key": icon_key,
        "anchor_labels": list(lead.storyline_anchor_labels) if lead else [],
        "member_count": len(members),
        "summaries": members,
    }


def readmit_hot_topic_families(
    hot_topics: list[dict[str, object]],
    regular_summaries: list[ClusterSummary],
    cfg: Config | None,
    *,
    min_members: int = 2,
    max_topic_tabs: int | None = None,
    max_items_per_topic: int | None = None,
) -> tuple[list[dict[str, object]], list[ClusterSummary], list[ClusterSummary], list[ClusterSummary]]:
    """Re-decide which storyline families own a tab, from the *surviving* set.

    Tab admission happens before display dedup and before the post-translation
    gate, so by render time a tab can be stale in two opposite ways:

    * **Shrunk** — a two-member family loses a member and ships with one card
      (2026-10-22 ``港股A股双双下跌`` shipped ``8427`` alone after the gate
      withheld ``8426``). A single card is a standalone story, not a topic.
    * **Starved** — the tab quota was spent on a family that later collapsed
      into another card, so a surviving multi-member family never got a tab
      (2026-10-22 France ``8438``/``8441``: quota went to ``single-c3664e0f``,
      which dedup then merged into ``8415``).

    Both are fixed by recomputing admission here instead of patching the two
    symptoms separately.

    Returns ``(hot_topics, regular_summaries, demoted, promoted)``. ``demoted``
    are members released back to the main lane, ``promoted`` are summaries
    pulled *out* of the main lane into a rebuilt tab, so callers can log the
    move instead of letting it vanish into a count.
    """
    hot_cfg = cfg.output.get("hot_topics", {}) if cfg is not None and isinstance(cfg.output, dict) else {}
    if max_topic_tabs is None:
        max_topic_tabs = int(hot_cfg.get("max_topic_tabs", 3))
    if max_items_per_topic is None:
        max_items_per_topic = max(1, int(hot_cfg.get("max_items_per_topic", 10)))
    max_name_chars = int(hot_cfg.get("tab_name_max_chars", 10))
    max_name_full_chars = int(hot_cfg.get("tab_name_full_max_chars", 60))

    # Keep each family's original icon and translations: re-admission decides
    # *which* families own a tab, it does not restyle or untranslate them. This
    # runs after the translation stage, so dropping the ``*_en`` keys here would
    # withhold the entire English edition (``_english_gaps`` reports
    # ``untranslated_family_label``).
    icons: dict[str, str] = {}
    translated_labels: dict[str, dict[str, str]] = {}
    for family in hot_topics:
        key = str(family.get("macro_topic_key") or family.get("storyline_key") or "")
        if not key:
            continue
        icon = family.get("topic_icon_key")
        if isinstance(icon, str) and icon:
            icons.setdefault(key, icon)
        for name, value in family.items():
            if name.endswith("_en") and isinstance(value, str) and value:
                translated_labels.setdefault(key, {}).setdefault(name, value)

    # Group every surviving summary by its family key. Main-lane members join
    # their family here: that is what lets a starved family be rebuilt, and it
    # keeps the two lanes from holding half a family each.
    families: dict[str, list[ClusterSummary]] = defaultdict(list)
    family_order: dict[str, int] = {}
    kept_main: list[ClusterSummary] = []
    for summary in regular_summaries:
        # ``_family_key`` — not ``_summary_is_hot_member`` — because the latter
        # treats the literal default ``"none"`` as a role and so reports every
        # summary as a non-member unless the role was copied over first.
        key = _family_key(summary)
        if not key:
            kept_main.append(summary)
            continue
        family_order.setdefault(key, len(family_order))
        families[key].append(summary)

    # Fold the surviving tab members into the same buckets. A family that lost
    # its quota to a collapsed family is only in the main lane, and must still
    # be able to win a tab back here.
    for family in hot_topics:
        key = str(family.get("macro_topic_key") or family.get("storyline_key") or "")
        if not key:
            continue
        members = [
            summary
            for summary in family.get("summaries", []) or []
            if isinstance(summary, ClusterSummary)
        ]
        if not members:
            continue
        bucket = families.setdefault(key, [])
        known = {id(summary) for summary in bucket}
        bucket.extend(summary for summary in members if id(summary) not in known)
        family_order.setdefault(key, len(family_order))

    admitted: dict[str, list[ClusterSummary]] = {
        key: members for key, members in families.items() if len(members) >= min_members
    }

    # Rank surviving families. A tab that still has two members keeps the slot
    # it already won (that admission may have come from Jev ranking); families
    # that never had one compete for whatever slots collapse freed, largest
    # first, then by strongest member.
    prior_rank: dict[str, int] = {}
    for position, family in enumerate(hot_topics):
        key = str(family.get("macro_topic_key") or family.get("storyline_key") or "")
        if key:
            prior_rank.setdefault(key, position)
    unranked = len(hot_topics)
    ranked = sorted(
        admitted.items(),
        key=lambda item: (
            prior_rank.get(item[0], unranked),
            -min(len(item[1]), max_items_per_topic),
            -max(_jev_adjusted_composite(summary) for summary in item[1]),
            family_order.get(item[0], 0),
        ),
    )
    # A family that wins a tab after translation has no stored English label, and
    # the English edition is all-or-nothing: a tab without ``macro_topic_name_en``
    # makes ``_english_gaps`` report ``untranslated_family_label`` and withholds
    # the whole edition. Recover the label from the lead member (translated in
    # the same pass, same source string) and skip the promotion if even that is
    # missing, rather than silently costing the English edition.
    english_enabled = bool(
        (cfg.output.get("english") or {}).get("enabled", False)
        if cfg is not None and isinstance(cfg.output, dict)
        else False
    )
    # Only enforce the label rule once the translation stage has actually run:
    # before it, every tab legitimately has no ``*_en`` key yet.
    translations_present = any(translated_labels.values())

    def _labels_for(key: str, members: list[ClusterSummary]) -> dict[str, str]:
        stored = translated_labels.get(key, {})
        if stored.get("macro_topic_name_en") and stored.get("storyline_name_en"):
            return stored
        lead = max(members, key=_jev_adjusted_composite)
        inherited = str(lead.storyline_name_en or lead.macro_topic_name_en or "").strip()
        if not inherited:
            return stored
        merged = dict(stored)
        merged.setdefault("macro_topic_name_en", inherited)
        merged.setdefault("storyline_name_en", inherited)
        merged.setdefault("macro_topic_name_full_en", inherited)
        merged.setdefault("storyline_name_full_en", inherited)
        return merged

    admitted_labels: dict[str, dict[str, str]] = {}
    for key, members in ranked:
        labels = _labels_for(key, members)
        # Only a *new* tab needs a label it never had. A tab that already shipped
        # keeps its slot: demoting it would not fix the missing translation, and
        # it would silently move stories between lanes.
        if (
            english_enabled
            and translations_present
            and key not in prior_rank
            and not labels.get("macro_topic_name_en")
        ):
            logger.warning(
                "Hot-topic re-admission skipped key=%s: no English tab label after translation",
                key,
            )
            continue
        admitted_labels[key] = labels

    selected = [(key, members) for key, members in ranked if key in admitted_labels][:max_topic_tabs]
    selected_keys = {key for key, _members in selected}

    was_hot = {
        id(summary)
        for family in hot_topics
        for summary in (family.get("summaries") or [])
        if isinstance(summary, ClusterSummary)
    }

    in_tab: list[ClusterSummary] = []
    rebuilt: list[dict[str, object]] = []
    for position, (key, members) in enumerate(selected, 1):
        members = sorted(members, key=_jev_adjusted_composite, reverse=True)[:max_items_per_topic]
        in_tab.extend(members)
        family = _build_hot_topic_family(
            key,
            members,
            position,
            icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
            max_name_chars,
            max_name_full_chars,
        )
        family.update(admitted_labels.get(key, {}))
        rebuilt.append(family)
    released: list[ClusterSummary] = [
        summary
        for key, members in families.items()
        if key not in selected_keys
        for summary in members
    ]

    promoted_ids = {id(summary) for summary in in_tab}
    # ``promoted`` is reported, not internal: only summaries that actually moved
    # from the main lane into a tab, so the log line stays meaningful.
    promoted = [summary for summary in in_tab if id(summary) not in was_hot]
    # Main lane = survivors that were never in a tab, minus anyone promoted
    # into one, plus members of families that lost their tab.
    next_main = [summary for summary in kept_main if id(summary) not in promoted_ids]
    seen = {id(summary) for summary in next_main}
    rejoined = [
        summary
        for summary in released
        if id(summary) not in seen and id(summary) not in promoted_ids
    ]
    if rejoined:
        # Released members rejoin the lane ranked the same way the first pass
        # ranked it, so a demoted card does not silently sink to the bottom.
        next_main = (
            _rank_main_summaries(next_main + rejoined, cfg, len(next_main) + len(rejoined))
            if cfg is not None
            else sorted(next_main + rejoined, key=_jev_adjusted_composite, reverse=True)
        )
    for summary in in_tab:
        summary.is_hot_topic = True
        summary.cluster.is_hot_topic = True
    for summary in released:
        summary.is_hot_topic = False
        summary.cluster.is_hot_topic = False

    # Only members that were in a tab before count as demoted; the rest merely
    # never had one, and reporting them as demotions would be misleading.
    moved_out = [summary for summary in released if id(summary) in was_hot]
    return rebuilt, next_main, moved_out, promoted


def _strip_summaries_from_families(
    families: list[dict[str, object]],
    drop_ids: set[int],
) -> list[dict[str, object]]:
    """Remove the given summaries from each family; drop families left empty."""
    kept: list[dict[str, object]] = []
    for family in families:
        family_summaries = family.get("summaries", [])
        if not isinstance(family_summaries, list):
            kept.append(family)
            continue
        remaining = [
            summary
            for summary in family_summaries
            if not (isinstance(summary, ClusterSummary) and id(summary) in drop_ids)
        ]
        if not remaining:
            continue
        family["summaries"] = remaining
        family["member_count"] = len(remaining)
        kept.append(family)
    return kept


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


def _merge_duplicate_summary(
    target: ClusterSummary,
    duplicate: ClusterSummary,
    reason: str,
    confidence: float,
    source_regions: dict[str, str] | None = None,
    source_aliases: dict[str, str] | None = None,
) -> None:
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

    target.grouped_perspectives = canonicalize_perspective_groups(
        target.grouped_perspectives
    )
    target.perspectives = {
        source: group.perspective
        for group in target.grouped_perspectives
        for source in group.sources
    }

    if target.evidence_summary:
        # The merged card gained articles/sources after the impact assessment
        # produced its evidence line; leaving the original counts in place made
        # the reader-facing "N 个来源、M 个地区" contradict the source list.
        # Count real articles only (cluster.sources also carries placeholder
        # outlet names) and use the same region lookup as ``evidence.py``, so
        # the two producers cannot drift again.
        sources, regions = evidence_counts(
            target.cluster, source_regions, source_aliases
        )
        target.evidence_summary = re.sub(
            r"^\s*\d+\s*个来源、\d+\s*个地区",
            f"{sources} 个来源、{regions} 个地区",
            target.evidence_summary,
            count=1,
        )

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


_CROSS_FAMILY_TOKEN_STOPWORDS = frozenset({
    "the", "and", "for", "with", "from", "that", "this", "news", "update",
    "美国", "中国", "全球", "今日", "最新", "消息", "报道", "事件", "表示", "已经",
})


def _event_entity_tokens(summary: ClusterSummary) -> set[str]:
    headline = _extract_markdown_headline(summary.summary) or summary.cluster.topic_category
    text = f"{headline}\n{summary.summary}"
    tokens = {
        token.lower()
        for token in re.findall(r"[A-Za-z][A-Za-z0-9.\-]{2,}", text)
        if token.lower() not in _CROSS_FAMILY_TOKEN_STOPWORDS
    }
    for sequence in re.findall(r"[\u4e00-\u9fff]{2,8}", text):
        for size in (2, 3, 4):
            tokens.update(
                sequence[index : index + size]
                for index in range(0, len(sequence) - size + 1)
                if sequence[index : index + size] not in _CROSS_FAMILY_TOKEN_STOPWORDS
            )
    return tokens


def _cross_family_same_event_candidate(left: ClusterSummary, right: ClusterSummary, similarity: float) -> bool:
    """Cheap recall-only candidate for beta's cross-family Jev adjudication.

    Embedding similarity can stay below the normal dedup floor when one family
    key absorbs one framing and another key absorbs a sibling framing (SpaceX
    101 vs 78 on 2026-10-21). Require a plausible similarity plus at least two
    shared surface entities; Jev still decides, and this only runs in beta.
    """
    if similarity < 0.40:
        return False
    shared = _event_entity_tokens(left) & _event_entity_tokens(right)
    return len(shared) >= 2


def resolve_display_duplicates(
    hot_topics: list[dict[str, object]],
    focus_storylines: list[dict[str, object]],
    regular_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
    jev_client=None,
    report_date=None,
    *,
    cross_family_same_event: bool = False,
    source_regions: dict[str, str] | None = None,
    source_aliases: dict[str, str] | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[ClusterSummary], list[ClusterSummary]]:
    displayed = _flatten_display_summaries(hot_topics, focus_storylines, regular_summaries, positive_summaries)
    positive_ids = {id(summary) for summary in positive_summaries}
    # Build summary → family-key index. Two summaries in the SAME storyline
    # family are never deduplicated against each other (Additional fix #2):
    # the storyline resolver already decided they belong to one ongoing story
    # (different daily incidents of the same conflict, different angles of one
    # event, etc.) — collapsing them defeats the purpose of the tab.
    family_id_by_summary: dict[int, str] = {}
    for family in hot_topics + focus_storylines:
        family_key = str(family.get("macro_topic_key") or family.get("storyline_key") or "")
        if not family_key:
            continue
        for summary in family.get("summaries", []) or []:
            if isinstance(summary, ClusterSummary):
                family_id_by_summary[id(summary)] = family_key
    centroids: dict[int, np.ndarray | None] = {}
    for summary in displayed:
        centroids[id(summary)] = _summary_centroid(summary)
        summary.duplicate_action = "kept"  # type: ignore[attr-defined]
        summary.duplicate_reason = ""  # type: ignore[attr-defined]
        summary.duplicate_confidence = 0.0  # type: ignore[attr-defined]

    suppressed_ids: set[int] = set()
    jev_pairs = 0
    cross_family_pairs = 0
    jev_cfg = jev_client.stage_options("display_dedup") if jev_client is not None else {}
    jev_max_pairs = max(0, int(jev_cfg.get("max_pairs", 0))) if jev_cfg else 0
    cross_family_max_pairs = max(0, int(jev_cfg.get("cross_family_max_pairs", 0))) if jev_cfg else 0
    jev_sim_min = float(jev_cfg.get("similarity_min", 0.62)) if jev_cfg else 0.62
    jev_rule_max = float(jev_cfg.get("rule_confidence_max", 0.88)) if jev_cfg else 0.88
    jev_merge = float(jev_cfg.get("merge_threshold", 0.76)) if jev_cfg else 0.76
    jev_veto = float(jev_cfg.get("veto_threshold", 0.34)) if jev_cfg else 0.34
    for left_index, left in enumerate(displayed):
        if id(left) in suppressed_ids:
            continue
        for right in displayed[left_index + 1 :]:
            if id(right) in suppressed_ids:
                continue
            # The planner only indexes families that claimed a hot-topic or
            # focus tab. Main-lane members of a storyline still belong to one
            # family, so fall back to the key the resolver stamped on the
            # cluster: without it, two members of one family were merged at
            # 0.78 embedding similarity (2026-09-13 DPRK missile cluster was
            # absorbed into the ROK submarine card).
            #
            # Only genuine family members are protected. A ``role=none`` cluster
            # re-adopted a *historical* key that a same-run component had already
            # claimed (2026-10-12: clusters 8140/8142 formed the real two-member
            # family ``single-67f6d8c1`` while detached singleton 8144 reused the
            # same key), and the shared-key exemption then let a third
            # near-duplicate card for one Houthi airport attack reach the main
            # lane. Membership, not key equality, is the family contract.
            left_fam = family_id_by_summary.get(id(left)) or _family_key(left)
            right_fam = family_id_by_summary.get(id(right)) or _family_key(right)
            duplicate, reason, confidence = _display_duplicate(left, right, centroids)
            is_family_pair = bool(left_fam and right_fam and left_fam == right_fam)
            if left_fam and right_fam and left_fam == right_fam:
                # Within one storyline, preserve distinct daily incidents. But
                # shared URLs are always duplicates, and multi-source cards
                # with near-identical event centroids are the same event split
                # into two clusters rather than legitimate storyline members.
                same_event_inside_family = bool(
                    duplicate
                    and (
                        reason == "shared_article_url"
                        or (confidence >= 0.90 and _source_overlap(left, right) >= 2)
                    )
                )
                if not same_event_inside_family:
                    continue
            cross_family_candidate = False
            if (
                cross_family_same_event
                and not is_family_pair
                and not duplicate
                and cross_family_pairs < cross_family_max_pairs
                and _cross_family_same_event_candidate(left, right, confidence)
            ):
                cross_family_candidate = True
                cross_family_pairs += 1
            # Jev adjudicates only the uncertain band: rule-duplicates below the
            # high-confidence bar can be vetoed, and near-threshold pairs the
            # embedding missed can be merged. High-confidence rules and family
            # members stay deterministic. In beta, cross-family entity-overlap
            # candidates below the embedding floor are also sent to Jev.
            if (
                jev_client is not None
                and not is_family_pair
                and jev_pairs < jev_max_pairs
                and (
                    duplicate and confidence <= jev_rule_max
                    or (not duplicate and confidence >= jev_sim_min)
                    or cross_family_candidate
                )
            ):
                jev_pairs += 1
                probability = jev_client.adjudicate_duplicate(
                    left={
                        "headline": _extract_markdown_headline(left.summary) or left.cluster.topic_category,
                        "summary": left.summary,
                        "sources": [str(source) for source in left.cluster.sources],
                        "published": str(left.cluster.articles[0].published_at) if left.cluster.articles else "",
                    },
                    right={
                        "headline": _extract_markdown_headline(right.summary) or right.cluster.topic_category,
                        "summary": right.summary,
                        "sources": [str(source) for source in right.cluster.sources],
                        "published": str(right.cluster.articles[0].published_at) if right.cluster.articles else "",
                    },
                    similarity=confidence,
                    report_date=report_date,
                    cluster_key=getattr(left.cluster, "storyline_key", None),
                )
                if probability is not None:
                    logger.info(
                        "Jev duplicate adjudication: p=%.2f rule=%s rule_confidence=%.2f shadow=%s",
                        probability,
                        "duplicate" if duplicate else "distinct",
                        confidence,
                        bool(getattr(jev_client, "shadow", True)),
                    )
                    if not bool(getattr(jev_client, "shadow", True)):
                        if duplicate and probability < jev_veto:
                            duplicate = False
                            reason = "jev_not_same_event"
                        elif not duplicate and probability >= jev_merge:
                            duplicate = True
                            reason = "jev_same_event"
                            confidence = max(confidence, probability)
            if not duplicate:
                continue
            # A cluster already claimed for the 正能量 lane keeps its slot; its
            # duplicate elsewhere is suppressed. feelgood carries no composite
            # weight, so without this the positive copy always loses the tiebreak
            # and the lane silently empties.
            left_positive = id(left) in positive_ids
            right_positive = id(right) in positive_ids
            if left_positive != right_positive:
                preferred = left if left_positive else right
            else:
                preferred = _prefer_summary(left, right)
            suppressed = right if preferred is left else left
            _merge_duplicate_summary(
                preferred, suppressed, reason, confidence, source_regions, source_aliases
            )
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
            # ``preferred`` just absorbed ``suppressed``'s articles, so it can
            # now share a URL with a card it was already compared against — and
            # that comparison happened before the merge, when the URL was not
            # there yet. 2026-10-22 beta: 127 absorbed 126, the merge handed it
            # the NPR/Davos URL that 129 also carries, but the 127/129 pair had
            # already been dismissed, so both cards shipped the same story.
            # Re-test against every surviving card, on either side of ``left``:
            # ``left`` keeps absorbing later cards, so the partner that newly
            # shares a URL is often one that has not been reached yet.
            for candidate in displayed:
                if id(preferred) in suppressed_ids:
                    break
                if candidate is preferred or candidate is suppressed:
                    continue
                if id(candidate) in suppressed_ids:
                    continue
                if not (_summary_urls(candidate) & _summary_urls(preferred)):
                    continue
                candidate_positive = id(candidate) in positive_ids
                preferred_positive = id(preferred) in positive_ids
                if candidate_positive != preferred_positive:
                    winner = candidate if candidate_positive else preferred
                else:
                    winner = _prefer_summary(candidate, preferred)
                loser = preferred if winner is candidate else candidate
                _merge_duplicate_summary(
                    winner, loser, "shared_article_url", 0.95, source_regions, source_aliases
                )
                suppressed_ids.add(id(loser))
                logger.info(
                    "Display duplicate suppressed after merge: kept='%s' suppressed='%s' reason=shared_article_url",
                    _extract_markdown_headline(winner.summary) or winner.cluster.topic_category,
                    _extract_markdown_headline(loser.summary) or loser.cluster.topic_category,
                )
                if loser is preferred:
                    # The absorbing card was itself absorbed; nothing left to
                    # re-test against.
                    break

    keep_ids = {id(summary) for summary in displayed} - suppressed_ids
    kept_hot = _filter_family_summaries(hot_topics, keep_ids)
    kept_focus = _filter_family_summaries(focus_storylines, keep_ids)
    kept_regular = [summary for summary in regular_summaries if id(summary) in keep_ids]
    kept_positive = [summary for summary in positive_summaries if id(summary) in keep_ids]

    # Funnel accounting. 2026-09-13 lost 4 of 22 surviving candidates between
    # dedup and render with no log line explaining where they went; this makes
    # every hand-off countable from the logs alone. `displayed` counts hot-topic
    # AND focus-storyline members, so the breakdown counts both as well: omitting
    # focus members logged `kept=0` for a family that was in fact kept, i.e. the
    # exact phantom-loss signature this line exists to expose.
    kept_hot_members = sum(len(family.get("summaries", [])) for family in kept_hot)
    kept_focus_members = sum(len(family.get("summaries", [])) for family in kept_focus)
    logger.info(
        "Display dedup accounting: displayed=%d suppressed=%d kept=%d "
        "(hot_topic_members=%d, focus_members=%d, main=%d, positive=%d)",
        len(displayed),
        len(suppressed_ids),
        kept_hot_members + kept_focus_members + len(kept_regular) + len(kept_positive),
        kept_hot_members,
        kept_focus_members,
        len(kept_regular),
        len(kept_positive),
    )
    # The pre-dedup soft-floor warning inside select_hot_topic_families cannot
    # see losses caused by dedup (or by the later publication gate), so a
    # collapsed main lane shipped silently: on 2026-10-12 the candidate pool
    # passed the first check, dedup plus the gate left 7 stories, and no warning
    # was ever emitted. Re-check the lane that actually survives.
    if len(kept_regular) < _MAIN_LANE_SOFT_FLOOR:
        logger.warning(
            "Main-lane soft floor not reached after display dedup: %d stories (< %d)",
            len(kept_regular),
            _MAIN_LANE_SOFT_FLOOR,
        )
    return kept_hot, kept_focus, kept_regular, kept_positive
