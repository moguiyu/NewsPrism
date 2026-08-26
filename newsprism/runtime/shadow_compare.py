"""Shadow comparison for published report JSON payloads.

This module compares a baseline report (e.g. today's known-good output)
against a candidate report produced by a changed pipeline. It is used by
scripts/shadow_compare.py and by tests to enforce output-quantity and quality
constraints during LLM cost-reduction work.

Layer: runtime (imports service/language; never imports repo)
"""
from __future__ import annotations

from typing import Any

from newsprism.service.language import looks_like_chinese_text

# Product constraints from the approved spec.
MAIN_LANE_MIN = 10
MAIN_LANE_MAX = 15
HOT_TOPIC_MAX = 3
HOT_TOPIC_MIN_STORIES = 3  # strict ">2" stored as minimum inclusive count 3


def _real_article_url(article: dict[str, Any]) -> str:
    url = article.get("url") or ""
    if url:
        return str(url)
    return f"{article.get('source', '')}:{article.get('title', '')}"


def _cluster_articles(cluster: dict[str, Any]) -> list[str]:
    articles = cluster.get("articles") or []
    seen: list[str] = []
    for article in articles:
        if article.get("is_placeholder"):
            continue
        key = _real_article_url(article)
        if key not in seen:
            seen.append(key)
    return seen


def _cluster_key(cluster: dict[str, Any]) -> str:
    return str(
        cluster.get("storyline_key")
        or cluster.get("macro_topic_key")
        or cluster.get("topic")
        or cluster.get("id")
        or ""
    )


def _language_issues(data: dict[str, Any]) -> list[str]:
    default_language = str(data.get("default_language") or "zh")
    issues: list[str] = []

    def check_cluster(cluster: dict[str, Any], location: str) -> None:
        if default_language == "zh":
            headline = str(cluster.get("headline") or "")
            summary = str(cluster.get("summary") or "")
            if headline and not looks_like_chinese_text(headline):
                issues.append(f"{location} headline is not Chinese: {headline[:60]}")
            if summary and not looks_like_chinese_text(summary):
                issues.append(f"{location} summary is not Chinese: {summary[:60]}")
        else:
            # For the English/default edition, prefer the explicit English fields
            # when present; the primary fields may remain Chinese in data.json.
            headline = str(
                cluster.get("headline_en") or cluster.get("headline") or ""
            )
            summary = str(
                cluster.get("summary_en") or cluster.get("summary") or ""
            )
            if headline and not any(ch.isascii() and ch.isalpha() for ch in headline):
                issues.append(f"{location} headline has no Latin text: {headline[:60]}")
            if summary and not any(ch.isascii() and ch.isalpha() for ch in summary):
                issues.append(f"{location} summary has no Latin text: {summary[:60]}")

    for index, cluster in enumerate(data.get("clusters") or [], 1):
        check_cluster(cluster, f"main#{index}")
    for ht_index, hot in enumerate(data.get("hot_topics") or [], 1):
        for member_index, cluster in enumerate(hot.get("clusters") or [], 1):
            check_cluster(cluster, f"hot#{ht_index}.{member_index}")
    return issues


def extract_report_metrics(data: dict[str, Any]) -> dict[str, Any]:
    """Extract comparable structural metrics from a rendered data.json payload."""
    clusters = data.get("clusters") or []
    hot_topics = data.get("hot_topics") or []

    main_lane_count = int(data.get("cluster_count") or len(clusters))
    hot_topic_count = int(data.get("hot_topic_count") or len(hot_topics))
    hot_topic_story_count = int(
        data.get("hot_topic_story_count")
        or sum(len(ht.get("clusters") or []) for ht in hot_topics)
    )

    cluster_keys = [_cluster_key(c) for c in clusters]
    hot_topic_keys = [
        str(ht.get("storyline_key") or ht.get("macro_topic_key") or "") for ht in hot_topics
    ]
    hot_topic_story_counts = [len(ht.get("clusters") or []) for ht in hot_topics]

    membership: dict[str, list[str]] = {}
    for cluster in clusters:
        key = _cluster_key(cluster)
        membership.setdefault(key, []).extend(_cluster_articles(cluster))

    hot_membership: dict[str, list[str]] = {}
    for hot in hot_topics:
        key = str(hot.get("storyline_key") or hot.get("macro_topic_key") or "")
        for cluster in hot.get("clusters") or []:
            hot_membership.setdefault(key, []).extend(_cluster_articles(cluster))

    return {
        "main_lane_count": main_lane_count,
        "hot_topic_count": hot_topic_count,
        "hot_topic_story_count": hot_topic_story_count,
        "hot_topic_story_counts": hot_topic_story_counts,
        "cluster_keys": cluster_keys,
        "hot_topic_keys": hot_topic_keys,
        "cluster_membership": membership,
        "hot_membership": hot_membership,
        "language_issues": _language_issues(data),
    }


def _jaccard(left: list[str], right: list[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set and not right_set:
        return 1.0
    union = left_set | right_set
    if not union:
        return 1.0
    return len(left_set & right_set) / len(union)


def _membership_jaccard(
    left: dict[str, list[str]],
    right: dict[str, list[str]],
) -> float:
    if not left and not right:
        return 1.0
    all_keys = set(left) | set(right)
    scores: list[float] = []
    for key in all_keys:
        scores.append(_jaccard(left.get(key, []), right.get(key, [])))
    if not scores:
        return 1.0
    return sum(scores) / len(scores)


def compare_reports(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    """Compare two report payloads and return a parity result dict."""
    base = extract_report_metrics(baseline)
    cand = extract_report_metrics(candidate)

    failures: list[str] = []

    # Output quantity constraints from product spec.
    if not (MAIN_LANE_MIN <= cand["main_lane_count"] <= MAIN_LANE_MAX):
        failures.append(
            f"main-lane count {cand['main_lane_count']} outside {MAIN_LANE_MIN}-{MAIN_LANE_MAX}"
        )
    if cand["hot_topic_count"] > HOT_TOPIC_MAX:
        failures.append(f"hot-topic count {cand['hot_topic_count']} > {HOT_TOPIC_MAX}")
    for index, count in enumerate(cand["hot_topic_story_counts"], 1):
        if count < HOT_TOPIC_MIN_STORIES:
            failures.append(
                f"hot-topic #{index} has {count} stories, expected >= {HOT_TOPIC_MIN_STORIES}"
            )

    # Structural parity.
    cluster_jaccard = _jaccard(base["cluster_keys"], cand["cluster_keys"])
    hot_jaccard = _jaccard(base["hot_topic_keys"], cand["hot_topic_keys"])
    membership_jaccard = _membership_jaccard(
        base["cluster_membership"], cand["cluster_membership"]
    )
    hot_membership_jaccard = _membership_jaccard(
        base["hot_membership"], cand["hot_membership"]
    )

    # These are informational in count-based shadow validation. Exact cluster
    # identity is stochastic in an LLM pipeline; the hard product constraints
    # are the count/language checks above.
    structural_warnings: list[str] = []
    if cluster_jaccard < 0.90:
        structural_warnings.append(f"main-lane cluster key Jaccard {cluster_jaccard:.3f} < 0.90")
    if hot_jaccard < 0.95:
        structural_warnings.append(f"hot-topic key Jaccard {hot_jaccard:.3f} < 0.95")
    if membership_jaccard < 0.90:
        structural_warnings.append(f"main-lane membership Jaccard {membership_jaccard:.3f} < 0.90")
    if hot_membership_jaccard < 0.95:
        structural_warnings.append(f"hot-topic membership Jaccard {hot_membership_jaccard:.3f} < 0.95")

    # Language purity.
    language_issues = cand["language_issues"]
    if language_issues:
        failures.append(f"language purity issues: {language_issues[:3]}")

    return {
        "passed": not failures,
        "failures": failures,
        "structural_warnings": structural_warnings,
        "baseline": base,
        "candidate": cand,
        "metrics": {
            "main_lane_count": cand["main_lane_count"],
            "hot_topic_count": cand["hot_topic_count"],
            "hot_topic_story_count": cand["hot_topic_story_count"],
            "cluster_key_jaccard": cluster_jaccard,
            "hot_topic_key_jaccard": hot_jaccard,
            "membership_jaccard": membership_jaccard,
            "hot_membership_jaccard": hot_membership_jaccard,
            "language_purity_issues": language_issues,
        },
    }
