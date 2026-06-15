"""Pure aggregation over evaluation rows — filtering, matrices, trends, source
review, and tiny inline-SVG charts. No DB, no I/O: unit-testable directly.

Layer: runtime.
"""
from __future__ import annotations

from collections import defaultdict

DIMENSIONS = (
    "scope", "severity", "novelty", "actor_influence", "decision_relevance", "feelgood",
)


def filter_rows(
    rows: list[dict],
    *,
    categories: list[str] | None = None,
    statuses: list[str] | None = None,
    selection: str = "all",           # all | selected | candidate
    composite_min: float | None = None,
    composite_max: float | None = None,
    subject_regions: list[str] | None = None,
    has_feedback: bool | None = None,
) -> list[dict]:
    cats = set(categories or [])
    stats = set(statuses or [])
    subj = set(subject_regions or [])
    out = []
    for r in rows:
        if cats and r.get("display_category") not in cats:
            continue
        if stats and r.get("status") not in stats:
            continue
        if selection == "selected" and not r.get("selected"):
            continue
        if selection == "candidate" and r.get("selected"):
            continue
        if composite_min is not None and r.get("composite", 0.0) < composite_min:
            continue
        if composite_max is not None and r.get("composite", 0.0) > composite_max:
            continue
        if subj and not (subj & set(r.get("subject_regions") or [])):
            continue
        if has_feedback is True and r.get("verdict") is None:
            continue
        if has_feedback is False and r.get("verdict") is not None:
            continue
        out.append(r)
    return out


def matrix_category_dimension(rows: list[dict]) -> dict[str, dict[str, float]]:
    """Average each dimension within each display category."""
    sums: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    counts: dict[str, int] = defaultdict(int)
    for r in rows:
        cat = r.get("display_category") or "?"
        counts[cat] += 1
        for dim in DIMENSIONS:
            sums[cat][dim] += float((r.get("dims") or {}).get(dim, 0.0))
    return {
        cat: {dim: round(sums[cat][dim] / counts[cat], 1) for dim in DIMENSIONS}
        for cat in counts
    }


def matrix_subject_category(rows: list[dict]) -> dict[str, dict[str, int]]:
    """Count of selected stories per subject-country × category."""
    out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in rows:
        if not r.get("selected"):
            continue
        cat = r.get("display_category") or "?"
        for region in (r.get("subject_regions") or ["?"]):
            out[region][cat] += 1
    return {k: dict(v) for k, v in out.items()}


def matrix_source_subject(rows: list[dict], source_rows: list[dict]) -> dict[str, dict[str, int]]:
    """Source-country × subject-country counts (selected clusters only).

    For each selected cluster, every (source origin_region) pairs with every
    (subject_region) once."""
    subj_by_cluster: dict[int, list[str]] = {}
    for r in rows:
        if r.get("selected") and r.get("cluster_id") is not None:
            subj_by_cluster[r["cluster_id"]] = r.get("subject_regions") or ["?"]
    src_by_cluster: dict[int, set[str]] = defaultdict(set)
    for s in source_rows:
        cid = s.get("cluster_id")
        if cid in subj_by_cluster:
            src_by_cluster[cid].add(s.get("origin_region") or "?")
    out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for cid, subjects in subj_by_cluster.items():
        for src in (src_by_cluster.get(cid) or {"?"}):
            for subj in subjects:
                out[src][subj] += 1
    return {k: dict(v) for k, v in out.items()}
