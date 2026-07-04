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


def trends(rows: list[dict]) -> list[dict]:
    """Per-date selection/feedback/composite trend, ascending by date."""
    by_date: dict[str, dict] = defaultdict(
        lambda: {"selected": 0, "candidates": 0, "accepts": 0, "verdicts": 0, "composite_sum": 0.0, "n": 0}
    )
    for r in rows:
        d = by_date[r.get("report_date") or "?"]
        d["n"] += 1
        d["composite_sum"] += float(r.get("composite", 0.0))
        if r.get("selected"):
            d["selected"] += 1
        else:
            d["candidates"] += 1
        if r.get("verdict") is not None:
            d["verdicts"] += 1
            if r.get("verdict") == 1:
                d["accepts"] += 1
    out = []
    for date in sorted(by_date):
        d = by_date[date]
        out.append({
            "date": date,
            "selected": d["selected"],
            "candidates": d["candidates"],
            "accept_rate": round(d["accepts"] / d["verdicts"], 2) if d["verdicts"] else None,
            "composite_avg": round(d["composite_sum"] / d["n"], 3) if d["n"] else 0.0,
        })
    return out


def source_review(rows: list[dict], source_rows: list[dict]) -> list[dict]:
    """Per source: impact-weighted contribution (Σ composite of selected clusters
    it appeared in) and cluster count. Selected clusters only."""
    composite_by_cluster: dict[int, float] = {}
    for r in rows:
        if r.get("selected") and r.get("cluster_id") is not None:
            composite_by_cluster[r["cluster_id"]] = float(r.get("composite", 0.0))
    agg: dict[str, dict] = defaultdict(lambda: {"contribution": 0.0, "clusters": 0})
    seen: set[tuple[str, int]] = set()
    for s in source_rows:
        name = s.get("source_name") or "?"
        cid = s.get("cluster_id")
        if cid not in composite_by_cluster or (name, cid) in seen:
            continue
        seen.add((name, cid))
        agg[name]["contribution"] += composite_by_cluster[cid]
        agg[name]["clusters"] += 1
    return sorted(
        ({"source": k, **v} for k, v in agg.items()),
        key=lambda x: -x["contribution"],
    )


def heat_class(value: float, scale: float) -> str:
    """Bucket a value in [0, scale] into one of 5 heat CSS classes c0..c4."""
    if scale <= 0:
        return "c0"
    frac = max(0.0, min(1.0, value / scale))
    return f"c{min(4, int(frac * 5))}" if frac < 1.0 else "c4"


def gate_badge(gate: dict) -> dict:
    """Ownership-gate verdict for one cluster evaluation, for the 单日审查 内政 column.

    Returns {label, cls, title}. Gate is active only when target_region is set:
    blocked = state-controlled source on foreign 内政 (suppress), review =
    constrained/low-evidence source (downrank), else allowed (independent/public).
    """
    if not gate or not gate.get("target"):
        return {"label": "—", "cls": "gate-none", "title": "非内政 / 门控未触发"}
    target = gate["target"]
    blocked = gate.get("blocked") or []
    review = gate.get("review") or []
    if blocked:
        return {"label": "禁", "cls": "gate-block",
                "title": f"{target} 内政 · 禁(state-controlled): {', '.join(blocked)}"}
    if review:
        return {"label": "审", "cls": "gate-review",
                "title": f"{target} 内政 · 审(constrained/low-evidence): {', '.join(review)}"}
    return {"label": "放", "cls": "gate-allow",
            "title": f"{target} 内政 · 独立/公共媒体放行"}


def sparkline_svg(values: list[float], width: int = 160, height: int = 32) -> str:
    """Tiny inline SVG line for a trend series (no JS, no deps)."""
    pts = [v for v in values if v is not None]
    if len(pts) < 2:
        return ""
    lo, hi = min(pts), max(pts)
    span = (hi - lo) or 1.0
    step = width / (len(pts) - 1)
    coords = " ".join(
        f"{i * step:.1f},{height - (v - lo) / span * (height - 4) - 2:.1f}"
        for i, v in enumerate(pts)
    )
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        f'<polyline fill="none" stroke="#057dbc" stroke-width="1.5" points="{coords}"/></svg>'
    )
