"""Post-publication review: judge what the page showed *and* what it dropped.

The calibration loop needs accepted and rejected examples to move a weight, but
`editorial_feedback` keys on `cluster_id` and joins `clusters`, which holds only
what survived selection -- 809 of 2790 evaluations in the 30 days to 2026-10-29.
A verdict about a dropped story therefore had nowhere to go, so the loop had no
input and `signal: 0.24` sat untouched since 2026-06-14.

This module reads the day's audit trail (every evaluated candidate, shown or
not) and records a verdict per candidate in `review_verdicts`, keyed by
`(report_date, cluster_key)` so it works for candidates that never became rows.

Layer: runtime (may import repo, config, types)
"""
from __future__ import annotations

import logging
from pathlib import Path

from newsprism.repo import (
    DB_PATH,
    get_cluster_audit_for_date,
    insert_review_verdict,
    list_review_verdicts,
)

logger = logging.getLogger(__name__)

_VERDICT_ALIASES = {
    "publish": 1,
    "+1": 1,
    "1": 1,
    "accept": 1,
    "drop": -1,
    "-1": -1,
    "reject": -1,
}


def record_verdict(
    report_date: str,
    cluster_key: str,
    verdict: str | int,
    reason: str = "",
    channel: str = "cli",
    db_path: Path = DB_PATH,
) -> int:
    """Record one editor judgement about an evaluated candidate.

    Returns the row id, or 0 when the key is not part of that day's audit trail:
    a typo must not invent a training example that never existed.
    """
    normalised = _VERDICT_ALIASES.get(str(verdict).lower()) if not isinstance(verdict, int) else (1 if verdict >= 0 else -1)
    if normalised is None:
        raise ValueError(f"unknown verdict: {verdict!r} (use publish/drop)")

    audit_rows = {row["cluster_key"]: row for row in get_cluster_audit_for_date(report_date, db_path=db_path)}
    audit = audit_rows.get(cluster_key)
    if audit is None:
        logger.warning(
            "Review verdict ignored: %s has no audit row for %s", cluster_key, report_date
        )
        return 0

    stage = "rendered" if audit.get("outcome") == "rendered" else "dropped"
    return insert_review_verdict(
        report_date=report_date,
        cluster_key=cluster_key,
        verdict=normalised,
        stage=stage,
        reason=reason,
        channel=channel,
        db_path=db_path,
    )


def format_review_list(report_date: str, db_path: Path = DB_PATH) -> str:
    """List the day's candidates for review, dropped-and-high-scoring first.

    Ordering is deliberate: the stories the editor never saw are the ones whose
    judgement carries information. A card that was published and accepted tells
    the calibration almost nothing on its own.
    """
    audit_rows = get_cluster_audit_for_date(report_date, db_path=db_path)
    if not audit_rows:
        return f"No audit rows for {report_date}."

    decided = {
        row["cluster_key"]: row
        for row in list_review_verdicts(days=3650, db_path=db_path)
        if row.get("report_date") == report_date
    }

    def _sort_key(row: dict) -> tuple:
        rendered = row.get("outcome") == "rendered"
        composite = float(row.get("composite") or 0.0)
        return (rendered, -composite)

    lines = [f"Review {report_date} — {len(audit_rows)} candidate(s)"]
    for row in sorted(audit_rows, key=_sort_key):
        key = row["cluster_key"]
        verdict = decided.get(key)
        if verdict is None:
            state = "待判定"
        else:
            state = "已判定"
            state += " ✓publish" if int(verdict["verdict"]) > 0 else " ✗drop"
        shown = "显示" if row.get("outcome") == "rendered" else "未显示"
        composite = float(row.get("composite") or 0.0)
        category = row.get("display_category") or "未分类"
        lines.append(
            f"  {key} [{state}] {shown} composite={composite:.3f} {category}"
            f" outcome={row.get('outcome')} reason={row.get('reason') or '-'}"
        )
    pending = sum(1 for row in audit_rows if row["cluster_key"] not in decided)
    lines.append(f"待判定 {pending} / {len(audit_rows)}")
    return "\n".join(lines)
