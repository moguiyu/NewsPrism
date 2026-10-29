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
    query_evaluations,
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
    # Only candidates the pipeline actually weighed are reviewable. Rows excluded
    # by the candidate window never reached scoring, and reading them into the
    # list buried the ~90 real candidates under 1261 unusable entries.
    audit_rows = [row for row in audit_rows if row.get("outcome") != "window_excluded"]
    if not audit_rows:
        return f"No audit rows for {report_date}."

    # The audit row is written before scoring and `composite` only exists from
    # 2026-10-30 on, so older days fall back to the stored evaluation.
    evaluated = {
        row["cluster_key"]: row
        for row in query_evaluations(report_date, report_date, db_path=db_path)
    }

    decided = {
        row["cluster_key"]: row
        for row in list_review_verdicts(days=3650, db_path=db_path)
        if row.get("report_date") == report_date
    }

    def _score(row: dict) -> float:
        value = row.get("composite")
        if value is None:
            value = (evaluated.get(row["cluster_key"]) or {}).get("composite")
        return float(value or 0.0)

    def _category(row: dict) -> str:
        value = row.get("display_category")
        if not value:
            value = (evaluated.get(row["cluster_key"]) or {}).get("display_category")
        return str(value or "未分类")

    def _sort_key(row: dict) -> tuple:
        rendered = row.get("outcome") == "rendered"
        return (rendered, -_score(row))

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
        lines.append(
            f"  {key} [{state}] {shown} composite={_score(row):.3f} {_category(row)}"
            f" outcome={row.get('outcome')} reason={row.get('reason') or '-'}"
        )
    pending = sum(1 for row in audit_rows if row["cluster_key"] not in decided)
    lines.append(f"待判定 {pending} / {len(audit_rows)}")
    return "\n".join(lines)
