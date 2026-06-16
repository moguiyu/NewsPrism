"""Editor-feedback channel for NewsPrism.

CLI feedback recording and listing. The Telegram inline-keyboard poller was
removed in favor of the portal backend (newsprism/runtime/portal); the
editorial_feedback table is still populated by portal and CLI.

Layer: runtime (may import repo, config, types)
"""
from __future__ import annotations

import logging

from newsprism.repo import insert_editorial_feedback, list_editorial_feedback

logger = logging.getLogger(__name__)


def record_feedback_cli(cluster_id: int, verdict: str, note: str = "") -> int:
    """Record a CLI feedback signal.

    Args:
        cluster_id: DB id of the cluster
        verdict: "accept"/"+1"/1 → +1; "reject"/"-1"/-1 → -1
        note: optional free-text note

    Returns:
        The inserted row id.
    """
    if verdict in ("accept", "+1", "1", 1):
        v = 1
    else:
        v = -1
    return insert_editorial_feedback(cluster_id, v, channel="cli", note=note)


def format_feedback_list(limit: int = 30) -> str:
    """Return a human-readable summary of recent editorial feedback."""
    rows = list_editorial_feedback(limit)
    lines = [f"Editorial feedback — {len(rows)} row(s):"]
    for row in rows:
        sign = "+" if int(row["verdict"]) >= 0 else "-"
        summary = str(row.get("cluster_summary") or "")[:50]
        lines.append(
            f"{row['created_at']} [{sign}] cluster={row['cluster_id']}"
            f" ({row.get('report_date', '')}): {summary}"
        )
    return "\n".join(lines)
