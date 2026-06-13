"""Editor-feedback channel for NewsPrism.

Handles Telegram inline-keyboard feedback polling and CLI feedback recording.

Layer: runtime (may import repo, config, types)
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import httpx

from newsprism.repo import insert_editorial_feedback, list_editorial_feedback

logger = logging.getLogger(__name__)

_FEEDBACK_PATTERN = re.compile(r"^fb:(\d+):([\+\-]?1)$")


class FeedbackPoller:
    """Polls Telegram getUpdates for inline-keyboard feedback callbacks."""

    def __init__(self, cfg) -> None:
        self._token: str = cfg.telegram_bot_token or ""
        self._chat_id: str = cfg.telegram_chat_id or ""
        self._evolution: dict = cfg.evolution if isinstance(cfg.evolution, dict) else {}
        self._offset_path = Path("data/.telegram_feedback_offset")

    def _read_offset(self) -> int:
        try:
            return int(self._offset_path.read_text().strip())
        except (FileNotFoundError, ValueError):
            return 0

    def _write_offset(self, offset: int) -> None:
        self._offset_path.parent.mkdir(parents=True, exist_ok=True)
        self._offset_path.write_text(str(offset))

    def poll_once(self) -> int:
        if not self._token or not self._chat_id:
            logger.warning("Telegram not configured — skipping feedback poll")
            return 0
        if not self._evolution.get("feedback_enabled", True):
            logger.info("Feedback polling disabled via evolution config")
            return 0

        offset = self._read_offset()
        count = 0
        try:
            with httpx.Client(timeout=20) as client:
                resp = client.get(
                    f"https://api.telegram.org/bot{self._token}/getUpdates",
                    params={
                        "offset": offset,
                        "timeout": 0,
                        "allowed_updates": '["callback_query"]',
                    },
                )
                data = resp.json()
                updates = data.get("result", [])
                max_update_id = offset - 1

                for update in updates:
                    uid = int(update.get("update_id", 0))
                    if uid > max_update_id:
                        max_update_id = uid

                    cq = update.get("callback_query")
                    if not cq:
                        continue
                    cq_data = cq.get("data", "")
                    m = _FEEDBACK_PATTERN.match(cq_data)
                    if not m:
                        continue
                    cluster_id = int(m.group(1))
                    verdict = 1 if m.group(2) in ("+1", "1") else -1
                    try:
                        insert_editorial_feedback(
                            cluster_id, verdict, channel="telegram", note=""
                        )
                        count += 1
                    except Exception as exc:
                        logger.error("insert_editorial_feedback failed: %s", exc)

                    emoji = "👍" if verdict == 1 else "👎"
                    try:
                        client.post(
                            f"https://api.telegram.org/bot{self._token}/answerCallbackQuery",
                            json={
                                "callback_query_id": cq["id"],
                                "text": f"已记录 {emoji}",
                            },
                        )
                    except Exception as exc:
                        logger.warning("answerCallbackQuery failed: %s", exc)

                if updates:
                    self._write_offset(max_update_id + 1)

        except Exception as exc:
            logger.error("Telegram feedback poll error: %s", exc)

        return count


def feedback_keyboard(stories: list[dict]) -> dict:
    """Build a Telegram inline keyboard markup for story ratings.

    Args:
        stories: list of {"index": int, "cluster_id": int}

    Returns:
        {"inline_keyboard": [[button, button, ...], ...]}
        At most 2 stories (4 buttons) per row.
    """
    valid = [s for s in stories if s.get("cluster_id")]
    rows: list[list[dict]] = []
    for i in range(0, len(valid), 2):
        batch = valid[i : i + 2]
        row: list[dict] = []
        for s in batch:
            idx = s["index"]
            cid = s["cluster_id"]
            row.append({"text": f"{idx}👍", "callback_data": f"fb:{cid}:+1"})
            row.append({"text": f"{idx}👎", "callback_data": f"fb:{cid}:-1"})
        if row:
            rows.append(row)
    return {"inline_keyboard": rows}


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
