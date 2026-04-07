"""Telegram publisher — sends daily digest as formatted messages.

Splits long content across multiple messages (Telegram 4096-char limit).
Each "message" in the digest represents one topic cluster.

Uses ParseMode.HTML throughout. LLM markdown output is converted to
Telegram-safe HTML via _body_to_tg_html() before sending.

Format: section label when category changes, then headline + body only.
No perspectives / source chips — keep the push simple and readable.

Layer: runtime (can import types, config, repo, service)
"""
from __future__ import annotations

import html
import logging
import re
from datetime import date

from telegram import Bot
from telegram.constants import ParseMode

from newsprism.config import Config
from newsprism.runtime.renderer import _BROAD_CATEGORY_MAP, _CATEGORY_META, _DEFAULT_BROAD, _body_only, _extract_headline
from newsprism.types import ClusterSummary

logger = logging.getLogger(__name__)

MAX_MSG_LEN = 4000  # leave headroom below Telegram's 4096-char limit

# Build emoji lookup from renderer metadata
_CAT_EMOJI: dict[str, str] = {cat: emoji for cat, emoji, _ in _CATEGORY_META}


def _broad(topic_category: str) -> str:
    return _BROAD_CATEGORY_MAP.get(topic_category, _DEFAULT_BROAD)


def _body_to_tg_html(text: str) -> str:
    """Convert body-only text (no headline, no bullets) to Telegram HTML.

    Handles **bold** markdown; HTML-escapes content first.
    """
    lines: list[str] = []
    for line in text.splitlines():
        escaped = html.escape(line)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
        lines.append(escaped)
    return "\n".join(lines)


class TelegramPublisher:
    def __init__(self, cfg: Config) -> None:
        self.token = cfg.telegram_bot_token
        self.chat_id = cfg.telegram_chat_id
        self.report_base_url = cfg.report_base_url

    async def publish(self, summaries: list[ClusterSummary], report_date: date) -> None:
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured — skipping publish")
            return

        bot = Bot(token=self.token)
        date_str = report_date.strftime("%Y年%m月%d日")
        day_name = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][report_date.weekday()]

        header = (
            f"📰 <b>NewsPrism 每日科技速览</b>\n"
            f"{date_str} {day_name} | 共 {len(summaries)} 个话题\n"
            f"{'─' * 20}"
        )

        report_url = f"{self.report_base_url}/{report_date.isoformat()}/index.html"
        footer = (
            f"{'─' * 20}\n"
            f'📊 <a href="{report_url}">查看完整报告</a>\n'
            f"<i>由 NewsPrism 自动生成</i>"
        )

        messages: list[str] = [header]

        current_broad = ""
        for i, cs in enumerate(summaries, 1):
            broad = _broad(cs.cluster.topic_category)
            if broad != current_broad:
                emoji = _CAT_EMOJI.get(broad, "")
                messages.append(f"\n<b>{emoji} {broad}</b>")
                current_broad = broad

            headline = _extract_headline(cs.summary) or html.escape(cs.cluster.topic_category)
            body = _body_to_tg_html(_body_only(cs.summary))
            block = f"\n<b>{i}. {html.escape(headline)}</b>\n{body}\n"
            messages.append(block)

        messages.append(footer)

        batches = self._batch_messages(messages)

        async with bot:
            for batch in batches:
                try:
                    await bot.send_message(
                        chat_id=self.chat_id,
                        text=batch,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                except Exception as exc:
                    logger.error("Telegram send failed: %s", exc)

        logger.info("Published %d clusters to Telegram", len(summaries))

    def _batch_messages(self, messages: list[str]) -> list[str]:
        """Merge message blocks into Telegram-sized batches."""
        batches: list[str] = []
        current = ""

        for msg in messages:
            if len(current) + len(msg) > MAX_MSG_LEN:
                if current:
                    batches.append(current.strip())
                current = msg
            else:
                current += msg

        if current:
            batches.append(current.strip())

        return batches
