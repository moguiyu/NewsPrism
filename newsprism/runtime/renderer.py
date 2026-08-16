"""Static HTML renderer — generates daily report page from Jinja2 template.

Layer: runtime (can import types, config, repo, service)
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import os
import re
import shutil
import struct
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from newsprism.service.categories import (
    DEFAULT_DISPLAY_CATEGORY,
    DISPLAY_CATEGORIES,
    LEGACY_DISPLAY_CATEGORY_MAP,
    display_category_label_zh,
    normalize_display_category,
)
from newsprism.service.language import looks_like_chinese_text
from newsprism.service.locales import region_flag, region_flags
from newsprism.types import ClusterSummary, SourceCertification, is_real_article

logger = logging.getLogger(__name__)

# Matches a published daily report directory (YYYY-MM-DD) at the output root.
_REPORT_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# English day/month abbreviations for RFC-822 <pubDate> in the RSS feed
# (locale-independent, since strftime %a/%b are locale-sensitive).
_RFC822_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_RFC822_MONTHS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


def _rfc822_pub_date(date_str: str, tz_name: str) -> str:
    """RFC-822 pubDate for a report date at 08:00 in the schedule timezone.

    Offset is formatted manually (locale-safe); DST is resolved by ZoneInfo.
    """
    d = date.fromisoformat(date_str)
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    offset = datetime(d.year, d.month, d.day, 8, 0, tzinfo=tz).utcoffset() or timedelta(0)
    total = abs(int(offset.total_seconds()))
    sign = "+" if offset.total_seconds() >= 0 else "-"
    hh, mm = divmod(total // 60, 60)
    return (
        f"{_RFC822_DAYS[d.weekday()]}, {d.day:02d} {_RFC822_MONTHS[d.month - 1]} "
        f"{d.year} 08:00:00 {sign}{hh:02d}{mm:02d}"
    )

# ── BROAD CATEGORY MAPPING ────────────────────────────────────────────────────

_BROAD_CATEGORY_MAP: dict[str, str] = {
    "Finance": "Business",
    "AI & LLM": "Technology",
    "Smartphones & Electronics": "Technology",
    "Smart Home": "Technology",
    "Robotics": "Technology",
    "Chips & Hardware": "Technology",
    "Tech Companies - China": "Technology",
    "Tech Companies - International": "Technology",
    "Space": "Technology",
    "Tech General": "Technology",
    "Tech-General": "Technology",
    "Chips": "Technology",
    "Geopolitics": "World",
    "Geopolitics - Extended": "World",
    "Regions": "World",
    "AI Policy & Regulation": "World",
    "World News": "World",
    "Society": "Society",
    "Film - Chinese": "Culture & Sports",
    "Film - International": "Culture & Sports",
    "Film - General": "Culture & Sports",
    "Music": "Culture & Sports",
    "Games - Chinese": "Culture & Sports",
    "Games - Platform": "Culture & Sports",
    "Games - General": "Culture & Sports",
    "Culture": "Culture & Sports",
    "Positive Energy": "Culture & Sports",
    "Sports": "Culture & Sports",
    "Energy": "Science & Health",
    "Energy & Climate": "Science & Health",
    "Science & Health": "Science & Health",
}

_DEFAULT_BROAD = DEFAULT_DISPLAY_CATEGORY

_BROAD_CATEGORY_EN_MAP: dict[str, str] = {
    "World": "World",
    "Business": "Business",
    "Technology": "Technology",
    "Science & Health": "Science & Health",
    "Society": "Society",
    "Culture & Sports": "Culture & Sports",
}

_CATEGORY_META: list[tuple[str, str, str]] = [
    # (broad_category, emoji, css_key)
    ("World", "🌍", "world"),
    ("Business", "💰", "finance"),
    ("Technology", "🔬", "tech"),
    ("Science & Health", "🔭", "science"),
    ("Society", "🏛️", "society"),
    ("Culture & Sports", "🎭", "culture"),
]

# ── REGION → FLAG EMOJI ───────────────────────────────────────────────────────

_REGION_FLAG: dict[str, str] = region_flags()

_HOT_TOPIC_ICON_MAP: dict[str, str] = {
    "globe": "🌍",
    "war": "⚠️",
    "trade": "📦",
    "chip": "🧠",
    "ai": "🤖",
    "energy": "⚡",
}

# Full-length hot-topic family label for in-body headers (overview + topic
# stage). Navigation tabs stay capped at tab_name_max_chars (10); body copy
# shows the complete storyline title. Matches editorial_planner's default
# tab_name_full_max_chars; the cap only guards against pathological titles.
_HOT_TOPIC_FULL_NAME_MAX_CHARS = 60

_INVALID_PERSPECTIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"无关"),
    re.compile(r"不相关"),
    re.compile(r"未提供.{0,8}(视角|信息|内容)"),
    re.compile(r"未稳定提炼出可单列的差异化视角"),
    re.compile(r"not related", re.IGNORECASE),
    re.compile(r"unrelated", re.IGNORECASE),
    re.compile(r"irrelevant", re.IGNORECASE),
    re.compile(r"no distinct perspective could be extracted", re.IGNORECASE),
)

# Seeker placeholder failure reasons → bilingual short label. Surfaced as the
# provenance label of an inline ⚠️ placeholder article so the reader sees WHY
# a regional perspective is missing (tooltip), without polluting the page.
_PLACEHOLDER_FAILURE_LABELS: dict[str, tuple[str, str]] = {
    "http_401": ("鉴权失败", "Auth failed"),
    "http_403": ("鉴权失败", "Auth failed"),
    "http_402": ("额度不足", "Quota exceeded"),
    "http_429": ("限流", "Rate limited"),
    "network": ("网络错误", "Network error"),
    "official_not_found": ("未找到官方回应", "No official response found"),
    "country_fallback_not_found": ("未找到当地报道", "No local reporting found"),
    "candidate_unverified": ("来源未能核验", "Source could not be verified"),
    "candidate_pending_review": ("来源待人工核验", "Source pending review"),
    "not_official_source": ("非官方来源", "Not an official source"),
    "not_related_country_source": ("非相关当地来源", "Not related local reporting"),
    "empty_results": ("无新结果", "No fresh results"),
    "no_acceptable_result": ("无可用结果", "No acceptable result"),
    "stale_result": ("结果过旧", "Result too old"),
    "region_mismatch": ("地区不匹配", "Region mismatch"),
    "event_mismatch": ("事件不匹配", "Event mismatch"),
    "duplicate_of_existing": ("已有相同报道", "Duplicate of existing"),
    "thin_result": ("结果内容过少", "Result too thin"),
    "query_generation_failed": ("查询生成失败", "Query generation failed"),
    "entity_mismatch": ("实体不匹配", "Entity mismatch"),
    "invalid_result_url": ("结果链接无效", "Invalid result URL"),
    "publisher_target_mismatch": ("发布者与目标不匹配", "Publisher does not match target"),
    "publisher_binding_unverified": ("未确认官方归属", "Official ownership unverified"),
    "official_binding_not_found": ("未找到可信官方渠道", "No verified official channel found"),
    "official_skipped_low_budget": ("官方渠道搜索已让位于本地搜索", "Official search deferred to preserve local-search budget"),
    "request_budget_exhausted": ("本轮搜索额度已用完", "Search budget exhausted"),
    "coverage_satisfied": ("已有合格来源", "Qualifying coverage already exists"),
    "identity_binding_resolved": ("已确认官方渠道", "Official channel verified"),
    "country_target_official_forbidden": ("国家目标不可使用官方搜索", "Country target cannot use official search"),
    "unknown": ("未知原因", "Unknown reason"),
}


def _placeholder_failure_label(reason: str | None) -> tuple[str, str]:
    """Return (zh, en) short label for a seeker placeholder failure reason."""
    if not reason:
        return _PLACEHOLDER_FAILURE_LABELS["unknown"]
    # Reasons may be comma-separated (acceptance gate logs combined rejections).
    primary = reason.split(",", 1)[0].strip()
    if primary in _PLACEHOLDER_FAILURE_LABELS:
        return _PLACEHOLDER_FAILURE_LABELS[primary]
    if primary.startswith("http_"):
        status = primary.removeprefix("http_") or "error"
        return (f"搜索服务错误（HTTP {status}）", f"Search service error (HTTP {status})")
    return _PLACEHOLDER_FAILURE_LABELS["unknown"]


def _placeholder_stage_detail(
    trace: list[dict[str, str]] | None,
) -> tuple[str, str] | None:
    """Render the attempted fallback chain instead of hiding it behind one reason."""
    if not trace:
        return None
    stage_labels = {
        "official": ("官方", "Official"),
        "country": ("当地", "Local"),
    }
    zh_parts: list[str] = []
    en_parts: list[str] = []
    for item in trace:
        stage = str(item.get("stage") or "").strip().lower()
        reason = str(item.get("reason") or "unknown").strip().lower()
        stage_zh, stage_en = stage_labels.get(stage, (stage or "搜索", stage.title() or "Search"))
        reason_zh, reason_en = _placeholder_failure_label(reason)
        zh_parts.append(f"{stage_zh}：{reason_zh}")
        en_parts.append(f"{stage_en}: {reason_en}")
    return "；".join(zh_parts), "; ".join(en_parts)


# ── TEXT HELPERS ──────────────────────────────────────────────────────────────


def _extract_headline(text: str) -> str:
    """Return the text of the first **bold headline** line, unformatted.

    The LLM always opens with a one-sentence bold headline per the style guide.
    Falls back to empty string if none found.
    """
    for line in text.splitlines():
        m = re.match(r"\*\*(.+?)\*\*", line.strip())
        if m:
            return m.group(1)
    return ""


def _body_only(text: str) -> str:
    """Strip the headline line and per-source perspective bullets.

    Leaves only the 2-4 sentence factual body, which is displayed in the
    summary area. The perspectives are shown separately in the expand section.
    """
    lines = text.splitlines()
    result: list[str] = []
    headline_consumed = False
    for line in lines:
        stripped = line.strip()
        # Drop the first **headline** line
        if not headline_consumed and re.match(r"\*\*(.+?)\*\*", stripped):
            headline_consumed = True
            continue
        # Drop perspective bullet lines: • 【Source】text
        if re.match(r"[•·\-\*]\s*【.+?】", stripped):
            continue
        result.append(line)
    # Trim leading/trailing blank lines
    while result and not result[0].strip():
        result.pop(0)
    while result and not result[-1].strip():
        result.pop()
    return "\n".join(result)


def _md_to_html(text: str) -> Markup:
    """Convert LLM markdown output to safe HTML for the Jinja2 template.

    Handles the subset produced by our style guide:
      **bold text**   →  <strong>bold text</strong>
      • 【Source】text  →  • <strong>【Source】</strong>text
    Content is HTML-escaped before tag substitution to prevent injection.
    Returns a Jinja2 Markup object so autoescape doesn't double-escape it.
    """
    lines: list[str] = []
    for line in text.splitlines():
        escaped = html_lib.escape(line)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"(•\s*)(【.+?】)", r"\1<strong>\2</strong>", escaped)
        lines.append(escaped)
    return Markup("<br>\n".join(lines))


_BROAD_CATEGORIES = set(DISPLAY_CATEGORIES)


def _broad_category(topic_category: str, display_category: str | None = None) -> str:
    display = (display_category or "").strip()
    if display in _BROAD_CATEGORIES:
        return display
    if display in LEGACY_DISPLAY_CATEGORY_MAP:
        return normalize_display_category(display)
    if topic_category in _BROAD_CATEGORIES:
        return topic_category
    if topic_category in LEGACY_DISPLAY_CATEGORY_MAP:
        return normalize_display_category(topic_category)
    if topic_category in _BROAD_CATEGORY_MAP:
        return _BROAD_CATEGORY_MAP[topic_category]
    for key, broad in _BROAD_CATEGORY_MAP.items():
        if key.lower() in topic_category.lower():
            return broad
    return _DEFAULT_BROAD


def _fallback_short_topic_name(summary: ClusterSummary, max_chars: int = 10) -> str:
    headline = _extract_headline(summary.summary) or summary.cluster.topic_category or "全球焦点"
    compact = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", headline).strip()
    compact = re.sub(r"\s+", "", compact)
    compact = compact[:max_chars].strip(" -:：，,、。.；;")
    return compact or "全球焦点"


def _normalize_hot_topic_name(name: str | None, summary: ClusterSummary | None = None, max_chars: int = 10) -> str:
    compact = (name or "").strip()
    compact = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", compact).strip()
    compact = re.sub(r"\s+", "", compact)
    compact = compact[:max_chars].strip(" -:：，,、。.；;")
    if compact:
        return compact
    if summary is not None:
        return _fallback_short_topic_name(summary, max_chars)
    return "全球焦点"


_JAPANESE_KANA_RE = re.compile(r"[\u3040-\u30ff]")
_CYRILLIC_RE = re.compile(r"[\u0400-\u04ff]")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_REFINERY_RE = re.compile(r"(製油|炼油|炼厂|油库|refiner|oil depot)", re.IGNORECASE)
_RUSSIA_RE = re.compile(r"(俄罗斯|俄军|俄方|俄国防部|莫斯科|克里米亚|Russia|Russian|Moscow|Crimea)", re.IGNORECASE)
_UKRAINE_RE = re.compile(r"(乌克兰|乌军|乌方|泽连斯基|Ukraine|Ukrainian|Zelensky)", re.IGNORECASE)
_MILITARY_ESCALATION_RE = re.compile(
    r"(军事|军|袭击|攻击|互袭|无人机|导弹|防空|边境|打击|"
    r"strike|attack|drone|missile|military|border|Belarus|Zaporizhzhia)",
    re.IGNORECASE,
)
_NON_RU_UA_CONFLICT_RE = re.compile(
    r"(美以伊|伊朗|以色列|加沙|哈马斯|Iran|Israeli?|Gaza|Hamas)",
    re.IGNORECASE,
)


def _is_public_chinese_hot_topic_name(name: str) -> bool:
    compact = (name or "").strip()
    if not compact:
        return False
    if _JAPANESE_KANA_RE.search(compact) or _CYRILLIC_RE.search(compact):
        return False
    return bool(_CJK_RE.search(compact))


def _hot_topic_family_text(summaries: list[ClusterSummary]) -> str:
    chunks: list[str] = []
    for summary in summaries:
        chunks.extend(
            str(value)
            for value in (
                summary.short_topic_name,
                summary.macro_topic_name,
                summary.storyline_name,
                _extract_headline(summary.summary),
                summary.summary,
                summary.cluster.topic_category,
            )
            if value
        )
        chunks.extend(article.title for article in summary.cluster.articles[:3] if article.title)
    return "\n".join(chunks)


def _is_russia_ukraine_military_escalation(text: str) -> bool:
    return bool(
        _RUSSIA_RE.search(text)
        and _UKRAINE_RE.search(text)
        and _MILITARY_ESCALATION_RE.search(text)
    )


def _specific_hot_topic_label(
    summaries: list[ClusterSummary],
    max_chars: int,
) -> tuple[str, str | None]:
    """Return the narrowest reader-facing label available for a family."""
    for summary in summaries:
        candidate = _normalize_hot_topic_name(
            summary.short_topic_name or _fallback_short_topic_name(summary, max_chars),
            summary,
            max_chars,
        )
        if _is_public_chinese_hot_topic_name(candidate):
            candidate_en = str(getattr(summary, "short_topic_name_en", "") or "").strip() or None
            return candidate, candidate_en
    return _normalize_hot_topic_name(None, summaries[0] if summaries else None, max_chars), None


def _disambiguate_hot_topic_label(
    label: str,
    label_en: str | None,
    storyline_name: str | None,
    summaries: list[ClusterSummary],
    used_labels: set[str],
    max_chars: int,
) -> tuple[str, str | None]:
    """Keep separately keyed hot-topic tabs visibly distinct.

    Duplicate labels are a presentation defect: readers cannot tell which tab
    they are opening. Prefer the planner's original storyline label, then an
    event-specific short label, before falling back to the lead headline.
    """
    candidates = [
        (
            _normalize_hot_topic_name(storyline_name, summaries[0] if summaries else None, max_chars),
            None,
        ),
        _specific_hot_topic_label(summaries, max_chars),
    ]
    for candidate, candidate_en in candidates:
        if candidate and candidate.casefold() not in used_labels:
            return candidate, candidate_en

    lead = _fallback_short_topic_name(summaries[0], max_chars) if summaries else label
    expanded = f"{label}：{lead}".strip("：")
    return expanded, label_en


def _repair_hot_topic_label(
    name: str | None,
    name_en: str | None,
    summaries: list[ClusterSummary],
    max_chars: int = 10,
) -> tuple[str, str | None]:
    topic_name = _normalize_hot_topic_name(name, summaries[0] if summaries else None, max_chars)
    topic_name_en = (name_en or "").strip() or None
    family_text = _hot_topic_family_text(summaries)
    is_ru_ua_escalation = _is_russia_ukraine_military_escalation(family_text)
    is_stale_refinery_label = bool(_REFINERY_RE.search(f"{topic_name}\n{topic_name_en or ''}"))

    # A generic Russia-Ukraine family must not retain a label for a different
    # conflict.  Do not, however, replace a valid narrow label such as
    # "特朗普拒供导弹": two distinct Russia-Ukraine storylines would otherwise
    # collapse into duplicate tabs named "俄乌军事升级".
    if is_ru_ua_escalation and (
        is_stale_refinery_label or _NON_RU_UA_CONFLICT_RE.search(topic_name)
    ):
        return _specific_hot_topic_label(summaries, max_chars)

    if _is_public_chinese_hot_topic_name(topic_name):
        return topic_name, topic_name_en

    candidate, candidate_en = _specific_hot_topic_label(summaries, max_chars)
    if _is_public_chinese_hot_topic_name(candidate):
        return candidate, candidate_en or topic_name_en

    return topic_name, topic_name_en


def _normalize_text_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _is_renderable_perspective(text: str) -> bool:
    normalized = _normalize_text_whitespace(text)
    if not normalized:
        return False
    return not any(pattern.search(normalized) for pattern in _INVALID_PERSPECTIVE_PATTERNS)


def _placeholder_source_label_en(source_name: str) -> str:
    """Translate the compact synthetic source marker without an LLM call."""
    label = str(source_name or "").strip()
    bracketed = label.startswith("[") and label.endswith("]")
    inner = label[1:-1] if bracketed else label
    inner = inner.replace("声音待补", " voice pending")
    inner = inner.replace("视角待补", " perspective pending")
    inner = inner.replace("待补", " pending")
    inner = re.sub(r"\s+", " ", inner).strip()
    return f"[{inner}]" if bracketed else inner


def _is_real_article(article: object) -> bool:
    return is_real_article(article)  # type: ignore[arg-type]


def _is_background_search_meta(meta: dict) -> bool:
    role = str(meta.get("search_evidence_role") or meta.get("evidence_role") or "").strip().lower()
    reason = str(meta.get("search_acceptance_reason") or "").strip().lower()
    freshness = str(meta.get("result_freshness_state") or "").strip().lower()
    return (
        role in {"background_context", "official_confirmation", "background"}
        or reason in {"background_context", "official_confirmation", "stale_result", "event_mismatch"}
        or freshness in {"stale", "old", "background"}
    )


def _counts_as_current_perspective(meta: dict) -> bool:
    if bool(meta.get("is_placeholder")) or _is_background_search_meta(meta):
        return False
    if not bool(meta.get("is_searched")):
        return True
    status = meta.get("search_acceptance_status")
    if status is not None and status != "accepted":
        return False
    # A persisted accepted row without freshness or an explicit evidence role
    # is review-only. Legacy in-memory test/articles with no acceptance verdict
    # remain renderable for compatibility.
    if status == "accepted" and not (
        meta.get("result_freshness_state")
        or meta.get("search_evidence_role")
        or meta.get("search_acceptance_reason") == "current_event_perspective"
    ):
        return False
    freshness = str(meta.get("result_freshness_state") or "").strip().lower()
    return freshness not in {"stale", "old", "background"}


def _counts_for_summary(
    summary: ClusterSummary,
    source_regions: dict[str, str] | None = None,
) -> dict[str, int]:
    source_regions = source_regions or {}
    real_articles = [article for article in summary.cluster.articles if _is_real_article(article)]
    placeholders = [article for article in summary.cluster.articles if not _is_real_article(article)]
    real_sources = {article.source_name for article in real_articles}
    organic_articles = [article for article in real_articles if not article.is_searched]
    organic_sources = {article.source_name for article in organic_articles}
    organic_regions = {
        article.origin_region or source_regions.get(article.source_name)
        for article in organic_articles
    } - {None, ""}
    return {
        "article_count": len(real_articles),
        "real_article_count": len(real_articles),
        "placeholder_count": len(placeholders),
        "total_article_count": len(real_articles) + len(placeholders),
        "real_source_count": len(real_sources),
        "placeholder_source_count": len({article.source_name for article in placeholders}),
        "organic_unique_sources": len(organic_sources),
        "organic_unique_regions": len(organic_regions),
    }


def _truncate_preview(text: str, max_chars: int = 54) -> str:
    compact = _normalize_text_whitespace(text)
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 1].rstrip("，,、；;：: ") + "…"


_MANIFEST_JSON = json.dumps(
    {
        "name": "NewsPrism - 多源新闻聚合",
        "short_name": "NewsPrism",
        "description": "全球多源新闻聚合与多视角分析",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#0a0b10",
        "theme_color": "#0a0b10",
        "orientation": "any",
        "icons": [
            {"src": "/icons/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/icons/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
        ],
    },
    ensure_ascii=False,
    indent=2,
)

_SW_JS = """\
const CACHE = 'newsprism-v1';
const PRECACHE = ['/manifest.json', '/icons/icon-192.png', '/icons/icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(PRECACHE)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE).map(k => caches.delete(k))
    )).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  // HTML: network-first (always fresh reports)
  if (req.headers.get('accept')?.includes('text/html')) {
    e.respondWith(
      fetch(req).then(res => {
        const clone = res.clone();
        caches.open(CACHE).then(c => c.put(req, clone));
        return res;
      }).catch(() => caches.match(req))
    );
    return;
  }
  // Other assets: cache-first
  e.respondWith(
    caches.match(req).then(cached => cached || fetch(req).then(res => {
      const clone = res.clone();
      caches.open(CACHE).then(c => c.put(req, clone));
      return res;
    }))
  );
});
"""


def _favicon_ico_bytes() -> bytes:
    width = 16
    height = 16
    pixel = bytes((0x2A, 0x66, 0xE9, 0xFF))  # BGRA
    xor_bitmap = pixel * width * height
    and_mask = b"\x00" * (4 * height)
    dib_header = struct.pack(
        "<IIIHHIIIIII",
        40,
        width,
        height * 2,
        1,
        32,
        0,
        len(xor_bitmap),
        0,
        0,
        0,
        0,
    )
    image_data = dib_header + xor_bitmap + and_mask
    icon_dir = struct.pack("<HHH", 0, 1, 1)
    icon_entry = struct.pack(
        "<BBBBHHII",
        width,
        height,
        0,
        0,
        1,
        32,
        len(image_data),
        6 + 16,
    )
    return icon_dir + icon_entry + image_data


# ── RENDERER ──────────────────────────────────────────────────────────────────


class HtmlRenderer:
    def __init__(
        self,
        output_dir: str = "output",
        template_dir: str = "templates",
        source_regions: dict[str, str] | None = None,
        source_certifications: dict[str, SourceCertification] | None = None,
        report_base_url: str = "",
        umami_website_id: str = "",
        umami_script_url: str = "",
        google_site_verification: str = "",
        bing_site_verification: str = "",
        schedule_timezone: str = "UTC",
    ) -> None:
        self.output_dir = Path(output_dir)
        self.template_file = "report-template.html"
        self.source_regions: dict[str, str] = source_regions or {}
        self.source_certifications: dict[str, SourceCertification] = source_certifications or {}
        # Public base URL used for SEO canonical / sitemap / OG absolute URLs.
        # Empty in tests or when REPORT_BASE_URL is unset — SEO URL tags are
        # then omitted rather than emitting broken localhost/relative URLs.
        self.report_base_url = (report_base_url or "").rstrip("/")
        # Self-hosted Umami analytics; tracker is emitted only when both are set.
        self.umami_website_id = (umami_website_id or "").strip()
        self.umami_script_url = (umami_script_url or "").strip()
        # Search-console verification tokens (rendered as <meta> tags when set).
        self.google_site_verification = (google_site_verification or "").strip()
        self.bing_site_verification = (bing_site_verification or "").strip()
        # Schedule timezone (IANA name) — used for the RSS feed's pubDate offset.
        self.schedule_timezone = (schedule_timezone or "UTC").strip() or "UTC"
        # Render a second English edition at /en/{date}/ (hreflang-linked with
        # the Chinese pages). Off by default; scheduler wires it from
        # output.english.separate_edition.
        self.english_edition_enabled = False
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(["html"]),
        )

    def _write_pwa_assets(self) -> None:
        """Write manifest.json, sw.js, and icon PNGs to the output root."""
        manifest_path = self.output_dir / "manifest.json"
        manifest_path.write_text(_MANIFEST_JSON, encoding="utf-8")
        manifest_path.chmod(0o644)

        sw_path = self.output_dir / "sw.js"
        sw_path.write_text(_SW_JS, encoding="utf-8")
        sw_path.chmod(0o644)

        icons_dir = self.output_dir / "icons"
        icons_dir.mkdir(parents=True, exist_ok=True)
        static_icons = Path(__file__).resolve().parent.parent / "static" / "icons"
        for name in ("icon-192.png", "icon-512.png", "apple-touch-icon.png"):
            dest = icons_dir / name
            src = static_icons / name
            if not dest.exists() and src.exists():
                import shutil
                shutil.copy2(src, dest)
                dest.chmod(0o644)

    def _write_fonts(self) -> None:
        """Copy static/fonts/ to output/fonts/ (idempotent, skips existing)."""
        src_dir = Path(__file__).resolve().parent.parent / "static" / "fonts"
        if not src_dir.is_dir():
            return
        dest_dir = self.output_dir / "fonts"
        dest_dir.mkdir(parents=True, exist_ok=True)
        for src_file in src_dir.iterdir():
            if not src_file.is_file():
                continue
            dest_file = dest_dir / src_file.name
            if dest_file.exists():
                continue
            import shutil
            shutil.copy2(src_file, dest_file)
            dest_file.chmod(0o644)

    def _write_static_favicon(self, report_dir: Path) -> None:
        favicon_bytes = _favicon_ico_bytes()
        for path in (self.output_dir / "favicon.ico", report_dir / "favicon.ico"):
            if path.exists() and path.read_bytes() == favicon_bytes:
                continue
            path.write_bytes(favicon_bytes)
            path.chmod(0o644)

    def _source_flag(self, source_name: str, search_region: str | None = None) -> str:
        """Get flag emoji for a source.

        For searched articles, use search_region directly.
        For organic articles, look up region from source_regions mapping.
        """
        if search_region:
            return region_flag(search_region) or "🌐"
        region = self.source_regions.get(source_name, "")
        return region_flag(region)

    def _provenance_label(
        self,
        source_kind: str,
        platform: str | None,
        is_searched: bool,
        lang: str = "zh",
    ) -> str | None:
        if source_kind == "official_web":
            return "官方网站" if lang == "zh" else "Official website"
        if source_kind == "official_social":
            platform_name = {
                "zh": {"x": "官方X", "youtube": "官方YouTube"},
                "en": {"x": "Official X", "youtube": "Official YouTube"},
            }[lang].get(platform or "", "官方渠道" if lang == "zh" else "Official channel")
            return platform_name
        if is_searched:
            return "搜索补充" if lang == "zh" else "Search supplement"
        return None

    def _article_meta(self, summary: ClusterSummary) -> dict[str, list[dict]]:
        by_source: dict[str, list[dict]] = defaultdict(list)
        for article in summary.cluster.articles:
            by_source[article.source_name].append(
                {
                    "url": article.url,
                    "is_searched": article.is_searched,
                    "search_region": article.search_region,
                    "source_kind": article.source_kind,
                    "platform": article.platform,
                    "is_official_source": article.is_official_source,
                    "origin_region": article.origin_region,
                    "searched_provider": article.searched_provider,
                    "is_placeholder": not _is_real_article(article),
                    "search_acceptance_status": getattr(article, "search_acceptance_status", None),
                    "search_acceptance_reason": getattr(article, "search_acceptance_reason", None),
                    "search_stage_trace": getattr(article, "search_stage_trace", []),
                    "result_freshness_state": getattr(article, "result_freshness_state", None),
                    "search_evidence_role": getattr(article, "search_evidence_role", None),
                }
            )
        return dict(by_source)

    def _select_source_meta(
        self,
        source_name: str,
        article_meta: dict[str, list[dict]],
        source_cursors: dict[str, int] | None = None,
    ) -> dict:
        entries = article_meta.get(source_name, [])
        if not entries:
            return {}
        if len(entries) == 1:
            return entries[0]
        if source_cursors is None:
            ambiguous = dict(entries[-1])
            ambiguous["ambiguous_url_count"] = len(entries)
            return ambiguous
        index = source_cursors.get(source_name, 0)
        if index < len(entries):
            source_cursors[source_name] = index + 1
            return entries[index]
        ambiguous = dict(entries[-1])
        ambiguous["url"] = None
        ambiguous["ambiguous_url_count"] = len(entries)
        return ambiguous

    def _build_source_entry(
        self,
        source_name: str,
        article_meta: dict[str, list[dict]],
        source_cursors: dict[str, int] | None = None,
    ) -> dict:
        meta = self._select_source_meta(source_name, article_meta, source_cursors)
        cert = self.source_certifications.get(source_name)
        is_searched = meta.get("is_searched", False)
        search_region = meta.get("search_region")
        source_kind = meta.get("source_kind", "news")
        platform = meta.get("platform")
        provenance_label = self._provenance_label(source_kind, platform, is_searched, lang="zh")
        provenance_label_en = self._provenance_label(source_kind, platform, is_searched, lang="en")
        url = meta.get("url")
        url_domain = urlparse(url).netloc.lower().removeprefix("www.") if isinstance(url, str) else ""
        if source_name == "联合早报" and url_domain == "zaochenbao.com" and not is_searched:
            provenance_label = "转载镜像"
            provenance_label_en = "Mirror"
        elif is_searched and _is_background_search_meta(meta):
            provenance_label = "背景资料"
            provenance_label_en = "Background context"
        elif is_searched and meta.get("search_acceptance_status") == "accepted" and not (
            meta.get("result_freshness_state")
            or meta.get("search_evidence_role")
            or meta.get("search_acceptance_reason") == "current_event_perspective"
        ):
            provenance_label = "待核验"
            provenance_label_en = "Review only"
        compact_label = source_name
        compact_label_en = source_name
        if is_searched:
            compact_label = f"🔍{compact_label}"
            compact_label_en = f"🔍{compact_label_en}"
        if provenance_label:
            compact_label = f"{compact_label} · {provenance_label}"
        if provenance_label_en:
            compact_label_en = f"{compact_label_en} · {provenance_label_en}"
        is_placeholder = bool(meta.get("is_placeholder", False))
        acceptance_status = meta.get("search_acceptance_status")
        acceptance_reason = meta.get("search_acceptance_reason") or ""
        # Placeholders override the compact label + tooltip; readers see a flat
        # inline "missing perspective" marker with the failure detail on hover.
        placeholder_reason_zh, placeholder_reason_en = _placeholder_failure_label(acceptance_reason)
        if is_placeholder:
            compact_label = f"🔍{source_name}"
            compact_label_en = f"🔍{_placeholder_source_label_en(source_name)}"
            stage_detail = _placeholder_stage_detail(meta.get("search_stage_trace"))
            provenance_label, provenance_label_en = stage_detail or (
                placeholder_reason_zh,
                placeholder_reason_en,
            )
            url = None  # Nullify synthetic placeholder URL to prevent broken links in template
        return {
            "source": source_name,
            "flag": self._source_flag(source_name, search_region),
            "is_searched": is_searched,
            "is_placeholder": is_placeholder,
            "search_acceptance_status": acceptance_status,
            "search_acceptance_reason": acceptance_reason,
            "search_stage_trace": meta.get("search_stage_trace") or [],
            "placeholder_reason_zh": placeholder_reason_zh,
            "placeholder_reason_en": placeholder_reason_en,
            "search_region": search_region,
            "represented_region": meta.get("origin_region") or search_region,
            "source_kind": source_kind,
            "platform": platform,
            "is_official_source": meta.get("is_official_source", False),
            "searched_provider": meta.get("searched_provider"),
            "result_freshness_state": meta.get("result_freshness_state"),
            "search_evidence_role": meta.get("search_evidence_role"),
            "counts_as_perspective": _counts_as_current_perspective(meta),
            "provenance_label": provenance_label,
            "provenance_label_en": provenance_label_en,
            "url": url,
            "ambiguous_url_count": meta.get("ambiguous_url_count", 0),
            "compact_label": compact_label,
            "compact_label_en": compact_label_en,
            "has_certification": cert is not None,
            "cert_detail_zh": cert.detail_zh if cert else "",
            "cert_detail_en": cert.detail_en if cert else "",
            "cert_codes": [c.code for c in cert.certifications] if cert else [],
        }

    def _perspective_groups_data(self, summary: ClusterSummary, english: bool = False) -> list[tuple[list[str], str]]:
        groups = summary.grouped_perspectives_en if english else summary.grouped_perspectives
        if groups:
            return [(group.sources, group.perspective) for group in groups]
        if not english and summary.perspectives:
            return [([source_name], text) for source_name, text in summary.perspectives.items()]
        return []

    def _group_payload(self, source_entries: list[dict], perspective: str, perspective_en: str = "") -> dict:
        return {
            "label": " / ".join(entry["compact_label"] for entry in source_entries),
            "label_en": " / ".join(entry["compact_label_en"] for entry in source_entries),
            "sources": source_entries,
            "perspective": perspective,
            "perspective_en": perspective_en,
            "url": source_entries[0]["url"] if len(source_entries) == 1 else None,
            "source_count": len(source_entries),
            "is_grouped": len(source_entries) > 1,
        }

    def _build_footer_sources(self, summary: ClusterSummary, preferred_sources: list[str] | None = None) -> list[dict]:
        article_meta = self._article_meta(summary)
        # Issue #1: include inline placeholder sources (missing-perspective
        # markers) even though they don't count toward cluster.sources /
        # is_multi_source. Without this, a failed regional search leaves no
        # visible trace in the source list.
        ordered_sources = list(preferred_sources) if preferred_sources is not None else list(summary.cluster.sources)
        if preferred_sources is not None:
            # Keep accepted-but-non-perspective evidence visible as labelled
            # provenance instead of silently dropping it from the footer.
            article_meta = self._article_meta(summary)
            ordered_sources.extend(
                source_name
                for source_name in summary.cluster.sources
                if source_name not in ordered_sources
                and any(
                    bool(entry.get("is_searched"))
                    and entry.get("search_acceptance_status") == "accepted"
                    for entry in article_meta.get(source_name, [])
                )
            )
        for article in summary.cluster.articles:
            if not _is_real_article(article) and article.source_name not in ordered_sources:
                ordered_sources.append(article.source_name)
        seen: set[str] = set()
        footer_sources: list[dict] = []
        for source_name in ordered_sources:
            if source_name in seen:
                continue
            seen.add(source_name)
            footer_sources.append(self._build_source_entry(source_name, article_meta))
        return footer_sources

    def _build_single_language_perspective_payload(
        self,
        summary: ClusterSummary,
        english: bool = False,
    ) -> dict[str, object]:
        article_meta = self._article_meta(summary)
        group_definitions = self._perspective_groups_data(summary, english=english)
        real_source_names = {
            article.source_name
            for article in summary.cluster.articles
            if _is_real_article(article)
        }
        is_multi_source = len(real_source_names) >= 2
        if not group_definitions:
            footer_sources = self._build_footer_sources(summary)
            return {
                "grouped_perspectives": [],
                "perspectives_list": [],
                "source_groups": [self._group_payload([entry], "") for entry in footer_sources],
                "footer_sources": footer_sources,
                "rendered_perspectives": {},
                "distinct_perspective_count": 0,
                "suppressed_group_count": 0,
                "has_expandable_perspectives": False,
                "perspective_preview": "",
            }

        renderable_groups: list[dict] = []
        perspectives_list: list[dict] = []
        rendered_perspectives: dict[str, str] = {}
        rendered_source_names: list[str] = []
        source_cursors: dict[str, int] = defaultdict(int)
        suppressed_group_count = 0
        eligible_source_names = {
            source_name
            for source_name, entries in article_meta.items()
            if any(_counts_as_current_perspective(entry) for entry in entries)
        }

        for sources, perspective in group_definitions:
            sources = [source_name for source_name in sources if source_name in eligible_source_names]
            if not sources:
                suppressed_group_count += 1
                continue
            source_entries = [
                self._build_source_entry(source_name, article_meta, source_cursors)
                for source_name in sources
            ]
            if not source_entries:
                continue
            if not _is_renderable_perspective(perspective):
                suppressed_group_count += 1
                continue
            group_payload = self._group_payload(source_entries, perspective)
            renderable_groups.append(group_payload)
            rendered_source_names.extend(entry["source"] for entry in source_entries)
            for entry in source_entries:
                rendered_perspectives[entry["source"]] = perspective
                perspectives_list.append({**entry, "text": perspective})

        if renderable_groups:
            footer_sources = self._build_footer_sources(summary, rendered_source_names)
            source_groups = renderable_groups
        else:
            footer_sources = self._build_footer_sources(summary)
            source_groups = [self._group_payload([entry], "") for entry in footer_sources]

        distinct_perspective_count = len(renderable_groups)
        perspective_preview = ""
        if distinct_perspective_count >= 2:
            preview_texts = [
                _truncate_preview(group["perspective"])
                for group in renderable_groups[:2]
                if group["perspective"]
            ]
            perspective_preview = " / ".join(preview_texts)

        return {
            "grouped_perspectives": renderable_groups if is_multi_source else [],
            "perspectives_list": perspectives_list if is_multi_source else [],
            "source_groups": source_groups,
            "footer_sources": footer_sources,
            "rendered_perspectives": rendered_perspectives,
            "distinct_perspective_count": distinct_perspective_count if is_multi_source else 0,
            "suppressed_group_count": suppressed_group_count,
            "has_expandable_perspectives": is_multi_source and distinct_perspective_count >= 2,
            "perspective_preview": perspective_preview,
        }

    def _build_perspective_payload(self, summary: ClusterSummary) -> dict[str, object]:
        return self._build_single_language_perspective_payload(summary, english=False)

    def _enrich_bilingual_perspective_payload(
        self,
        zh_payload: dict[str, object],
        en_payload: dict[str, object],
    ) -> None:
        zh_groups = zh_payload.get("grouped_perspectives", [])
        en_groups = en_payload.get("grouped_perspectives", [])
        if isinstance(zh_groups, list) and isinstance(en_groups, list):
            for zh_group, en_group in zip(zh_groups, en_groups):
                if isinstance(zh_group, dict) and isinstance(en_group, dict):
                    zh_group["perspective_en"] = en_group.get("perspective", "")

        zh_source_groups = zh_payload.get("source_groups", [])
        en_source_groups = en_payload.get("source_groups", [])
        if isinstance(zh_source_groups, list) and isinstance(en_source_groups, list):
            for zh_group, en_group in zip(zh_source_groups, en_source_groups):
                if isinstance(zh_group, dict) and isinstance(en_group, dict):
                    zh_group["perspective_en"] = en_group.get("perspective", "")
                    zh_group["label_en"] = en_group.get("label_en", zh_group.get("label_en", ""))

    def _english_available(
        self,
        summaries: list[ClusterSummary],
        hot_topics: list[dict[str, object]],
        focus_storylines: list[dict[str, object]],
        positive_summaries: list[ClusterSummary] | None = None,
    ) -> bool:
        positive_summaries = positive_summaries or []
        visible_summaries = list(summaries) + list(positive_summaries)
        for family in hot_topics:
            family_summaries = family.get("summaries", [])
            if isinstance(family_summaries, list):
                visible_summaries.extend(
                    summary for summary in family_summaries if isinstance(summary, ClusterSummary)
                )
        if not visible_summaries:
            return False
        if any(not summary.summary_en for summary in visible_summaries):
            return False
        if any(not looks_like_chinese_text(summary.summary) for summary in visible_summaries):
            return False
        for family in hot_topics:
            if not family.get("macro_topic_name_en") or not family.get("storyline_name_en"):
                return False
        return True

    def _build_grouped_perspectives(self, summary: ClusterSummary) -> list[dict]:
        if not summary.cluster.is_multi_source:
            return []
        return self._build_perspective_payload(summary)["grouped_perspectives"]  # type: ignore[return-value]

    def _build_perspectives_list(self, summary: ClusterSummary) -> list[dict]:
        return self._build_perspective_payload(summary)["perspectives_list"]  # type: ignore[return-value]

    def _build_cluster_payload(
        self,
        summary: ClusterSummary,
        index: int,
        storyline_display_mode: str = "main",
        include_english: bool = False,
    ) -> tuple[dict, dict]:
        counts = _counts_for_summary(summary, self.source_regions)
        articles_data = [
            {
                "title": article.title,
                "url": article.url if _is_real_article(article) else None,
                "source": article.source_name,
                "published_at": article.published_at.strftime("%H:%M") if article.published_at else "",
                "search_acceptance_status": getattr(article, "search_acceptance_status", "accepted" if article.is_searched else None),
                "search_acceptance_reason": getattr(article, "search_acceptance_reason", ""),
                "search_stage_trace": getattr(article, "search_stage_trace", []),
                "result_freshness_state": getattr(article, "result_freshness_state", None),
                "is_placeholder": not _is_real_article(article),
                "is_real_article": _is_real_article(article),
            }
            for article in summary.cluster.articles
        ]

        headline_raw = _extract_headline(summary.summary) or summary.cluster.topic_category
        body_text = _body_only(summary.summary)
        headline_raw_en = _extract_headline(summary.summary_en or "") if include_english and summary.summary_en else ""
        body_text_en = _body_only(summary.summary_en or "") if include_english and summary.summary_en else ""
        perspective_payload = self._build_perspective_payload(summary)
        perspective_payload_en = (
            self._build_single_language_perspective_payload(summary, english=True)
            if include_english
            else {
                "grouped_perspectives": [],
                "perspectives_list": [],
                "source_groups": [],
                "footer_sources": [],
                "rendered_perspectives": {},
                "distinct_perspective_count": 0,
                "suppressed_group_count": 0,
                "has_expandable_perspectives": False,
                "perspective_preview": "",
            }
        )
        self._enrich_bilingual_perspective_payload(perspective_payload, perspective_payload_en)
        source_groups = perspective_payload["source_groups"]
        footer_sources = perspective_payload["footer_sources"]
        grouped_perspectives = perspective_payload["grouped_perspectives"]
        grouped_perspectives_en = perspective_payload_en["grouped_perspectives"]
        perspectives_list = perspective_payload["perspectives_list"]
        perspectives_list_en = perspective_payload_en["perspectives_list"]
        broad = _broad_category(
            summary.cluster.topic_category,
            getattr(summary, "display_category", None) or getattr(summary.cluster, "display_category", None),
        )
        base = {
            "index": index,
            "topic": summary.cluster.topic_category,
            "broad_category": broad,
            "broad_category_en": _BROAD_CATEGORY_EN_MAP.get(broad, broad),
            "sources": list(dict.fromkeys(
                article.source_name
                for article in summary.cluster.articles
                if _is_real_article(article)
            )),
            "is_multi": counts["real_source_count"] >= 2,
            "perspectives": perspective_payload["rendered_perspectives"],
            "grouped_perspectives": grouped_perspectives,
            "grouped_perspectives_en": grouped_perspectives_en,
            "perspectives_list": perspectives_list,
            "perspectives_list_en": perspectives_list_en,
            "source_groups": source_groups,
            "source_groups_en": perspective_payload_en["source_groups"],
            "footer_sources": footer_sources,
            "footer_sources_en": perspective_payload_en["footer_sources"],
            "distinct_perspective_count": perspective_payload["distinct_perspective_count"],
            "suppressed_group_count": perspective_payload["suppressed_group_count"],
            "perspective_preview": perspective_payload["perspective_preview"],
            "perspective_preview_en": perspective_payload_en["perspective_preview"],
            "source_confirmation_preview": "",
            "source_confirmation_preview_en": "",
            "has_expandable_perspectives": perspective_payload["has_expandable_perspectives"],
            "articles": articles_data,
            "article_count": counts["article_count"],
            "real_article_count": counts["real_article_count"],
            "placeholder_count": counts["placeholder_count"],
            "total_article_count": counts["total_article_count"],
            "real_source_count": counts["real_source_count"],
            "placeholder_source_count": counts["placeholder_source_count"],
            "freshness_state": getattr(summary, "freshness_state", "new"),
            "is_developing": getattr(summary, "freshness_state", "new") == "developing",
            "storyline_key": getattr(summary, "storyline_key", None),
            "storyline_name": getattr(summary, "storyline_name", None),
            "storyline_name_en": getattr(summary, "storyline_name_en", None) if include_english else None,
            "storyline_role": getattr(summary, "storyline_role", "none"),
            "storyline_confidence": getattr(summary, "storyline_confidence", 0.0),
            "storyline_state": getattr(summary, "storyline_state", "emerging"),
            "storyline_timeline": [
                {
                    "storyline_key": event.storyline_key,
                    "event_date": event.event_date,
                    "title": event.title,
                    "state": event.state,
                    "summary": event.summary,
                    "cluster_id": event.cluster_id,
                    "quality_score": event.quality_score,
                    "event_type": event.event_type,
                }
                for event in list(getattr(summary, "storyline_timeline", []) or [])
            ],
            "storyline_membership_status": getattr(summary, "storyline_membership_status", "none"),
            "storyline_anchor_labels": list(getattr(summary, "storyline_anchor_labels", [])),
            "storyline_display_mode": storyline_display_mode,
            "quality_score": round(float(getattr(summary, "quality_score", 0.0) or 0.0), 3),
            "quality_status": getattr(summary, "quality_status", "unknown"),
            "quality_flags": list(getattr(summary, "quality_flags", []) or []),
            "confirmed_claims": list(getattr(summary, "confirmed_claims", []) or []),
            "contested_claims": list(getattr(summary, "contested_claims", []) or []),
            "evidence_summary": getattr(summary, "evidence_summary", ""),
            "short_topic_name": getattr(summary, "short_topic_name", None),
            "short_topic_name_en": getattr(summary, "short_topic_name_en", None) if include_english else None,
            "macro_topic_name": getattr(summary, "macro_topic_name", None),
            "macro_topic_name_en": getattr(summary, "macro_topic_name_en", None) if include_english else None,
            "topic_icon_key": getattr(summary, "topic_icon_key", None),
            "organic_unique_regions": counts["organic_unique_regions"],
            "organic_unique_sources": counts["organic_unique_sources"],
            "event_signature": getattr(summary, "event_signature", None),
            "duplicate_action": getattr(summary, "duplicate_action", "kept"),
            "duplicate_reason": getattr(summary, "duplicate_reason", ""),
            "duplicate_confidence": getattr(summary, "duplicate_confidence", 0.0),
            "selection_score": getattr(summary, "selection_score", None),
            "selection_reasons": list(getattr(summary, "selection_reasons", [])),
            "cluster_id": getattr(summary, "cluster_db_id", None),
            "impact_composite": round(float(impact.composite), 3) if (impact := getattr(summary, "impact", None)) else None,
            "impact_rationale": getattr(impact, "rationale", "") if impact else "",
        }
        return (
            {
                **base,
                "headline": Markup(html_lib.escape(headline_raw)),
                "summary": _md_to_html(body_text),
                "headline_en": Markup(html_lib.escape(headline_raw_en)) if headline_raw_en else None,
                "summary_en": _md_to_html(body_text_en) if body_text_en else None,
            },
            {
                **base,
                "headline": headline_raw,
                "summary": body_text,
                "headline_en": headline_raw_en or None,
                "summary_en": body_text_en if include_english else None,
            },
        )

    def _render_cn_edition(
        self,
        report_date: date,
        common: dict,
        clusters_ctx: list[dict],
        sections: list[dict],
        positive_ctx: list[dict],
        hot_topics_ctx: list[dict],
        template,
        update_latest: bool,
    ) -> None:
        """Render the Chinese edition at /cn/{date}/ for a dual-edition day.

        The root /{date}/ page of a dual day is the English edition (site
        default); this writes the Chinese twin with its own /cn/ canonical and
        zh_CN SEO context. Called only when the separate edition is enabled and
        English content exists.
        """
        date_str = report_date.isoformat()
        common_cn = dict(common)
        common_cn.update(
            self._build_seo_context(
                report_date,
                [
                    {"headline": c.get("headline"), "headline_en": c.get("headline_en")}
                    for c in clusters_ctx
                ],
                True,
                edition="zh",
                zh_under_cn=True,
            )
        )
        common_cn["default_language"] = "zh"
        common_cn["available_languages"] = ["zh", "en"]
        common_cn["day_links"] = self._build_day_links(report_date, edition="zh")
        common_cn["archive_link"] = "/cn/archive/"
        common_cn["language_crosslink"] = f"/{date_str}/"

        cn_dir = self.output_dir / "cn" / date_str
        cn_dir.mkdir(parents=True, exist_ok=True)
        page = template.render(
            **common_cn,
            clusters=clusters_ctx,
            sections=sections,
            main_sections=sections,
            positive_stories=positive_ctx,
            hot_topics=hot_topics_ctx,
        )
        cn_index = cn_dir / "index.html"
        cn_index.write_text(page, encoding="utf-8")
        cn_index.chmod(0o644)

        if update_latest:
            cn_latest = self.output_dir / "cn" / "latest"
            if cn_latest.is_symlink():
                cn_latest.unlink()
            try:
                cn_latest.symlink_to(date_str)
            except OSError:
                pass
        logger.info("Chinese edition written: %s", cn_index)

    def _is_en_primary(self, date_str: str) -> bool:
        """True when the root report for a date is the English edition.

        Marked in data.json (`default_language: "en"`) on dual-edition days;
        legacy/absent payloads read as Chinese-primary.
        """
        try:
            payload = json.loads(
                (self.output_dir / date_str / "data.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError):
            return False
        return isinstance(payload, dict) and payload.get("default_language") == "en"

    def _zh_href(self, date_str: str) -> str | None:
        """Path of the Chinese edition for a date, or None if it doesn't exist.

        /cn/{date}/ on dual-edition days (and legacy dates migrated to /cn),
        the root URL itself on zh-only fallback days.
        """
        if (self.output_dir / "cn" / date_str / "index.html").exists():
            return f"/cn/{date_str}/"
        if (self.output_dir / date_str / "index.html").exists() and not self._is_en_primary(date_str):
            return f"/{date_str}/"
        return None

    def _build_day_links(
        self, report_date: date, days: int = 3, edition: str = "zh"
    ) -> list[dict[str, object]]:
        """Adjacent-day footer links as real date URLs for THIS page's edition.

        English pages link neighbouring English-primary root dates; Chinese
        pages link the neighbouring Chinese URLs (/cn/ when it exists, the root
        URL on zh-only fallback days). /p/N/ symlinks are still maintained for
        already-indexed/bookmarked alias URLs; nothing links to them.
        """
        day_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        day_names_en = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        links: list[dict[str, object]] = []
        for offset, label, label_en in (
            (-1, "前一天", "Prev day"),
            (1, "后一天", "Next day"),
        ):
            current = report_date + timedelta(days=offset)
            date_str = current.isoformat()
            if edition == "en":
                href = f"/{date_str}/" if (
                    (self.output_dir / date_str / "index.html").exists()
                    and self._is_en_primary(date_str)
                ) else None
            else:
                href = self._zh_href(date_str)
            links.append(
                {
                    "date": date_str,
                    "label": label,
                    "label_en": label_en,
                    "date_display": current.strftime("%m月%d日"),
                    "date_display_en": current.strftime("%b %d"),
                    "day_name": day_names[current.weekday()],
                    "day_name_en": day_names_en[current.weekday()],
                    "href": href,
                    "available": href is not None,
                    "active": False,
                }
            )
        return links

    def _promote_day_symlinks(self, report_date: date, days: int) -> None:
        """Rotate output/p/N symlinks to point at the past-N-days reports.

        Mirrors the safety posture of the `latest` symlink: any failure logs
        a warning but never raises, so a transient FS issue can't break a
        publish.
        """
        if days <= 0:
            return
        p_root = self.output_dir / "p"
        try:
            p_root.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.warning("Failed to create %s for day symlinks", p_root, exc_info=True)
            return
        for offset in range(1, days + 1):
            target_name = (report_date - timedelta(days=offset)).isoformat()
            target_dir = self.output_dir / target_name
            link = p_root / str(offset)
            try:
                if link.is_symlink() or link.is_file():
                    link.unlink()
                elif link.exists():
                    # Directory (shouldn't happen, but recover defensively).
                    import shutil
                    shutil.rmtree(link)
                if target_dir.exists():
                    link.symlink_to(f"../{target_name}")
            except OSError:
                logger.warning(
                    "Failed to rotate day symlink %s -> ../%s",
                    link,
                    target_name,
                    exc_info=True,
                )

    def render(
        self,
        summaries: list[ClusterSummary],
        report_date: date,
        hot_topics: list[dict[str, object]] | None = None,
        focus_storylines: list[dict[str, object]] | None = None,
        positive_summaries: list[ClusterSummary] | None = None,
        report_subdir: str | Path | None = None,
        update_latest: bool = True,
    ) -> Path:
        date_str = report_date.isoformat()
        report_base = self.output_dir / Path(report_subdir) if report_subdir else self.output_dir
        report_dir = report_base / date_str
        report_dir.mkdir(parents=True, exist_ok=True)
        report_dir.chmod(0o755)
        self._write_static_favicon(report_dir)
        self._write_pwa_assets()
        self._write_fonts()

        hot_topics = hot_topics or []
        positive_summaries = positive_summaries or []
        english_available = self._english_available(summaries, hot_topics, [], positive_summaries)
        clusters_ctx = []
        clusters_json: list[dict] = []

        for i, cs in enumerate(summaries, 1):
            ctx_payload, json_payload = self._build_cluster_payload(
                cs,
                i,
                storyline_display_mode="main",
                include_english=english_available,
            )
            clusters_ctx.append(ctx_payload)
            clusters_json.append(json_payload)

        # Defensive fallback (Issue #2 rec #4 main-lane branch): if 2+ main-lane
        # clusters share a storyline_key (e.g. a 2-member family that didn't
        # claim a tab because of max_topic_tabs), tag them with a shared label
        # so the reader can see the connection. Flat tag — no card/lift/shadow.
        _CJK_CHAR = re.compile(r"[一-鿿]")
        shared_label_by_key: dict[str, str] = {}
        shared_label_en_by_key: dict[str, str] = {}
        key_counts: dict[str, int] = {}
        for ctx in clusters_ctx:
            key = ctx.get("storyline_key")
            if not key or key.startswith("single-"):
                continue
            key_counts[key] = key_counts.get(key, 0) + 1
            if key not in shared_label_by_key:
                shared_label_by_key[key] = ctx.get("storyline_name") or ctx.get("macro_topic_name") or ""
                shared_label_en_by_key[key] = ctx.get("storyline_name_en") or shared_label_by_key[key]
        for ctx, jsonp in zip(clusters_ctx, clusters_json):
            key = ctx.get("storyline_key")
            if key and key_counts.get(key, 0) >= 2:
                ctx["shared_storyline_label"] = shared_label_by_key.get(key, "")
                ctx["shared_storyline_label_en"] = shared_label_en_by_key.get(key, "")
                jsonp["shared_storyline_label"] = ctx["shared_storyline_label"]
                jsonp["shared_storyline_label_en"] = ctx["shared_storyline_label_en"]

        present_categories = {cluster["broad_category"] for cluster in clusters_ctx}
        sections = []
        for label, emoji, css_key in _CATEGORY_META:
            if label in present_categories:
                sections.append(
                    {
                        "label": label,
                        "label_zh": display_category_label_zh(label),
                        "label_en": _BROAD_CATEGORY_EN_MAP.get(label, label),
                        "emoji": emoji,
                        "css_key": css_key,
                    }
                )

        hot_topics_ctx: list[dict] = []
        hot_topics_json: list[dict] = []
        used_hot_topic_labels: set[str] = set()
        used_hot_topic_full_labels: set[str] = set()
        for i, family in enumerate(hot_topics, 1):
            family_summaries = family.get("summaries", [])
            if not isinstance(family_summaries, list):
                continue
            family_name = family.get("macro_topic_name") if isinstance(family.get("macro_topic_name"), str) else None
            family_name_en = None
            if english_available:
                family_name_en = str(family.get("macro_topic_name_en") or "").strip() or None
            topic_name, topic_name_en = _repair_hot_topic_label(
                family_name,
                family_name_en,
                [summary for summary in family_summaries if isinstance(summary, ClusterSummary)],
            )
            # Full-length family label for in-body headers. Navigation tabs
            # keep the capped topic_name; overview + topic-stage headers show
            # the complete storyline title. Same repair logic, wider cap.
            family_name_full = family.get("macro_topic_name_full")
            family_name_full_en = family.get("macro_topic_name_full_en")
            topic_name_full, topic_name_full_en = _repair_hot_topic_label(
                family_name_full if isinstance(family_name_full, str) else None,
                family_name_full_en if isinstance(family_name_full_en, str) else None,
                [summary for summary in family_summaries if isinstance(summary, ClusterSummary)],
                max_chars=_HOT_TOPIC_FULL_NAME_MAX_CHARS,
            )
            typed_summaries = [summary for summary in family_summaries if isinstance(summary, ClusterSummary)]
            storyline_name = family.get("storyline_name") if isinstance(family.get("storyline_name"), str) else None
            if topic_name.casefold() in used_hot_topic_labels:
                topic_name, topic_name_en = _disambiguate_hot_topic_label(
                    topic_name,
                    topic_name_en,
                    storyline_name,
                    typed_summaries,
                    used_hot_topic_labels,
                    max_chars=10,
                )
            if topic_name_full.casefold() in used_hot_topic_full_labels:
                topic_name_full, topic_name_full_en = _disambiguate_hot_topic_label(
                    topic_name_full,
                    topic_name_full_en,
                    storyline_name,
                    typed_summaries,
                    used_hot_topic_full_labels,
                    max_chars=_HOT_TOPIC_FULL_NAME_MAX_CHARS,
                )
            used_hot_topic_labels.add(topic_name.casefold())
            used_hot_topic_full_labels.add(topic_name_full.casefold())
            icon_key = family.get("topic_icon_key") if isinstance(family.get("topic_icon_key"), str) else "globe"
            if icon_key not in _HOT_TOPIC_ICON_MAP:
                icon_key = "globe"

            member_ctx: list[dict] = []
            member_json: list[dict] = []
            core_count = 0
            spillover_count = 0
            for member_index, summary in enumerate(family_summaries, 1):
                ctx_payload, json_payload = self._build_cluster_payload(
                    summary,
                    member_index,
                    storyline_display_mode="hot_topic",
                    include_english=english_available,
                )
                ctx_payload["hot_seq_index"] = member_index
                json_payload["hot_seq_index"] = member_index
                ctx_payload["macro_topic_name"] = topic_name
                json_payload["macro_topic_name"] = topic_name
                ctx_payload["macro_topic_name_en"] = topic_name_en
                json_payload["macro_topic_name_en"] = topic_name_en
                ctx_payload["storyline_name"] = topic_name
                json_payload["storyline_name"] = topic_name
                ctx_payload["storyline_name_en"] = topic_name_en
                json_payload["storyline_name_en"] = topic_name_en
                if ctx_payload["storyline_role"] == "core":
                    core_count += 1
                elif ctx_payload["storyline_role"] == "spillover":
                    spillover_count += 1
                member_ctx.append(ctx_payload)
                member_json.append(json_payload)

            scope_summary = f"收纳 {len(member_ctx)} 条相关报道，其中 {core_count} 条为核心事件。"
            scope_summary_en = f"Collects {len(member_ctx)} related stories, including {core_count} core events."
            preview_clusters = [
                {
                    "headline": member["headline"],
                    "headline_en": member.get("headline_en"),
                    "hot_seq_index": member["hot_seq_index"],
                }
                for member in member_json[:2]
            ]

            hot_topics_ctx.append(
                {
                    "dom_id": family.get("dom_id", f"hot-topic-{i}"),
                    "macro_topic_key": family.get("macro_topic_key", f"hot-topic-{i}"),
                    "macro_topic_name": topic_name,
                    "macro_topic_name_en": topic_name_en,
                    "macro_topic_name_full": topic_name_full,
                    "macro_topic_name_full_en": topic_name_full_en,
                    "storyline_key": family.get("storyline_key", family.get("macro_topic_key", f"hot-topic-{i}")),
                    "storyline_name": topic_name,
                    "storyline_name_en": topic_name_en,
                    "storyline_name_full": topic_name_full,
                    "storyline_name_full_en": topic_name_full_en,
                    "topic_icon_key": icon_key,
                    "topic_icon": _HOT_TOPIC_ICON_MAP.get(icon_key, _HOT_TOPIC_ICON_MAP["globe"]),
                    "anchor_labels": list(family.get("anchor_labels", [])),
                    "member_count": len(member_ctx),
                    "core_count": core_count,
                    "spillover_count": spillover_count,
                    "scope_summary": scope_summary,
                    "scope_summary_en": scope_summary_en,
                    "preview_clusters": preview_clusters,
                    "clusters": member_ctx,
                }
            )
            hot_topics_json.append(
                {
                    "dom_id": family.get("dom_id", f"hot-topic-{i}"),
                    "macro_topic_key": family.get("macro_topic_key", f"hot-topic-{i}"),
                    "macro_topic_name": topic_name,
                    "macro_topic_name_en": topic_name_en,
                    "macro_topic_name_full": topic_name_full,
                    "macro_topic_name_full_en": topic_name_full_en,
                    "storyline_key": family.get("storyline_key", family.get("macro_topic_key", f"hot-topic-{i}")),
                    "storyline_name": family.get("storyline_name", topic_name),
                    "storyline_name_en": str(family.get("storyline_name_en") or topic_name_en or "").strip() or None,
                    "storyline_name_full": topic_name_full,
                    "storyline_name_full_en": topic_name_full_en,
                    "topic_icon_key": icon_key,
                    "anchor_labels": list(family.get("anchor_labels", [])),
                    "member_count": len(member_json),
                    "core_count": core_count,
                    "spillover_count": spillover_count,
                    "scope_summary": scope_summary,
                    "scope_summary_en": scope_summary_en,
                    "preview_clusters": preview_clusters,
                    "clusters": member_json,
                }
            )

        # Propagate repaired hot-topic tab names to main-feed shared labels.
        # The tab label was repaired by _repair_hot_topic_label above, but
        # shared_label_by_key was populated earlier from the stale
        # clusters.storyline_name. Any main-feed card sharing a hot-topic
        # family's storyline_key should show the repaired name, not the stale
        # one (e.g. "俄乌军事升级" instead of "美以伊局势").
        for hot in hot_topics_ctx:
            hot_key = hot.get("macro_topic_key") or hot.get("storyline_key")
            if not hot_key or hot_key not in shared_label_by_key:
                continue
            repaired = hot.get("macro_topic_name") or ""
            repaired_en = hot.get("macro_topic_name_en") or repaired
            if repaired and repaired != shared_label_by_key[hot_key]:
                shared_label_by_key[hot_key] = repaired
                shared_label_en_by_key[hot_key] = repaired_en
                for ctx, jsonp in zip(clusters_ctx, clusters_json):
                    if ctx.get("storyline_key") == hot_key:
                        ctx["shared_storyline_label"] = repaired
                        ctx["shared_storyline_label_en"] = repaired_en
                        jsonp["shared_storyline_label"] = repaired
                        jsonp["shared_storyline_label_en"] = repaired_en

        # Stale-name guard: for every key (repaired or not), check the label
        # against each card's headline individually. If the label's CJK chars
        # have zero overlap with a card's CJK headline, clear the tag on THAT
        # card — the storyline family may be a grab-bag with off-topic members
        # (e.g. storyline-8aa4fcb4 contains both Ru-Ua and Trump-Hamas stories;
        # only the Ru-Ua cards should show the "俄乌军事升级" tag).
        for key in list(shared_label_by_key.keys()):
            label = shared_label_by_key[key]
            label_cjk = set(_CJK_CHAR.findall(label))
            if not label_cjk:
                continue
            for ctx, jsonp in zip(clusters_ctx, clusters_json):
                if ctx.get("storyline_key") != key:
                    continue
                if not ctx.get("shared_storyline_label"):
                    continue
                headline = ctx.get("headline") or ""
                headline_cjk = set(_CJK_CHAR.findall(headline))
                # Only suppress when the headline has CJK chars to compare.
                # Cross-script headlines (non-CJK) skip the guard.
                if headline_cjk and not label_cjk & headline_cjk:
                    ctx["shared_storyline_label"] = ""
                    ctx["shared_storyline_label_en"] = ""
                    jsonp["shared_storyline_label"] = ""
                    jsonp["shared_storyline_label_en"] = ""

        positive_ctx: list[dict] = []
        positive_json: list[dict] = []
        for i, summary in enumerate(positive_summaries, 1):
            ctx_payload, json_payload = self._build_cluster_payload(
                summary,
                i,
                storyline_display_mode="positive_energy",
                include_english=english_available,
            )
            reason = getattr(summary, "positive_energy_reason", "")
            reason_en = getattr(summary, "positive_energy_reason_en", reason)
            score = getattr(summary, "positive_energy_score", 0.0)
            category = getattr(summary, "positive_energy_category", summary.cluster.topic_category)
            source = getattr(summary, "positive_energy_source", summary.cluster.sources[0] if summary.cluster.sources else "")
            ctx_payload["positive_seq_index"] = i
            ctx_payload["positive_reason"] = reason
            ctx_payload["positive_reason_en"] = reason_en
            ctx_payload["positive_score"] = score
            ctx_payload["positive_category"] = category
            ctx_payload["positive_source"] = source
            json_payload["positive_seq_index"] = i
            json_payload["positive_reason"] = reason
            json_payload["positive_reason_en"] = reason_en
            json_payload["positive_score"] = score
            json_payload["positive_category"] = category
            json_payload["positive_source"] = source
            positive_ctx.append(ctx_payload)
            positive_json.append(json_payload)

        display_rank = 1
        for ctx_payload, json_payload in zip(clusters_ctx, clusters_json):
            ctx_payload["display_rank"] = display_rank
            ctx_payload["seq_index"] = display_rank
            json_payload["display_rank"] = display_rank
            json_payload["seq_index"] = display_rank
            display_rank += 1

        hot_topic_story_count = sum(len(family["clusters"]) for family in hot_topics_ctx)
        day_nav_cfg = {}
        try:
            day_nav_cfg = getattr(self, "day_navigation_cfg", {}) or {}
        except AttributeError:
            day_nav_cfg = {}
        day_link_count = int(day_nav_cfg.get("days", 3)) if isinstance(day_nav_cfg, dict) else 3

        dual_edition = self.english_edition_enabled and english_available
        common = {
            "report_date": date_str,
            "report_date_display": report_date.strftime("%Y年%m月%d日"),
            "report_date_display_en": report_date.strftime("%b %d, %Y"),
            "day_name": ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][
                report_date.weekday()
            ],
            "day_name_en": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][report_date.weekday()],
            "cluster_count": len(summaries),
            "focus_storyline_count": 0,
            "focus_storyline_story_count": 0,
            "hot_topic_count": len(hot_topics_ctx),
            "hot_topic_story_count": hot_topic_story_count,
            "positive_story_count": len(positive_ctx),
            "total_cluster_count": len(summaries) + len(positive_ctx) + hot_topic_story_count,
            "english_available": english_available,
            "available_languages": ["zh", "en"] if english_available else ["zh"],
            # Root is the English edition on dual days; falls back to Chinese
            # when no English content exists (data.json records which).
            "default_language": "en" if dual_edition else "zh",
            "day_links": self._build_day_links(
                report_date, day_link_count, edition="en" if dual_edition else "zh"
            ),
            "archive_link": "/archive/" if dual_edition else "/cn/archive/",
        }
        if dual_edition:
            common["language_crosslink"] = f"/cn/{date_str}/"
        common.update(
            self._build_seo_context(
                report_date,
                clusters_json,
                english_available,
                edition="en" if dual_edition else "zh",
                zh_under_cn=dual_edition,
            )
        )
        # Umami tracker values — present only when configured (see template).
        if self.umami_website_id and self.umami_script_url:
            common["umami_website_id"] = self.umami_website_id
            common["umami_script_url"] = self.umami_script_url
        # Search-console verification tokens — present only when configured.
        if self.google_site_verification:
            common["google_site_verification"] = self.google_site_verification
        if self.bing_site_verification:
            common["bing_site_verification"] = self.bing_site_verification

        template = self.env.get_template(self.template_file)
        page_html = template.render(
            **common,
            clusters=clusters_ctx,
            sections=sections,
            main_sections=sections,
            positive_stories=positive_ctx,
            hot_topics=hot_topics_ctx,
        )
        html_path = report_dir / "index.html"
        html_path.write_text(page_html, encoding="utf-8")
        html_path.chmod(0o644)

        json_path = report_dir / "data.json"
        json_path.write_text(
            json.dumps(
                {
                    **common,
                    "clusters": clusters_json,
                    "positive_stories": positive_json,
                    "hot_topics": hot_topics_json,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        json_path.chmod(0o644)

        total_story_count = (
            common["cluster_count"]
            + common["focus_storyline_story_count"]
            + common["hot_topic_story_count"]
        )
        if dual_edition:
            self._render_cn_edition(
                report_date,
                common,
                clusters_ctx,
                sections,
                positive_ctx,
                hot_topics_ctx,
                template,
                update_latest=update_latest and total_story_count > 0,
            )
        latest = self.output_dir / "latest"
        if update_latest and total_story_count > 0:
            if latest.is_symlink():
                latest.unlink()
            try:
                latest.symlink_to(date_str)
            except OSError:
                pass
            self._promote_day_symlinks(report_date, day_link_count)
        elif update_latest:
            logger.info(
                "HTML report has zero stories for %s — preserving existing latest symlink",
                date_str,
            )

        self._write_seo_files(report_date)
        logger.info("HTML report written: %s", html_path)
        return html_path

    def _build_seo_context(
        self,
        report_date: date,
        clusters_json: list[dict],
        english_available: bool,
        edition: str = "zh",
        zh_under_cn: bool = False,
    ) -> dict[str, object]:
        """Per-page SEO meta (title / description / canonical) from today's clusters.

        Plain strings only — the template stays declarative. Language editions
        are path-isolated: the root /{date}/ URL is the English edition (site
        default), the Chinese edition lives at /cn/{date}/. On days without
        English content the root falls back to Chinese and the zh canonical
        stays at the root URL (zh_under_cn=False). hreflang pairs are emitted
        only when both editions exist; x-default points at the English root.
        """
        date_display_zh = report_date.strftime("%Y年%m月%d日")
        date_display_en = report_date.strftime("%b %d, %Y")
        date_str = report_date.isoformat()

        headlines_zh = [
            str(c.get("headline") or "").strip()
            for c in clusters_json
            if str(c.get("headline") or "").strip()
        ]
        top_zh = headlines_zh[0] if headlines_zh else ""

        if headlines_zh:
            joined = "、".join(headlines_zh[:3])
            description = f"NewsPrism {date_display_zh} 多角度新闻解读：{joined}。同一事件，多国媒体立场对照"
            if len(description) > 155:
                description = description[:154].rstrip("、，。 ") + "…"
        else:
            description = f"NewsPrism · {date_display_zh} 全球新闻多角度解读：同一事件，多国媒体立场对照"

        title_zh = (
            f"NewsPrism · {date_display_zh} · {top_zh}"
            if top_zh
            else f"NewsPrism · {date_display_zh} 每日新闻多角度解读"
        )
        seo_title = title_zh[:70] + ("…" if len(title_zh) > 70 else "")
        og_locale = "zh_CN"
        in_language = "zh-CN"
        zh_path = f"/cn/{date_str}/" if zh_under_cn else f"/{date_str}/"
        canonical_url = f"{self.report_base_url}{zh_path}" if self.report_base_url else ""

        if edition == "en":
            headlines_en = [
                str(c.get("headline_en") or "").strip()
                for c in clusters_json
                if str(c.get("headline_en") or "").strip()
            ]
            top_en = headlines_en[0] if headlines_en else top_zh
            if headlines_en:
                description = (
                    f"NewsPrism {date_display_en} daily digest — one event, many perspectives: "
                    f"{'; '.join(headlines_en[:3])}."
                )
                if len(description) > 160:
                    description = description[:159].rstrip(",;. ") + "…"
            else:
                description = "NewsPrism daily digest — one event, many perspectives on the same facts."
            title_en = (
                f"NewsPrism · {date_display_en} · {top_en}"
                if top_en
                else f"NewsPrism · {date_display_en} Daily News, Many Perspectives"
            )
            seo_title = title_en[:70] + ("…" if len(title_en) > 70 else "")
            og_locale = "en_US"
            in_language = "en-US"
            canonical_url = f"{self.report_base_url}/{date_str}/" if self.report_base_url else ""

        og_image = f"{self.report_base_url}/og-image.png" if self.report_base_url else ""

        # hreflang pair — only meaningful when both editions have absolute URLs.
        hreflang_alternates: list[dict[str, str]] = []
        if self.report_base_url:
            zh_url = f"{self.report_base_url}{zh_path}"
            en_url = f"{self.report_base_url}/{date_str}/"
            if english_available:
                hreflang_alternates = [
                    {"lang": "zh", "url": zh_url},
                    {"lang": "en", "url": en_url},
                    {"lang": "x-default", "url": en_url},
                ]
            else:
                hreflang_alternates = [{"lang": "zh", "url": zh_url}]

        ld: dict = {
            "@context": "https://schema.org",
            "@type": "NewsArticle",
            "headline": seo_title,
            "description": description,
            "datePublished": date_str,
            "dateModified": date_str,
            "inLanguage": in_language,
            "author": {"@type": "Organization", "name": "NewsPrism"},
            "publisher": {"@type": "Organization", "name": "NewsPrism"},
        }
        if canonical_url:
            ld["mainEntityOfPage"] = {"@type": "WebPage", "@id": canonical_url}
        if og_image:
            ld["image"] = [og_image]
        # Escape chars that would break out of a <script> JSON-LD block; expose as
        # a pre-escaped string so Jinja's autoescape does not double-encode it.
        json_ld = (
            json.dumps(ld, ensure_ascii=False)
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
            .replace("&", "\\u0026")
        )

        return {
            "seo_title": seo_title,
            "seo_description": description,
            "canonical_url": canonical_url,
            "og_image": og_image,
            "og_locale": og_locale,
            "hreflang_alternates": hreflang_alternates,
            "json_ld": json_ld,
        }

    def _discover_report_dates(self) -> list[str]:
        """Sorted YYYY-MM-DD dirs at the output root that contain a report.

        Zero-story dates are excluded from the discovery set (they feed the
        sitemap, archive, and RSS feed) but keep their direct URL browsable.
        Fail-open: unreadable or legacy data.json keeps the date listed.
        """
        dates: list[str] = []
        if not self.output_dir.is_dir():
            return dates
        for entry in self.output_dir.iterdir():
            if (
                entry.is_dir()
                and _REPORT_DATE_DIR_RE.match(entry.name)
                and (entry / "index.html").exists()
                and self._report_has_stories(entry.name)
            ):
                dates.append(entry.name)
        return sorted(dates)

    def _report_has_stories(self, date_str: str) -> bool:
        try:
            payload = json.loads(
                (self.output_dir / date_str / "data.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError):
            return True  # fail-open: legacy/corrupt exports stay listed
        if not isinstance(payload, dict) or "total_cluster_count" not in payload:
            return True
        try:
            return int(payload.get("total_cluster_count") or 0) > 0
        except (TypeError, ValueError):
            return True

    def _write_seo_files(self, report_date: date) -> None:
        """Refresh robots.txt and sitemap.xml at the output root after each publish.

        Keeps the sitemap in sync with every published daily report. Only the
        canonical date URLs (plus the homepage) are listed — the /latest and
        /p/N/ aliases are excluded so crawlers see one URL per report and we
        avoid duplicate-content signals. Sitemap is written only when a public
        base URL is configured (absolute URLs are required by the spec).
        """
        robots_lines = ["User-agent: *", "Allow: /"]
        if self.report_base_url:
            robots_lines += ["", f"Sitemap: {self.report_base_url}/sitemap.xml"]
        robots_lines.append("")
        (self.output_dir / "robots.txt").write_text("\n".join(robots_lines), encoding="utf-8")

        # Social share image — static brand asset, copied idempotently like the
        # PWA icons. Referenced by the og:image / twitter:image meta tags.
        og_src = Path(__file__).resolve().parent.parent / "static" / "og-image.png"
        if og_src.exists():
            import shutil
            og_dest = self.output_dir / "og-image.png"
            if not (og_dest.exists() and og_dest.stat().st_size == og_src.stat().st_size):
                shutil.copy2(og_src, og_dest)
                og_dest.chmod(0o644)

        if not self.report_base_url:
            self._write_archive_page("en")
            self._write_archive_page("zh")
            return

        def _lastmod(date_dir: str) -> str:
            # mtime reflects replays/corrections, not just the original publish
            # date — Google then re-fetches fixed reports (lastmod moves forward).
            idx = self.output_dir / date_dir / "index.html"
            try:
                return datetime.fromtimestamp(idx.stat().st_mtime).date().isoformat()
            except OSError:
                return date_dir

        dates = self._discover_report_dates()
        base = self.report_base_url
        newest = _lastmod(dates[-1]) if dates else report_date.isoformat()

        def _en_lastmod(d: str) -> str:
            return _lastmod(d)

        def _zh_lastmod(d: str) -> str:
            if zh_of[d].startswith("/cn/"):
                idx = self.output_dir / "cn" / d / "index.html"
                try:
                    return datetime.fromtimestamp(idx.stat().st_mtime).date().isoformat()
                except OSError:
                    return d
            return _lastmod(d)

        def _url(loc: str, lastmod: str, alternates: list[dict[str, str]]) -> list[str]:
            block = ["  <url>", f"    <loc>{html_lib.escape(loc, quote=False)}</loc>"]
            for alt in alternates:
                block.append(
                    f'    <xhtml:link rel="alternate" hreflang="{alt["lang"]}" '
                    f'href="{html_lib.escape(alt["url"], quote=False)}"/>'
                )
            block.append(f"    <lastmod>{lastmod}</lastmod>")
            block.append("  </url>")
            return block

        dual_dates = [d for d in dates if self._is_en_primary(d)]
        zh_of = {d: self._zh_href(d) for d in dates}

        def _pair(d: str) -> list[dict[str, str]]:
            return [
                {"lang": "zh", "url": f"{base}{zh_of[d]}"},
                {"lang": "en", "url": f"{base}/{d}/"},
                {"lang": "x-default", "url": f"{base}/{d}/"},
            ]

        archive_alts = (
            [
                {"lang": "zh", "url": f"{base}/cn/archive/"},
                {"lang": "en", "url": f"{base}/archive/"},
                {"lang": "x-default", "url": f"{base}/archive/"},
            ]
            if dual_dates
            else []
        )
        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" '
            'xmlns:xhtml="http://www.w3.org/1999/xhtml">',
        ]
        lines += _url(f"{base}/", newest, [])
        lines += _url(f"{base}/archive/", newest, archive_alts)
        if (self.output_dir / "cn" / "archive" / "index.html").exists():
            lines += _url(f"{base}/cn/archive/", newest, archive_alts)
            lines += _url(f"{base}/cn/", newest, [])
        for d in dates:
            if d in dual_dates:
                # dual day: root English page + /cn Chinese twin, hreflang-paired
                lines += _url(f"{base}/{d}/", _en_lastmod(d), _pair(d))
                lines += _url(f"{base}{zh_of[d]}", _zh_lastmod(d), _pair(d))
            else:
                # zh-only day: single Chinese entry (legacy /cn/ or fallback root)
                zh_url = f"{base}{zh_of[d]}"
                lines += _url(zh_url, _zh_lastmod(d), [{"lang": "zh", "url": zh_url}])
        lines.append("</urlset>")
        (self.output_dir / "sitemap.xml").write_text("\n".join(lines) + "\n", encoding="utf-8")

        self._write_archive_page("en")
        self._write_archive_page("zh")
        self._write_feed()

    def prune_old_reports(self, retention_days: int) -> list[str]:
        """Delete report date dirs older than the retention window.

        Boundless output growth was an ops risk; the default window keeps two
        years of daily reports. Targets of the `latest` and `p/N` symlinks are
        always protected. Sitemap/archive/feed are regenerated afterwards so
        the deleted URLs disappear from discovery surfaces in the same pass.
        Returns the deleted date strings.
        """
        if retention_days <= 0:
            return []
        cutoff = date.today() - timedelta(days=retention_days)
        protected: set[str] = set()

        def _protect(link: Path) -> None:
            try:
                protected.add(Path(os.readlink(str(link))).name)
            except OSError:
                pass

        latest = self.output_dir / "latest"
        if latest.is_symlink():
            _protect(latest)
        p_root = self.output_dir / "p"
        if p_root.is_dir():
            for link in p_root.iterdir():
                if link.is_symlink():
                    _protect(link)

        deleted: list[str] = []
        if self.output_dir.is_dir():
            for entry in self.output_dir.iterdir():
                if not (entry.is_dir() and _REPORT_DATE_DIR_RE.match(entry.name)):
                    continue
                try:
                    entry_date = date.fromisoformat(entry.name)
                except ValueError:
                    continue
                if entry_date >= cutoff or entry.name in protected:
                    continue
                shutil.rmtree(entry, ignore_errors=True)
                deleted.append(entry.name)

        if deleted:
            logger.info("Output retention: pruned %d report dirs (%s ... %s)", len(deleted), deleted[0], deleted[-1])
            self._write_seo_files(date.today())
        return deleted

    def _write_archive_page(self, edition: str = "en") -> None:
        """Render the archive index: /archive/ (English) and /cn/archive/ (Chinese).

        The English archive lists dual-edition dates (root URLs); the Chinese
        archive lists every date with a Chinese page (/cn/ when it exists, the
        root URL on zh-only fallback days). Regenerated on every publish.
        """
        all_dates = self._discover_report_dates()
        if edition == "en":
            dates = [d for d in all_dates if self._is_en_primary(d)]
        else:
            dates = [d for d in all_dates if self._zh_href(d)]
        zh_days = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        en_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        months: list[dict[str, object]] = []
        recent: list[dict[str, object]] = []
        for d_str in reversed(dates):  # newest first
            d = date.fromisoformat(d_str)
            href = f"/{d_str}/" if edition == "en" else self._zh_href(d_str)
            day = {
                "date": d_str,
                "display": d.strftime("%m月%d日"),
                "display_en": d.strftime("%b %d"),
                "day_name": zh_days[d.weekday()],
                "day_name_en": en_days[d.weekday()],
                "href": href,
            }
            key = f"{d.year}-{d.month:02d}"
            if not months or months[-1]["key"] != key:
                months.append({"key": key, "label": f"{d.year}年{d.month}月", "days": []})
            months[-1]["days"].append(day)
            if len(recent) < 7:
                recent.append(day)

        total = len(dates)
        canonical = (
            f"{self.report_base_url}/{'' if edition == 'en' else 'cn/'}archive/"
            if self.report_base_url
            else ""
        )
        try:
            template = self.env.get_template("archive-template.html")
            page = template.render(
                months=months,
                recent=recent,
                total_count=total,
                canonical_url=canonical,
                edition=edition,
                umami_website_id=self.umami_website_id or None,
                umami_script_url=self.umami_script_url or None,
            )
        except Exception:
            logger.exception("Archive page rendering failed")
            return
        base_dir = self.output_dir if edition == "en" else self.output_dir / "cn"
        archive_dir = base_dir / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        (archive_dir / "index.html").write_text(page, encoding="utf-8")
        (archive_dir / "index.html").chmod(0o644)

    def _write_feed(self) -> None:
        """Write /feed.xml (RSS 2.0) listing the most recent daily reports.

        Drives subscription return-visits and gives crawlers a second discovery
        channel alongside the sitemap. Item titles/descriptions are read from
        each date's data.json (already written by the renderer), so the feed
        needs no re-render of old reports. Newest first; up to 20 items.
        """
        dates = list(reversed(self._discover_report_dates()))[:20]
        if not dates:
            return

        def esc(value: object) -> str:
            return html_lib.escape(str(value), quote=False)

        def rfc822(date_str: str) -> str:
            return _rfc822_pub_date(date_str, self.schedule_timezone)

        items: list[str] = []
        for d in dates:
            title = f"NewsPrism · {d}"
            description = "每日全球新闻多角度解读"
            try:
                payload = json.loads((self.output_dir / d / "data.json").read_text(encoding="utf-8"))
                title = str(payload.get("seo_title") or payload.get("report_date_display") or title)
                description = str(payload.get("seo_description") or description)
            except (OSError, ValueError):
                pass
            zh_path = self._zh_href(d) or f"/{d}/"
            link = f"{self.report_base_url}{zh_path}"
            items.append("    <item>")
            items.append(f"      <title>{esc(title)}</title>")
            items.append(f"      <link>{esc(link)}</link>")
            items.append(f'      <guid isPermaLink="true">{esc(link)}</guid>')
            items.append(f"      <pubDate>{rfc822(d)}</pubDate>")
            items.append(f"      <description>{esc(description)}</description>")
            items.append("    </item>")

        feed = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">',
            "  <channel>",
            "    <title>NewsPrism</title>",
            f"    <link>{esc(self.report_base_url)}/</link>",
            "    <description>同一事件，多国媒体多角度解读 · 每日全球新闻中文速览</description>",
            "    <language>zh-CN</language>",
            f'    <atom:link href="{esc(self.report_base_url)}/feed.xml" rel="self" type="application/rss+xml" />',
        ]
        feed += items
        feed += ["  </channel>", "</rss>"]
        (self.output_dir / "feed.xml").write_text("\n".join(feed) + "\n", encoding="utf-8")
