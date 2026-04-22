"""Static HTML renderer — generates daily report page from Jinja2 template.

Layer: runtime (can import types, config, repo, service)
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import re
import struct
from collections import defaultdict
from datetime import date
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from newsprism.types import ClusterSummary

logger = logging.getLogger(__name__)

# ── BROAD CATEGORY MAPPING ────────────────────────────────────────────────────

_BROAD_CATEGORY_MAP: dict[str, str] = {
    # 商业财经
    "Finance": "商业财经",
    # 科技创新
    "AI & LLM": "科技创新",
    "Smartphones & Electronics": "科技创新",
    "Smart Home": "科技创新",
    "Robotics": "科技创新",
    "Chips & Hardware": "科技创新",
    "Tech Companies - China": "科技创新",
    "Tech Companies - International": "科技创新",
    "Space": "科技创新",
    "Tech General": "科技创新",
    "Tech-General": "科技创新",
    "Chips": "科技创新",
    # 国际时政
    "Geopolitics": "国际时政",
    "Geopolitics - Extended": "国际时政",
    "Regions": "国际时政",
    "AI Policy & Regulation": "国际时政",
    "World News": "国际时政",
    # 社会民生
    "Society": "社会民生",
    # 文化艺术
    "Film - Chinese": "文化艺术",
    "Film - International": "文化艺术",
    "Film - General": "文化艺术",
    "Music": "文化艺术",
    "Games - Chinese": "文化艺术",
    "Games - Platform": "文化艺术",
    "Games - General": "文化艺术",
    "Culture": "文化艺术",
    # 体育运动
    "Sports": "体育运动",
    # 科学健康
    "Energy": "科学健康",
    "Energy & Climate": "科学健康",
    "Science & Health": "科学健康",
}

_DEFAULT_BROAD = "国际时政"

_CATEGORY_META: list[tuple[str, str, str]] = [
    # (broad_category, emoji, css_key)
    ("商业财经", "💰", "finance"),
    ("科技创新", "🔬", "tech"),
    ("国际时政", "🌍", "world"),
    ("社会民生", "🏛️", "society"),
    ("文化艺术", "🎭", "culture"),
    ("体育运动", "🏅", "sport"),
    ("科学健康", "🔭", "science"),
]

# ── REGION → FLAG EMOJI ───────────────────────────────────────────────────────

_REGION_FLAG: dict[str, str] = {
    "cn": "🇨🇳",
    "us": "🇺🇸",
    "gb": "🇬🇧",
    "de": "🇩🇪",
    "nl": "🇳🇱",
    "pl": "🇵🇱",
    "jp": "🇯🇵",
    "kr": "🇰🇷",
    "ru": "🇷🇺",
    "in": "🇮🇳",
    "sg": "🇸🇬",
    "fr": "🇫🇷",
    "eu": "🇪🇺",
    "au": "🇦🇺",
    "ca": "🇨🇦",
}

_HOT_TOPIC_ICON_MAP: dict[str, str] = {
    "globe": "🌍",
    "war": "⚠️",
    "trade": "📦",
    "chip": "🧠",
    "ai": "🤖",
    "energy": "⚡",
}

_INVALID_PERSPECTIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"无关"),
    re.compile(r"不相关"),
    re.compile(r"未提供.{0,8}(视角|信息|内容)"),
    re.compile(r"not related", re.IGNORECASE),
    re.compile(r"unrelated", re.IGNORECASE),
    re.compile(r"irrelevant", re.IGNORECASE),
)


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


def _broad_category(topic_category: str) -> str:
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


def _normalize_text_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _is_renderable_perspective(text: str) -> bool:
    normalized = _normalize_text_whitespace(text)
    if not normalized:
        return False
    return not any(pattern.search(normalized) for pattern in _INVALID_PERSPECTIVE_PATTERNS)


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
        template_name: str = "design-a",
        source_regions: dict[str, str] | None = None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.template_file = f"report-{template_name}.html"
        self.source_regions: dict[str, str] = source_regions or {}
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
            return _REGION_FLAG.get(search_region, "🌐")
        region = self.source_regions.get(source_name, "")
        return _REGION_FLAG.get(region, "")

    def _provenance_label(self, source_kind: str, platform: str | None, is_searched: bool) -> str | None:
        if source_kind == "official_web":
            return "官方网站"
        if source_kind == "official_social":
            platform_name = {"x": "官方X", "youtube": "官方YouTube"}.get(platform or "", "官方渠道")
            return platform_name
        if is_searched:
            return "搜索补充"
        return None

    def _article_meta(self, summary: ClusterSummary) -> dict[str, dict]:
        return {
            article.source_name: {
                "url": article.url,
                "is_searched": article.is_searched,
                "search_region": article.search_region,
                "source_kind": article.source_kind,
                "platform": article.platform,
                "is_official_source": article.is_official_source,
                "origin_region": article.origin_region,
                "searched_provider": article.searched_provider,
            }
            for article in summary.cluster.articles
        }

    def _build_source_entry(self, source_name: str, article_meta: dict[str, dict]) -> dict:
        meta = article_meta.get(source_name, {})
        is_searched = meta.get("is_searched", False)
        search_region = meta.get("search_region")
        source_kind = meta.get("source_kind", "news")
        platform = meta.get("platform")
        provenance_label = self._provenance_label(source_kind, platform, is_searched)
        compact_label = source_name
        if is_searched:
            compact_label = f"🔍{compact_label}"
        if provenance_label:
            compact_label = f"{compact_label} · {provenance_label}"
        return {
            "source": source_name,
            "flag": self._source_flag(source_name, search_region),
            "is_searched": is_searched,
            "search_region": search_region,
            "represented_region": meta.get("origin_region") or search_region,
            "source_kind": source_kind,
            "platform": platform,
            "is_official_source": meta.get("is_official_source", False),
            "searched_provider": meta.get("searched_provider"),
            "provenance_label": provenance_label,
            "url": meta.get("url", "#"),
            "compact_label": compact_label,
        }

    def _perspective_groups_data(self, summary: ClusterSummary) -> list[tuple[list[str], str]]:
        if summary.grouped_perspectives:
            return [(group.sources, group.perspective) for group in summary.grouped_perspectives]
        if summary.perspectives:
            return [([source_name], text) for source_name, text in summary.perspectives.items()]
        return []

    def _group_payload(self, source_entries: list[dict], perspective: str) -> dict:
        return {
            "label": " / ".join(entry["compact_label"] for entry in source_entries),
            "sources": source_entries,
            "perspective": perspective,
            "url": source_entries[0]["url"] if len(source_entries) == 1 else None,
            "source_count": len(source_entries),
            "is_grouped": len(source_entries) > 1,
        }

    def _build_footer_sources(self, summary: ClusterSummary, preferred_sources: list[str] | None = None) -> list[dict]:
        article_meta = self._article_meta(summary)
        ordered_sources = preferred_sources or summary.cluster.sources
        seen: set[str] = set()
        footer_sources: list[dict] = []
        for source_name in ordered_sources:
            if source_name in seen:
                continue
            seen.add(source_name)
            footer_sources.append(self._build_source_entry(source_name, article_meta))
        return footer_sources

    def _build_perspective_payload(self, summary: ClusterSummary) -> dict[str, object]:
        article_meta = self._article_meta(summary)
        group_definitions = self._perspective_groups_data(summary)
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
        suppressed_group_count = 0

        for sources, perspective in group_definitions:
            source_entries = [
                self._build_source_entry(source_name, article_meta)
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
            "grouped_perspectives": renderable_groups if summary.cluster.is_multi_source else [],
            "perspectives_list": perspectives_list if summary.cluster.is_multi_source else [],
            "source_groups": source_groups,
            "footer_sources": footer_sources,
            "rendered_perspectives": rendered_perspectives,
            "distinct_perspective_count": distinct_perspective_count if summary.cluster.is_multi_source else 0,
            "suppressed_group_count": suppressed_group_count,
            "has_expandable_perspectives": summary.cluster.is_multi_source and distinct_perspective_count >= 2,
            "perspective_preview": perspective_preview,
        }

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
    ) -> tuple[dict, dict]:
        articles_data = [
            {
                "title": article.title,
                "url": article.url,
                "source": article.source_name,
                "published_at": article.published_at.strftime("%H:%M"),
            }
            for article in summary.cluster.articles
        ]

        headline_raw = _extract_headline(summary.summary) or summary.cluster.topic_category
        body_text = _body_only(summary.summary)
        perspective_payload = self._build_perspective_payload(summary)
        source_groups = perspective_payload["source_groups"]
        footer_sources = perspective_payload["footer_sources"]
        grouped_perspectives = perspective_payload["grouped_perspectives"]
        perspectives_list = perspective_payload["perspectives_list"]
        broad = _broad_category(summary.cluster.topic_category)

        base = {
            "index": index,
            "topic": summary.cluster.topic_category,
            "broad_category": broad,
            "sources": summary.cluster.sources,
            "is_multi": summary.cluster.is_multi_source,
            "perspectives": perspective_payload["rendered_perspectives"],
            "grouped_perspectives": grouped_perspectives,
            "perspectives_list": perspectives_list,
            "source_groups": source_groups,
            "footer_sources": footer_sources,
            "distinct_perspective_count": perspective_payload["distinct_perspective_count"],
            "suppressed_group_count": perspective_payload["suppressed_group_count"],
            "perspective_preview": perspective_payload["perspective_preview"],
            "has_expandable_perspectives": perspective_payload["has_expandable_perspectives"],
            "articles": articles_data,
            "article_count": len(articles_data),
            "freshness_state": getattr(summary, "freshness_state", "new"),
            "is_developing": getattr(summary, "freshness_state", "new") == "developing",
            "storyline_key": getattr(summary, "storyline_key", None),
            "storyline_name": getattr(summary, "storyline_name", None),
            "storyline_role": getattr(summary, "storyline_role", "none"),
            "storyline_confidence": getattr(summary, "storyline_confidence", 0.0),
            "storyline_membership_status": getattr(summary, "storyline_membership_status", "none"),
            "storyline_anchor_labels": list(getattr(summary, "storyline_anchor_labels", [])),
            "storyline_display_mode": storyline_display_mode,
            "short_topic_name": getattr(summary, "short_topic_name", None),
            "topic_icon_key": getattr(summary, "topic_icon_key", None),
            "organic_unique_regions": getattr(summary, "organic_unique_regions", 0),
            "organic_unique_sources": getattr(summary, "organic_unique_sources", 0),
        }
        return (
            {
                **base,
                "headline": Markup(html_lib.escape(headline_raw)),
                "summary": _md_to_html(body_text),
            },
            {**base, "headline": headline_raw, "summary": summary.summary},
        )

    def render(
        self,
        summaries: list[ClusterSummary],
        report_date: date,
        hot_topics: list[dict[str, object]] | None = None,
        focus_storylines: list[dict[str, object]] | None = None,
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

        hot_topics = hot_topics or []
        focus_storylines = focus_storylines or []
        clusters_ctx = []
        clusters_json: list[dict] = []

        for i, cs in enumerate(summaries, 1):
            ctx_payload, json_payload = self._build_cluster_payload(cs, i, storyline_display_mode="main")
            clusters_ctx.append(ctx_payload)
            clusters_json.append(json_payload)

        present_categories = {cluster["broad_category"] for cluster in clusters_ctx}
        sections = []
        for label, emoji, css_key in _CATEGORY_META:
            if label in present_categories:
                sections.append({"label": label, "emoji": emoji, "css_key": css_key})

        hot_topics_ctx: list[dict] = []
        hot_topics_json: list[dict] = []
        for i, family in enumerate(hot_topics, 1):
            family_summaries = family.get("summaries", [])
            if not isinstance(family_summaries, list):
                continue
            topic_name = _normalize_hot_topic_name(
                family.get("macro_topic_name") if isinstance(family.get("macro_topic_name"), str) else None,
                family_summaries[0] if family_summaries else None,
            )
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
                )
                ctx_payload["hot_seq_index"] = member_index
                json_payload["hot_seq_index"] = member_index
                if ctx_payload["storyline_role"] == "core":
                    core_count += 1
                elif ctx_payload["storyline_role"] == "spillover":
                    spillover_count += 1
                member_ctx.append(ctx_payload)
                member_json.append(json_payload)

            scope_summary = f"聚焦 {core_count} 条核心事件，延伸 {spillover_count} 条直接外溢。"
            preview_clusters = [
                {
                    "headline": member["headline"],
                    "hot_seq_index": member["hot_seq_index"],
                }
                for member in member_json[:2]
            ]

            hot_topics_ctx.append(
                {
                    "dom_id": family.get("dom_id", f"hot-topic-{i}"),
                    "macro_topic_key": family.get("macro_topic_key", f"hot-topic-{i}"),
                    "macro_topic_name": topic_name,
                    "storyline_key": family.get("storyline_key", family.get("macro_topic_key", f"hot-topic-{i}")),
                    "storyline_name": family.get("storyline_name", topic_name),
                    "topic_icon_key": icon_key,
                    "topic_icon": _HOT_TOPIC_ICON_MAP.get(icon_key, _HOT_TOPIC_ICON_MAP["globe"]),
                    "anchor_labels": list(family.get("anchor_labels", [])),
                    "member_count": len(member_ctx),
                    "core_count": core_count,
                    "spillover_count": spillover_count,
                    "scope_summary": scope_summary,
                    "preview_clusters": preview_clusters,
                    "clusters": member_ctx,
                }
            )
            hot_topics_json.append(
                {
                    "dom_id": family.get("dom_id", f"hot-topic-{i}"),
                    "macro_topic_key": family.get("macro_topic_key", f"hot-topic-{i}"),
                    "macro_topic_name": topic_name,
                    "storyline_key": family.get("storyline_key", family.get("macro_topic_key", f"hot-topic-{i}")),
                    "storyline_name": family.get("storyline_name", topic_name),
                    "topic_icon_key": icon_key,
                    "anchor_labels": list(family.get("anchor_labels", [])),
                    "member_count": len(member_json),
                    "core_count": core_count,
                    "spillover_count": spillover_count,
                    "scope_summary": scope_summary,
                    "preview_clusters": preview_clusters,
                    "clusters": member_json,
                }
            )

        focus_storylines_ctx: list[dict] = []
        focus_storylines_json: list[dict] = []
        for i, family in enumerate(focus_storylines, 1):
            family_summaries = family.get("summaries", [])
            if not isinstance(family_summaries, list):
                continue
            storyline_name = _normalize_hot_topic_name(
                family.get("storyline_name") if isinstance(family.get("storyline_name"), str) else None,
                family_summaries[0] if family_summaries else None,
            )
            icon_key = family.get("topic_icon_key") if isinstance(family.get("topic_icon_key"), str) else "globe"
            if icon_key not in _HOT_TOPIC_ICON_MAP:
                icon_key = "globe"

            member_ctx: list[dict] = []
            member_json: list[dict] = []
            for member_index, summary in enumerate(family_summaries, 1):
                ctx_payload, json_payload = self._build_cluster_payload(
                    summary,
                    member_index,
                    storyline_display_mode="focus_storyline",
                )
                ctx_payload["focus_seq_index"] = member_index
                json_payload["focus_seq_index"] = member_index
                member_ctx.append(ctx_payload)
                member_json.append(json_payload)

            focus_storylines_ctx.append(
                {
                    "dom_id": family.get("dom_id", f"focus-storyline-{i}"),
                    "storyline_key": family.get("storyline_key", f"focus-storyline-{i}"),
                    "storyline_name": storyline_name,
                    "topic_icon_key": icon_key,
                    "topic_icon": _HOT_TOPIC_ICON_MAP.get(icon_key, _HOT_TOPIC_ICON_MAP["globe"]),
                    "member_count": len(member_ctx),
                    "clusters": member_ctx,
                }
            )
            focus_storylines_json.append(
                {
                    "dom_id": family.get("dom_id", f"focus-storyline-{i}"),
                    "storyline_key": family.get("storyline_key", f"focus-storyline-{i}"),
                    "storyline_name": storyline_name,
                    "topic_icon_key": icon_key,
                    "member_count": len(member_json),
                    "clusters": member_json,
                }
            )

        display_rank = 1
        for family_ctx, family_json in zip(focus_storylines_ctx, focus_storylines_json):
            for member_ctx, member_json in zip(family_ctx["clusters"], family_json["clusters"]):
                member_ctx["display_rank"] = display_rank
                member_ctx["seq_index"] = display_rank
                member_json["display_rank"] = display_rank
                member_json["seq_index"] = display_rank
                display_rank += 1
        for ctx_payload, json_payload in zip(clusters_ctx, clusters_json):
            ctx_payload["display_rank"] = display_rank
            ctx_payload["seq_index"] = display_rank
            json_payload["display_rank"] = display_rank
            json_payload["seq_index"] = display_rank
            display_rank += 1

        focus_storyline_story_count = sum(len(family["clusters"]) for family in focus_storylines_ctx)
        hot_topic_story_count = sum(len(family["clusters"]) for family in hot_topics_ctx)

        common = {
            "report_date": date_str,
            "report_date_display": report_date.strftime("%Y年%m月%d日"),
            "day_name": ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][
                report_date.weekday()
            ],
            "cluster_count": len(summaries),
            "focus_storyline_count": len(focus_storylines_ctx),
            "focus_storyline_story_count": focus_storyline_story_count,
            "hot_topic_count": len(hot_topics_ctx),
            "hot_topic_story_count": hot_topic_story_count,
            "total_cluster_count": len(summaries) + focus_storyline_story_count + hot_topic_story_count,
        }

        template = self.env.get_template(self.template_file)
        page_html = template.render(
            **common,
            clusters=clusters_ctx,
            sections=sections,
            main_sections=sections,
            focus_storylines=focus_storylines_ctx,
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
                    "focus_storylines": focus_storylines_json,
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
        latest = self.output_dir / "latest"
        if update_latest and total_story_count > 0:
            if latest.is_symlink():
                latest.unlink()
            try:
                latest.symlink_to(date_str)
            except OSError:
                pass
        elif update_latest:
            logger.info(
                "HTML report has zero stories for %s — preserving existing latest symlink",
                date_str,
            )

        logger.info("HTML report written: %s", html_path)
        return html_path
