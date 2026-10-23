"""Small language-shape checks used by service and runtime layers."""
from __future__ import annotations

import re


_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_LATIN_RE = re.compile(r"[A-Za-z]")

# Sentinels the numeric-grounding fallback writes when it has no grounded text
# left. They are non-empty, so a "field is populated" check accepts them, but
# they carry no story: on 2026-10-23 five of eighteen English headlines shipped
# as ``Related report: specific figures remain unverified``.
PLACEHOLDER_OUTPUT_MARKERS: tuple[str, ...] = (
    "related report: specific figures remain unverified",
    "the report describes the development, while specific figures remain unverified",
    "相关报道：具体数字有待进一步核实",
    "报道披露了相关进展，但具体数字仍待进一步核实",
)


def is_placeholder_output(text: str | None) -> bool:
    """True when text is *only* a numeric-fallback sentinel, not real content.

    Exact match after normalisation: a real headline that merely mentions
    unverified figures keeps its own wording and must not be caught here.
    """
    normalized = re.sub(r"\s+", " ", (text or "")).strip(" *").casefold().rstrip("。.")
    if not normalized:
        return False
    return any(normalized == marker for marker in PLACEHOLDER_OUTPUT_MARKERS)



def cjk_char_count(text: str) -> int:
    return len(_CJK_RE.findall(text or ""))


def looks_like_chinese_text(text: str, *, min_cjk: int = 4, min_ratio: float = 0.08) -> bool:
    """Return true when text has enough Chinese signal for the primary zh fields."""
    value = re.sub(r"\s+", "", text or "")
    if not value:
        return False
    cjk_count = cjk_char_count(value)
    if cjk_count < min_cjk:
        return False
    latin_count = len(_LATIN_RE.findall(value))
    signal_count = cjk_count + latin_count
    if signal_count == 0:
        return False
    return (cjk_count / signal_count) >= min_ratio


_JAPANESE_KANA_RE = re.compile(r"[\u3040-\u30ff]")
_CYRILLIC_RE = re.compile(r"[\u0400-\u04ff]")
_HANGUL_RE = re.compile(r"[\u1100-\u11ff\u3130-\u318f\uac00-\ud7af]")


def looks_like_chinese_label(text: str, *, min_cjk: int = 2) -> bool:
    """Stricter than looks_like_chinese_text: reject any foreign script present.

    looks_like_chinese_text only weighs CJK against Latin letters, so a Japanese
    headline full of kana passes it. Labels derived from a headline need the
    stronger test.
    """
    value = (text or "").strip()
    if not value:
        return False
    if (
        _JAPANESE_KANA_RE.search(value)
        or _CYRILLIC_RE.search(value)
        or _HANGUL_RE.search(value)
    ):
        return False
    return looks_like_chinese_text(value, min_cjk=min_cjk)
