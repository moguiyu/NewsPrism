"""Small language-shape checks used by service and runtime layers."""
from __future__ import annotations

import re


_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_LATIN_RE = re.compile(r"[A-Za-z]")


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
