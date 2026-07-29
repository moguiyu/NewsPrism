"""Deterministic cleanup for model-generated perspective groups."""
from __future__ import annotations

import re
import unicodedata

from rapidfuzz import fuzz

from newsprism.types import PerspectiveGroup


_INVALID_PATTERNS = (
    re.compile(r"未稳定提炼出可单列的差异化视角"),
    re.compile(r"未提供.{0,8}(?:视角|信息|内容)"),
    re.compile(r"no distinct perspective could be extracted", re.IGNORECASE),
    re.compile(r"reports? a similar angle to the main summary", re.IGNORECASE),
)


def _normalized_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", value).casefold()
    text = re.sub(r"(?:该来源|媒体|报道|共同|主要|聚焦|关注|强调)", "", text)
    return "".join(char for char in text if char.isalnum())


def _ngrams(value: str, size: int = 3) -> set[str]:
    if len(value) <= size:
        return {value} if value else set()
    return {value[index:index + size] for index in range(len(value) - size + 1)}


def perspectives_are_equivalent(left: str, right: str) -> bool:
    left_norm = _normalized_text(left)
    right_norm = _normalized_text(right)
    if not left_norm or not right_norm:
        return False
    if left_norm == right_norm:
        return True
    ratio = fuzz.ratio(left_norm, right_norm) / 100.0
    left_grams, right_grams = _ngrams(left_norm), _ngrams(right_norm)
    union = left_grams | right_grams
    jaccard = len(left_grams & right_grams) / len(union) if union else 0.0
    return bool(
        ratio >= 0.84
        or (ratio >= 0.76 and len(left_grams & right_grams) >= 4)
        or (ratio >= 0.72 and jaccard >= 0.48)
    )


def is_distinct_perspective(text: str) -> bool:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    return bool(cleaned and not any(pattern.search(cleaned) for pattern in _INVALID_PATTERNS))


def canonicalize_perspective_groups(
    groups: list[PerspectiveGroup],
) -> list[PerspectiveGroup]:
    """Drop confirmation prose and merge semantically equivalent groups."""
    canonical: list[PerspectiveGroup] = []
    assigned_sources: set[str] = set()
    for group in groups:
        perspective = re.sub(r"\s+", " ", (group.perspective or "").strip())
        sources = [
            source
            for source in dict.fromkeys(source.strip() for source in group.sources if source.strip())
            if source not in assigned_sources
        ]
        if not sources or not is_distinct_perspective(perspective):
            continue
        equivalent = next(
            (
                existing
                for existing in canonical
                if perspectives_are_equivalent(existing.perspective, perspective)
            ),
            None,
        )
        if equivalent is None:
            canonical.append(PerspectiveGroup(sources=list(sources), perspective=perspective))
        else:
            equivalent.sources.extend(source for source in sources if source not in equivalent.sources)
        assigned_sources.update(sources)
    return canonical
