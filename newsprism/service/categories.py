"""Public report category vocabulary and legacy normalization."""

from __future__ import annotations

DISPLAY_CATEGORIES = (
    "World",
    "Business",
    "Technology",
    "Science & Health",
    "Society",
    "Culture & Sports",
)

DEFAULT_DISPLAY_CATEGORY = "World"

DISPLAY_CATEGORY_LABELS_ZH = {
    "World": "国际",
    "Business": "商业",
    "Technology": "科技",
    "Science & Health": "科学健康",
    "Society": "社会",
    "Culture & Sports": "文化体育",
}

LEGACY_DISPLAY_CATEGORY_MAP = {
    "国际时政": "World",
    "商业财经": "Business",
    "科技创新": "Technology",
    "科学健康": "Science & Health",
    "社会民生": "Society",
    "文化艺术": "Culture & Sports",
    "体育运动": "Culture & Sports",
}


def normalize_display_category(value: str | None) -> str:
    category = (value or "").strip()
    if category in DISPLAY_CATEGORIES:
        return category
    return LEGACY_DISPLAY_CATEGORY_MAP.get(category, DEFAULT_DISPLAY_CATEGORY)


def display_category_label_zh(value: str | None) -> str:
    category = normalize_display_category(value)
    return DISPLAY_CATEGORY_LABELS_ZH.get(category, category)
