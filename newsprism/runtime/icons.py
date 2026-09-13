"""Hugeicons SVG sprite access for rendered reports.

Icons are the free Hugeicons stroke-rounded set (MIT), vendored by
scripts/vendor_hugeicons.py into newsprism/static/icons/hugeicons-sprite.svg.
The sprite is inlined once per page so reports stay self-contained and need no
third-party request, icon font, or network access at render time.
"""
from __future__ import annotations

from pathlib import Path

from markupsafe import Markup

SPRITE_PATH = (
    Path(__file__).resolve().parent.parent / "static" / "icons" / "hugeicons-sprite.svg"
)

# Semantic slug -> Hugeicons module. Kept in sync with the vendoring script by
# tests/test_icons.py (every slug must appear as a <symbol> in the sprite).
ICON_SLUGS: dict[str, str] = {
    "category-world": "Globe02Icon",
    "category-business": "TradeUpIcon",
    "category-technology": "CpuIcon",
    "category-science-health": "Telescope02Icon",
    "category-society": "CourtHouseIcon",
    "category-culture-sports": "TheaterIcon",
    "topic-globe": "Globe02Icon",
    "topic-war": "Alert02Icon",
    "topic-trade": "PackageIcon",
    "topic-chip": "AiChipIcon",
    "topic-ai": "Robot01Icon",
    "topic-energy": "ZapIcon",
    "ui-good-news": "SparklesIcon",
    "ui-searched": "Search01Icon",
    "ui-certified": "ShieldCheckIcon",
    "ui-theme-system": "MonitorIcon",
    "ui-theme-light": "Sun02Icon",
    "ui-theme-dark": "Moon02Icon",
    "ui-arrow-left": "ArrowLeft01Icon",
    "ui-arrow-right": "ArrowRight01Icon",
    "ui-arrow-down": "ArrowDown01Icon",
    "ui-feedback-up": "ThumbsUpIcon",
    "ui-feedback-down": "ThumbsDownIcon",
    "ui-selected": "CheckmarkCircle01Icon",
}


def sprite_markup() -> Markup:
    """Return the inline sprite for one page. Raises if the asset is missing."""
    return Markup(SPRITE_PATH.read_text(encoding="utf-8"))


def icon(slug: str, *, extra_class: str = "") -> Markup:
    """Return an inline <svg><use/></svg> for a semantic slug.

    Decorative by design: the adjacent label carries the meaning, matching the
    aria-hidden emoji spans this replaces.
    """
    if slug not in ICON_SLUGS:
        raise KeyError(f"unknown icon slug: {slug!r}")
    css = f"icon {extra_class}".strip()
    return Markup(
        f'<svg class="{css}" aria-hidden="true"><use href="#hi-{slug}"/></svg>'
    )
