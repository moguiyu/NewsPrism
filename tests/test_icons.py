from __future__ import annotations

import re

import pytest
from markupsafe import Markup

from newsprism.runtime import icons

EMOJI_PRESENTATION = re.compile(
    "[\U0001F000-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF]\uFE0F?"
)


def test_sprite_file_is_committed_and_self_contained():
    text = icons.SPRITE_PATH.read_text(encoding="utf-8")
    assert text.startswith('<svg xmlns="http://www.w3.org/2000/svg"')
    # one <symbol> per slug: two slugs share Globe02Icon, so count slugs not names
    assert text.count("<symbol ") == len(icons.ICON_SLUGS)
    assert 'viewBox="0 0 24 24"' in text
    assert text.count('fill="none"') == text.count("<symbol ")
    # provenance header keeps the vendored geometry auditable
    assert "Hugeicons" in text and "MIT" in text and "4.3.2" in text


def test_sprite_has_no_colour_emoji_codepoints():
    text = icons.SPRITE_PATH.read_text(encoding="utf-8")
    assert not EMOJI_PRESENTATION.search(text)


def test_every_node_keeps_current_color_stroke():
    """Guards the defect that made the whole sprite invisible: SVG defaults stroke
    to none, so a vendored node without stroke="currentColor" paints nothing.
    Element geometry cannot catch this — bboxes stay identical (verified in a
    browser: broken and fixed symbols both measure 15 x 13.5 px)."""
    text = icons.SPRITE_PATH.read_text(encoding="utf-8")
    nodes = text.count("/>")
    assert nodes > 0
    assert text.count('stroke="currentColor"') == nodes
    assert "stroke=\"none\"" not in text


def test_every_slug_has_a_symbol():
    text = icons.SPRITE_PATH.read_text(encoding="utf-8")
    for slug in icons.ICON_SLUGS:
        assert f'<symbol id="hi-{slug}"' in text


def test_icon_returns_use_element():
    markup = icons.icon("category-world")
    assert isinstance(markup, Markup)
    assert (
        str(markup)
        == '<svg class="icon" aria-hidden="true"><use href="#hi-category-world"/></svg>'
    )


def test_icon_extra_class_and_unknown_slug():
    assert 'class="icon icon--sm"' in str(icons.icon("ui-certified", extra_class="icon--sm"))
    with pytest.raises(KeyError):
        icons.icon("not-a-slug")


def test_sprite_markup_is_markup_and_contains_every_symbol():
    markup = icons.sprite_markup()
    assert isinstance(markup, Markup)
    assert markup.count("<symbol ") == len(icons.ICON_SLUGS)
