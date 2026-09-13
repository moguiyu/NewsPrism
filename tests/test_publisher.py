"""Tests for the Telegram publisher's chat-surface formatting."""
from __future__ import annotations


def test_every_category_has_a_telegram_emoji():
    from newsprism.runtime.publisher import _POSITIVE_CATEGORY, _TELEGRAM_CATEGORY_EMOJI
    from newsprism.runtime.renderer import _CATEGORY_META

    for label, _slug, _css in _CATEGORY_META:
        assert _TELEGRAM_CATEGORY_EMOJI[label], label
    assert _TELEGRAM_CATEGORY_EMOJI[_POSITIVE_CATEGORY] == "🌟"
    # the digest must never leak a sprite slug into chat text
    assert not any("category-" in v for v in _TELEGRAM_CATEGORY_EMOJI.values())
