"""Country and query-locale metadata for Active Seeker.

This metadata improves query construction only. It must never be used to infer
publisher ownership, source origin, or a candidate's represented country.
"""
from __future__ import annotations

from babel import Locale
from babel.core import get_global

# Query more than one language only where both are materially useful. The
# seeker still caps variants, so this does not create an unbounded search fanout.
MULTILINGUAL_NEWS_LANGUAGES: dict[str, tuple[str, ...]] = {
    "be": ("nl", "fr"), "ca": ("en", "fr"), "ch": ("de", "fr"),
    "cy": ("el", "tr"), "lu": ("fr", "de"), "za": ("en", "af"),
}


def country_name(region: str) -> str:
    """Return an ISO country name, preserving the code for unknown territories."""
    code = (region or "").strip().upper()
    return Locale("en").territories.get(code, code or "unknown country")


def query_languages(region: str, override: str | list[str] | None = None) -> tuple[str, ...]:
    """Return CLDR official language defaults, with an editorial override."""
    if isinstance(override, str) and override.strip():
        return (override.strip().lower(),)
    if isinstance(override, list):
        values = tuple(str(value).strip().lower() for value in override if str(value).strip())
        if values:
            return values
    code = (region or "").strip().upper()
    if code.lower() in MULTILINGUAL_NEWS_LANGUAGES:
        return MULTILINGUAL_NEWS_LANGUAGES[code.lower()]
    languages = get_global("territory_languages").get(code, {})
    ranked = sorted(
        (
            (str(language).split("_")[0].split("-")[0], float(info.get("population_percent") or 0))
            for language, info in languages.items()
            if info.get("official_status") in {"official", "de_facto_official"}
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    return (ranked[0][0],) if ranked else ()


def language_name(language: str) -> str:
    try:
        return Locale(language).get_display_name("en")
    except Exception:
        return language
