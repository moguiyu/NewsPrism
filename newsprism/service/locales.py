"""Country and query-locale metadata for Active Seeker.

This metadata improves query construction only. It must never be used to infer
publisher ownership, source origin, or a candidate's represented country.
"""
from __future__ import annotations

import re
import unicodedata

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


def is_recognized_country(region: str) -> bool:
    """Whether ``region`` is a CLDR-recognized ISO territory code."""
    code = (region or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{2}", code) and code in Locale("en").territories)


def region_flag(region: str) -> str:
    """Return a flag for any recognized ISO alpha-2 territory."""
    code = (region or "").strip().upper()
    if not is_recognized_country(code):
        return ""
    return "".join(chr(0x1F1E6 + ord(char) - ord("A")) for char in code)


def region_flags() -> dict[str, str]:
    """Compatibility mapping for renderers/tests; generated from CLDR."""
    return {
        str(code).lower(): region_flag(str(code))
        for code in Locale("en").territories
        if is_recognized_country(str(code))
    }


def _identity_text(value: str) -> str:
    """Normalize a territory/entity label for equality comparisons."""
    return "".join(
        char for char in unicodedata.normalize("NFKC", value).casefold() if char.isalnum()
    )


def is_territory_name(value: str, languages: tuple[str, ...] = ()) -> bool:
    """Return whether a label is a CLDR territory name in a relevant locale.

    This is a guard against treating ``France`` (or a localized alias) as a
    named actor.  It is not publisher provenance data.
    """
    normalized = _identity_text(value)
    if not normalized:
        return False
    locale_codes = ("en", *languages)
    for code in dict.fromkeys(locale_codes):
        try:
            territory_names = Locale(code).territories.values()
        except Exception:
            continue
        if normalized in {_identity_text(name) for name in territory_names}:
            return True
    return False


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
