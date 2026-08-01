"""Pure publication-contract checks for summarized stories.

The scheduler/editorial boundary can call this module before selecting or
rendering cards.  It intentionally does not mutate summaries, consult the
database, or perform network I/O.  A future human-approved path may waive a
review status, but it may not waive malformed text or a missing real source.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from newsprism.types import ClusterSummary, is_real_article


@dataclass(frozen=True)
class PublicationIssue:
    """One deterministic reason a summary must stay out of publication."""

    summary_index: int
    code: str
    message: str
    topic: str = ""


_NUMERIC_PLACEHOLDER_PATTERN = re.compile(
    r"有关数字|\bcertain\s+number\b",
    re.IGNORECASE,
)
_ORPHAN_NUMERIC_SENTENCE_PATTERN = re.compile(
    r"^\s*(?:[$€£¥￥]\s*)?\d[\d,]*(?:\.\d+)?"
    r"(?:\s*(?:千|천|万|만|亿|억|兆|조)(?:\s*\d[\d,]*)?)?"
    r"(?:\s*(?:%|％|万亿美元|亿美元|亿元|万元|美元|欧元|人民币|"
    r"票|项|例|病例|人|名|家|国|枚|架|艘|倍|岁|年|月|日|건|명|"
    r"dead|deaths?|people|cases|injured?))?"
    r"\s*[.,。!?！？]\s*$",
    re.IGNORECASE,
)
_LEADING_NUMERIC_FRAGMENT_PATTERN = re.compile(
    r"^\s*(?:[$€£¥￥]\s*)?\d[\d,]*(?:\.\d+)?"
    r"(?:\s*(?:千|천|万|만|亿|억|兆|조)(?:\s*\d[\d,]*)?)?"
    r"(?:\s*(?:%|％|万亿美元|亿美元|亿元|万元|美元|欧元|人民币|"
    r"票|项|例|病例|人|名|家|国|枚|架|艘|倍|岁|年|月|日|건|명|"
    r"dead|deaths?|people|cases|injured?))?"
    r"\s*[.,。!?！？]",
    re.IGNORECASE,
)
_MALFORMED_NUMERIC_REMNANT_PATTERN = re.compile(
    r"(?:超|超过|约|近|达|至少|至多|致|导致|造成)\s*"
    r"(?:例|病例|人|名|死亡|受伤|%|％|[，,。.!！？?])"
    r"|\b(?:kills?|killed|dead|deaths?|injured?)\s*[,.;:]",
    re.IGNORECASE,
)


def _body_only(text: str) -> str:
    lines = (text or "").splitlines()
    if lines and re.match(r"\*\*(.+?)\*\*", lines[0].strip()):
        lines = lines[1:]
    return "\n".join(lines).strip()


def _split_sentences(text: str) -> list[str]:
    return [
        sentence.strip()
        for sentence in re.split(r"(?<=[。！？])\s*|(?<=[.!?])(?:\s+|$)", text or "")
        if sentence.strip()
    ]


def _text_issues(text: str, language: str) -> list[tuple[str, str]]:
    if not text or not text.strip():
        return [("empty_summary", f"{language} summary is empty")]

    body = _body_only(text)
    issues: list[tuple[str, str]] = []
    if len(re.sub(r"\s+", "", body)) < 20:
        issues.append(("summary_body_too_short", f"{language} summary body is too short"))

    if _NUMERIC_PLACEHOLDER_PATTERN.search(text):
        issues.append(("numeric_placeholder", f"{language} summary contains a numeric placeholder"))
    if _LEADING_NUMERIC_FRAGMENT_PATTERN.search(body):
        issues.append(("leading_numeric_fragment", f"{language} summary starts with a numeric fragment"))
    if any(_ORPHAN_NUMERIC_SENTENCE_PATTERN.fullmatch(sentence) for sentence in _split_sentences(body)):
        issues.append(("orphan_numeric_fragment", f"{language} summary contains an orphan numeric sentence"))
    if re.match(r"^\s*[，,、。.!！？?]", body):
        issues.append(("malformed_sentence_start", f"{language} summary starts with punctuation"))
    if _MALFORMED_NUMERIC_REMNANT_PATTERN.search(text):
        issues.append(("malformed_numeric_remnant", f"{language} summary contains a malformed numeric remnant"))
    return issues


def _is_real_article(article: object) -> bool:
    return is_real_article(article)  # type: ignore[arg-type]


def _article_issues(summary: ClusterSummary) -> list[tuple[str, str]]:
    issues: list[tuple[str, str]] = []
    articles = list(getattr(summary.cluster, "articles", []) or [])
    for article in articles:
        url = str(getattr(article, "url", "") or "")
        if url.casefold().startswith("placeholder:") and not bool(getattr(article, "is_placeholder", False)):
            issues.append(("placeholder_url_mismatch", "placeholder URL is not marked as a placeholder"))

        if (
            bool(getattr(article, "is_searched", False))
            and getattr(article, "search_acceptance_status", None) == "accepted"
            and not getattr(article, "result_freshness_state", None)
            and getattr(article, "search_acceptance_reason", None) != "current_event_perspective"
        ):
            issues.append(
                (
                    "search_evidence_unverified",
                    "accepted searched article has no freshness/materiality evidence",
                )
            )
    if not any(_is_real_article(article) for article in articles):
        issues.append(("no_real_articles", "summary has no non-placeholder HTTP(S) article"))
    return issues


def publication_issues(
    summary: ClusterSummary,
    *,
    summary_index: int = 0,
    human_approved: bool = False,
) -> list[PublicationIssue]:
    """Return publication issues for one summary without changing it."""
    topic = str(getattr(summary.cluster, "topic_category", "") or "")
    issues: list[PublicationIssue] = []

    for code, message in _article_issues(summary):
        issues.append(PublicationIssue(summary_index, code, message, topic))

    if not human_approved:
        quality_status = str(getattr(summary, "quality_status", "unknown") or "unknown")
        if quality_status != "publishable":
            issues.append(
                PublicationIssue(
                    summary_index,
                    "quality_status_not_publishable",
                    f"quality status is {quality_status}",
                    topic,
                )
            )
        quality_flags = set(getattr(summary, "quality_flags", []) or [])
        if "unsupported_numeric_claim" in quality_flags:
            issues.append(
                PublicationIssue(
                    summary_index,
                    "unsupported_numeric_claim",
                    "summary contains an unsupported numeric claim",
                    topic,
                )
            )

    for language, text in (("Chinese", getattr(summary, "summary", "")), ("English", getattr(summary, "summary_en", None))):
        if not text:
            continue
        for code, message in _text_issues(str(text), language):
            issues.append(PublicationIssue(summary_index, code, message, topic))
    return issues


def validate_publication_contract(
    summaries: Iterable[ClusterSummary],
    *,
    human_approval: Callable[[ClusterSummary], bool] | None = None,
) -> list[PublicationIssue]:
    """Validate summaries in order and return auditable, stable issue records.

    ``human_approval`` is intentionally explicit and per-summary.  It can
    waive review status for a future operator workflow, but structural source
    and text-safety failures remain hard blockers.
    """
    issues: list[PublicationIssue] = []
    for index, summary in enumerate(summaries):
        approved = bool(human_approval(summary)) if human_approval else False
        issues.extend(
            publication_issues(
                summary,
                summary_index=index,
                human_approved=approved,
            )
        )
    return issues


def is_publication_safe(
    summary: ClusterSummary,
    *,
    human_approved: bool = False,
) -> bool:
    """Convenience predicate for a future selection/publish integration."""
    return not publication_issues(summary, human_approved=human_approved)
