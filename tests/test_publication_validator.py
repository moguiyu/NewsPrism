from datetime import datetime, timezone

from newsprism.runtime.publication_validator import (
    is_publication_safe,
    validate_publication_contract,
)
from newsprism.types import Article, ArticleCluster, ClusterSummary


def _summary(
    *,
    url: str = "https://example.com/story",
    summary: str = "**A confirmed event**\n\nSeveral sources describe the development and its immediate consequences for the public.",
    summary_en: str | None = None,
    quality_status: str = "publishable",
    quality_flags: list[str] | None = None,
    is_placeholder: bool = False,
    is_searched: bool = False,
    search_acceptance_status: str | None = None,
) -> ClusterSummary:
    article = Article(
        url=url,
        title="A confirmed event",
        source_name="Example News",
        published_at=datetime.now(tz=timezone.utc),
        content="Several sources describe the development and its immediate consequences for the public.",
        is_placeholder=is_placeholder,
        is_searched=is_searched,
        search_acceptance_status=search_acceptance_status,
    )
    cluster = ArticleCluster(topic_category="World News", articles=[article])
    return ClusterSummary(
        cluster=cluster,
        summary=summary,
        summary_en=summary_en,
        quality_status=quality_status,
        quality_flags=list(quality_flags or []),
    )


def test_publication_validator_accepts_real_publishable_summary():
    summary = _summary()

    assert validate_publication_contract([summary]) == []
    assert is_publication_safe(summary) is True


def test_publication_validator_rejects_placeholder_only_and_malformed_numeric_output():
    summary = _summary(
        url="placeholder:ua:cluster-1",
        summary="**俄罗斯袭击基辅致有关数字死亡**\n\n1%。",
        is_placeholder=False,
    )

    issues = validate_publication_contract([summary])
    codes = {issue.code for issue in issues}

    assert "placeholder_url_mismatch" in codes
    assert "no_real_articles" in codes
    assert "numeric_placeholder" in codes
    assert "orphan_numeric_fragment" in codes
    assert is_publication_safe(summary) is False


def test_publication_validator_blocks_review_and_unverified_search_rows():
    summary = _summary(
        quality_status="needs_review",
        quality_flags=["unsupported_numeric_claim"],
        is_searched=True,
        search_acceptance_status="accepted",
    )

    codes = {issue.code for issue in validate_publication_contract([summary])}

    assert "quality_status_not_publishable" in codes
    assert "unsupported_numeric_claim" in codes
    assert "search_evidence_unverified" in codes


def test_explicit_human_approval_only_waives_review_status():
    summary = _summary(
        quality_status="needs_review",
        quality_flags=["unsupported_numeric_claim"],
    )

    assert validate_publication_contract([summary], human_approval=lambda _summary: True) == []

    malformed = _summary(
        quality_status="needs_review",
        summary="**A story**\n\n70,000.",
    )
    codes = {
        issue.code
        for issue in validate_publication_contract([malformed], human_approval=lambda _summary: True)
    }
    assert "orphan_numeric_fragment" in codes


def test_english_text_checks_run_by_default():
    """``include_english`` defaults to True, so every pre-existing caller keeps
    validating the translated field.

    The trigger is a genuine stripped-object remnant, not an ordinary casualty
    sentence: 2026-10-24 showed that flagging ``soldiers were killed,`` erased a
    correct English card (8484) and rendered it in Chinese on the English
    edition. See ``test_ordinary_casualty_sentences_are_not_malformed_remnants``.
    """
    summary = _summary(
        summary_en="**A confirmed event**\n\nHouthi attack on Saudi airport kills, group says."
    )

    codes = {issue.code for issue in validate_publication_contract([summary])}

    assert "malformed_numeric_remnant" in codes


def test_ordinary_casualty_sentences_are_not_malformed_remnants():
    """Passive and adjectival casualty phrasing is complete English.

    2026-10-24: card 8484 shipped as Chinese on the English edition because
    ``Several people were killed, according to local officials.`` was read as a
    stripped-figure remnant and the fallback erased headline and body.
    """
    for sentence in (
        "Several people were killed, according to local officials.",
        "At least 16 people are dead, and 76 were injured, state media reported.",
        "Two soldiers have been killed, the military said.",
    ):
        summary = _summary(summary_en=f"**A confirmed event**\n\n{sentence}")

        codes = {issue.code for issue in validate_publication_contract([summary])}

        assert "malformed_numeric_remnant" not in codes, sentence


def test_validator_and_summarizer_agree_on_the_remnant_pattern():
    """The pattern is duplicated across layers; the copies must not drift.

    ``runtime.publication_validator`` cannot import ``service.summarizer``
    without inverting the dependency order, so both carry the regex. A silent
    divergence here would mean a card validates cleanly and then fails the
    summarizer gate (or vice versa).
    """
    from newsprism.runtime import publication_validator
    from newsprism.service.summarizer import Summarizer

    samples = [
        "Houthi attack on Saudi airport kills, group says.",
        "The blast injures, officials said.",
        "Two Israeli soldiers were killed, and the attackers were shot dead.",
        "At least 16 people are dead, and 76 were injured, state media reported.",
        "该事件造成3人死亡，另有10人受伤。",
        "袭击致2名士兵死亡。",
    ]
    for sample in samples:
        assert bool(publication_validator._MALFORMED_NUMERIC_REMNANT_PATTERN.search(sample)) == bool(
            Summarizer._MALFORMED_NUMERIC_REMNANT_PATTERN.search(sample)
        ), sample


def test_include_english_false_still_validates_the_chinese_text():
    """Opting out of the English field must not weaken the Chinese checks."""
    summary = _summary(summary="**A story**\n\n70,000.", summary_en=None)

    codes = {
        issue.code
        for issue in validate_publication_contract([summary], include_english=False)
    }

    assert "orphan_numeric_fragment" in codes
