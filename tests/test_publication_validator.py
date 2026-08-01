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
