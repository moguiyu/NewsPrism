from datetime import datetime, timezone

from newsprism.runtime.scheduler import (
    _is_real_article,
    _summary_publication_rejection,
)
from newsprism.types import Article, ArticleCluster, ClusterSummary


def _summary(
    *,
    url: str = "https://example.com/event",
    is_placeholder: bool = False,
    quality_status: str = "publishable",
    quality_flags: list[str] | None = None,
    summary: str = "**Confirmed event**\n\nSeveral sources describe the development and its public consequences.",
) -> ClusterSummary:
    article = Article(
        url=url,
        title="Event",
        source_name="Example News",
        published_at=datetime.now(tz=timezone.utc),
        content="Several sources describe the development and its public consequences.",
        is_placeholder=is_placeholder,
    )
    return ClusterSummary(
        cluster=ArticleCluster(topic_category="World News", articles=[article]),
        summary=summary,
        quality_status=quality_status,
        quality_flags=list(quality_flags or []),
    )


def test_scheduler_real_article_gate_uses_placeholder_url_as_source_of_truth():
    article = _summary(url="placeholder:ua:cluster", is_placeholder=False).cluster.articles[0]

    assert _is_real_article(article) is False
    assert _summary_publication_rejection(
        _summary(url="placeholder:ua:cluster", is_placeholder=False)
    )


def test_scheduler_withholds_review_and_malformed_numeric_summaries():
    assert _summary_publication_rejection(
        _summary(quality_status="needs_review", quality_flags=["unsupported_numeric_claim"])
    )
    assert _summary_publication_rejection(
        _summary(summary="**Event**\n\n70,000.")
    )
