"""Reader-facing evidence counts: real sources, and the region fallback.

2026-10-22: 11 of 15 shipped cards said "0 个地区" because the summarizer read
``article.origin_region`` only, and organically collected articles never get that
field. The impact assessor already fell back to the configured source region;
the reader-facing line did not.
"""
from datetime import datetime, timezone

from newsprism.service.evidence import (
    article_region,
    evidence_counts,
    evidence_summary_text,
    real_articles,
    real_source_names,
)
from newsprism.types import Article, ArticleCluster


def _article(source: str, region: str | None = None, url: str | None = None) -> Article:
    return Article(
        url=url or f"https://example.com/{source}",
        title=f"{source} report",
        source_name=source,
        published_at=datetime.now(tz=timezone.utc),
        content="body",
        origin_region=region,
    )


def test_article_region_prefers_origin_region():
    article = _article("Reuters", region="us")
    assert article_region(article, {"Reuters": "gb"}) == "us"


def test_article_region_falls_back_to_configured_source_region():
    article = _article("Le Monde")
    assert article_region(article, {"Le Monde": "fr"}) == "fr"


def test_article_region_returns_none_without_either():
    assert article_region(_article("Unknown Wire"), {"Reuters": "us"}) is None


def test_evidence_counts_uses_source_region_when_articles_lack_it():
    """The regression: no ``origin_region`` on any article must not mean 0."""
    cluster = ArticleCluster(
        topic_category="France riots",
        articles=[_article("Le Monde"), _article("franceinfo"), _article("BBC News", region="gb")],
    )

    sources, regions = evidence_counts(cluster, {"Le Monde": "fr", "franceinfo": "fr"})

    assert sources == 3
    assert regions == 2


def test_evidence_counts_deduplicates_regions():
    cluster = ArticleCluster(
        topic_category="US politics",
        articles=[_article("Reuters", region="us"), _article("AP News", region="us")],
    )
    assert evidence_counts(cluster) == (2, 1)


def test_evidence_counts_ignores_placeholder_articles():
    cluster = ArticleCluster(
        topic_category="Ukraine aid",
        articles=[
            _article("Reuters", region="us"),
            Article(
                url="placeholder:pl:poland-aid",
                title="待补充：Poland声音",
                source_name="[Poland声音待补]",
                published_at=datetime.now(tz=timezone.utc),
                content="",
                is_placeholder=True,
                origin_region="pl",
            ),
        ],
    )

    sources, regions = evidence_counts(cluster, {"Poland": "pl"})

    assert sources == 1
    assert regions == 1
    assert len(real_articles(cluster)) == 1
    assert real_source_names(cluster) == {"Reuters"}


def test_evidence_summary_text_matches_reader_format():
    cluster = ArticleCluster(
        topic_category="French unrest",
        articles=[_article("Le Monde"), _article("franceinfo")],
    )

    assert (
        evidence_summary_text(cluster, {"Le Monde": "fr", "franceinfo": "fr"})
        == "2 个来源、1 个地区参与评估。"
    )
