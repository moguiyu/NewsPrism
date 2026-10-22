"""Reader-facing evidence counts for one cluster.

The "N 个来源、M 个地区参与评估" line is built by the summarizer when a cluster is
summarized and then recounted by the editorial planner after a display merge. It
is user-visible text, so both producers must count the same way: once the two
implementations drifted apart, merged cards shipped lines like "6 个来源" above a
four-source list (2026-10-21, cluster 8405).

Regions used to be read from ``article.origin_region`` alone. Only searched
articles ever get that field, so every organically collected card reported a
region count of zero (2026-10-22: 11 of 15 cards said "0 个地区"). The impact
assessor and the renderer already fall back to the configured source region;
this module is that same lookup, shared.

Layer: service (imports types only).
"""
from __future__ import annotations

from newsprism.types import Article, ArticleCluster, is_real_article

EVIDENCE_PREFIX = "参与评估。"


def real_articles(cluster: ArticleCluster) -> list[Article]:
    """Real articles only — inline placeholder markers are not articles."""
    return [article for article in cluster.articles if is_real_article(article)]


def real_source_names(cluster: ArticleCluster) -> set[str]:
    return {
        article.source_name
        for article in real_articles(cluster)
        if article.source_name
    }


def article_region(
    article: Article,
    source_regions: dict[str, str] | None = None,
) -> str | None:
    """Region for one article: the article's own origin, else its source's."""
    region = getattr(article, "origin_region", None)
    if region:
        return str(region)
    if source_regions:
        configured = source_regions.get(article.source_name)
        if configured:
            return str(configured)
    return None


def evidence_counts(
    cluster: ArticleCluster,
    source_regions: dict[str, str] | None = None,
) -> tuple[int, int]:
    """(distinct real sources, distinct regions) for the evidence line."""
    articles = real_articles(cluster)
    regions = {
        article_region(article, source_regions)
        for article in articles
    } - {None, ""}
    return len(real_source_names(cluster)), len(regions)


def evidence_summary_text(
    cluster: ArticleCluster,
    source_regions: dict[str, str] | None = None,
) -> str:
    sources, regions = evidence_counts(cluster, source_regions)
    return f"{sources} 个来源、{regions} 个地区{EVIDENCE_PREFIX}"
