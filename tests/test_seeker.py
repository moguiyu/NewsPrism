"""Tests for the slim Tavily-only Active Seeker: triggers and acceptance gates."""
from datetime import datetime, timedelta, timezone

from newsprism.config import Config, SourceConfig
from newsprism.service.seeker import ActiveSeeker
from newsprism.types import Article, ArticleCluster, ImpactAssessment


def _config(tavily_key: str = "test-key") -> Config:
    cfg = Config(
        raw={},
        sources=[
            SourceConfig("Reuters", "Reuters", "https://reuters.com", None, "rss", 1.0, "en", region="us"),
            SourceConfig("Le Monde", "Le Monde", "https://lemonde.fr", None, "rss", 1.0, "fr", region="fr"),
        ],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={},
        active_search={
            "telemetry_enabled": False,
            "max_regions_per_cluster": 2,
            "min_organic_sources_to_skip": 8,
            "search_profiles": {
                "us": {"language": "en"},
                "fr": {"language": "fr"},
                "jp": {"language": "ja"},
            },
        },
    )
    cfg.tavily_api_key = tavily_key
    return cfg


def _article(source: str, title: str, region: str = "us", url: str | None = None) -> Article:
    return Article(
        url=url or f"https://example.com/{source}/{title}",
        title=title,
        source_name=source,
        published_at=datetime.now(tz=timezone.utc),
        content="body " * 40,
        origin_region=region,
    )


def _cluster(status: str = "seek_more_evidence", composite: float = 0.6, hot: bool = False) -> ArticleCluster:
    cluster = ArticleCluster(topic_category="US strikes", articles=[_article("Reuters", "US strikes targets")])
    cluster.impact = ImpactAssessment(cluster_key="k", composite=composite, status=status)
    cluster.is_hot_topic = hot
    return cluster


def test_region_config_only_keeps_major_regions():
    seeker = ActiveSeeker(_config())
    assert set(seeker.region_config) == {"us", "fr", "jp"}


def test_should_enrich_on_seek_more_evidence():
    seeker = ActiveSeeker(_config())
    assert seeker._should_enrich(_cluster(status="seek_more_evidence")) is True


def test_should_enrich_hot_cluster_above_trigger():
    seeker = ActiveSeeker(_config())
    assert seeker._should_enrich(_cluster(status="publishable", composite=0.7, hot=True)) is True


def test_should_enrich_main_feed_cluster_above_trigger():
    seeker = ActiveSeeker(_config())
    assert seeker._should_enrich(_cluster(status="publishable", composite=0.7, hot=False)) is True


def test_should_not_enrich_ordinary_publishable():
    seeker = ActiveSeeker(_config())
    assert seeker._should_enrich(_cluster(status="publishable", composite=0.3, hot=False)) is False


def test_should_not_enrich_when_many_organic_sources():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles = [_article(f"src{i}", f"t{i}") for i in range(9)]
    assert seeker._should_enrich(cluster) is False


def test_disabled_without_api_key():
    seeker = ActiveSeeker(_config(tavily_key=""))
    clusters = [_cluster()]
    assert seeker.enhance_clusters(clusters) is clusters  # untouched


def test_region_valid_by_tld():
    seeker = ActiveSeeker(_config())
    article = _article("lemonde.fr", "Une frappe", region="fr", url="https://lemonde.fr/article")
    assert seeker._is_region_valid(article, "fr") is True
    assert seeker._is_region_valid(article, "jp") is False


def test_freshness_gate_rejects_old():
    seeker = ActiveSeeker(_config())
    fresh = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    old = datetime.now(tz=timezone.utc) - timedelta(hours=200)
    assert seeker._is_fresh(fresh) is True
    assert seeker._is_fresh(old) is False
    # Unknown publish date is now ACCEPTED (not rejected). Tavily frequently
    # returns published_date=None even for fresh results — the search itself
    # is date-bounded (days: 3), so trust that bound rather than dropping
    # 100% of results. See the 2026-07-22 incident.
    assert seeker._is_fresh(None) is True


def test_parse_url_date_extracts_date_from_common_url_patterns():
    """URL-path date fallback for the Tavily published_date=None problem."""
    seeker = ActiveSeeker(_config())
    # Major outlets embed the date in the path.
    assert seeker._parse_url_date("https://www.cnn.com/2026/07/20/world/live-news/x").date().isoformat() == "2026-07-20"
    assert seeker._parse_url_date("https://news.northeastern.edu/2026/07/20/andy-burnham").date().isoformat() == "2026-07-20"
    # No date-like segment → None (freshness gate falls back to query-bound trust).
    assert seeker._parse_url_date("https://www.bbc.co.uk/news/uk-politics-12345678") is None
    assert seeker._parse_url_date("https://example.com/no-date-here") is None
    assert seeker._parse_url_date(None) is None


def test_result_to_article_rejects_thin_content():
    seeker = ActiveSeeker(_config())
    thin = {"url": "https://x.fr/a", "title": "t", "content": "short"}
    assert seeker._result_to_article(thin, "fr") is None
    full = {"url": "https://x.fr/a", "title": "Titre", "content": "x" * 200, "published_at": None}
    article = seeker._result_to_article(full, "fr")
    assert article is not None and article.is_searched and article.search_region == "fr"
