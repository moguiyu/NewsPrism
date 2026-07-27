"""Tests for the slim Tavily-only Active Seeker: triggers and acceptance gates."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from newsprism.config import Config, SourceConfig
from newsprism.service.locales import country_name, query_languages
from newsprism.service.seeker import ActiveSeeker, VoiceTarget
from newsprism.types import Article, ArticleCluster, ImpactAssessment, VoiceNeed


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
    cluster.impact = ImpactAssessment(cluster_key="k", composite=composite, status=status, subject_regions=["us"])
    cluster.is_hot_topic = hot
    return cluster


def test_region_config_only_keeps_major_regions():
    seeker = ActiveSeeker(_config())
    assert set(seeker.region_config) == {"us", "fr", "jp"}


def test_iso_locale_metadata_covers_unprofiled_country_and_multilingual_override():
    assert country_name("cd") == "Congo - Kinshasa"
    assert query_languages("cd") == ("fr",)
    assert query_languages("tz") == ("sw",)
    assert query_languages("ch") == ("de", "fr")
    assert query_languages("cd", ["ln", "fr"]) == ("ln", "fr")


def test_unprofiled_country_uses_full_name_and_local_language_query():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    target = VoiceTarget(region="cd", label="Acme Congo", role="company")
    with patch.object(ActiveSeeker, "_localize_search_keyword", side_effect=lambda *args: f"local:{args[-1]}"):
        queries = seeker._build_search_queries(cluster, target, "Acme incident", "country")
    assert queries[0] == "local:fr"
    assert "Congo - Kinshasa" in queries[-1]


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


def test_should_enrich_is_not_suppressed_by_unrelated_source_count():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles = [_article(f"src{i}", f"t{i}") for i in range(9)]
    assert seeker._should_enrich(cluster) is True


def test_disabled_without_api_key():
    seeker = ActiveSeeker(_config(tavily_key=""))
    clusters = [_cluster()]
    assert seeker.enhance_clusters(clusters) is clusters  # untouched


def test_missing_target_is_limited_to_involved_country_not_absent_profiles():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].url = "https://reuters.com/us-event"
    # Reuters is an existing US voice, so no country fallback is required.
    assert seeker._missing_voice_targets(cluster) == []


def test_missing_country_without_named_entity_uses_country_fallback():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.impact.subject_regions = ["il"]
    target = seeker._missing_voice_targets(cluster)[0]
    assert target.region == "il"
    assert target.role == "country"


def test_missing_country_renders_failure_marker_after_country_search_fails():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.impact.subject_regions = ["il"]
    with patch.object(ActiveSeeker, "_analyze_search_keyword", return_value="Israel event"), \
         patch.object(ActiveSeeker, "_search_tavily", return_value=([], None)):
        seeker.enhance_clusters([cluster])
    placeholder = next(article for article in cluster.articles if article.is_placeholder)
    assert placeholder.search_region == "il"
    assert placeholder.search_acceptance_reason == "country_fallback_not_found"
    assert "Israel" in placeholder.source_name


def test_dynamic_entity_target_rejects_wrong_entity_results():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].url = "https://reuters.com/us-event"
    cluster.articles[0].content = "Acme Labs announced a security incident."
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    assert target.label == "Acme Labs"
    unrelated = _article("bbc.com", "Other AI story", region="gb", url="https://bbc.com/other")
    unrelated.content = "Anthropic statement"
    assert seeker._rejection_reason(unrelated, target, [], None) == "entity_mismatch"
    wrong_entity = _article("acme.example", "Company post", region="us", url="https://acme.example/post")
    wrong_entity.content = "A general platform update"
    assert seeker._rejection_reason(wrong_entity, target, [], None) == "entity_mismatch"


def test_event_voice_needs_only_targets_the_named_related_entities():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].url = "https://reuters.com/openai-hugging-face"
    cluster.articles[0].content = "OpenAI and Hugging Face reported an AI security incident."
    cluster.impact.voice_needs = [
        VoiceNeed(label="OpenAI", country="us", kind="company"),
        VoiceNeed(label="Hugging Face", country="us", kind="company"),
    ]
    targets = seeker._missing_voice_targets(cluster)
    assert [(target.region, target.label) for target in targets] == [
        ("us", "OpenAI"),
        ("us", "Hugging Face"),
    ]


def test_official_search_precedes_country_fallback_and_unreviewed_local_source_stays_pending():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    calls: list[str] = []
    local_result = {
        "url": "https://local-paper.example/acme",
        "title": "Acme Labs responds to incident",
        "content": "Acme Labs response and details. " * 10,
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    def search(region, query):
        calls.append(query)
        return ([], None) if "official statement" in query else ([local_result], None)

    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value="country_editorial"):
        article, reason = seeker._search_target(cluster, target, "Acme breach", None)

    assert article is None
    assert reason == "candidate_pending_review"
    assert "official statement" in calls[0]
    assert any("local news" in query for query in calls[1:])


def test_reviewed_local_source_can_be_used_as_country_fallback():
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "local-paper.example": {"verdict": "country_editorial", "region": "us"},
    }
    seeker = ActiveSeeker(cfg)
    cluster = _cluster()
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    result = {
        "url": "https://local-paper.example/acme",
        "title": "Acme Labs responds to incident",
        "content": "Acme Labs response and details. " * 10,
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    with patch.object(ActiveSeeker, "_search_tavily", return_value=([result], None)), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value="country_editorial"):
        article, reason = seeker._search_target(cluster, target, "Acme breach", None)
    assert reason is None
    assert article is not None and article.origin_region == "us"
    assert article.is_official_source is False


def test_unverified_candidate_is_not_accepted_as_official_or_country_voice():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    result = {
        "url": "https://shady.example/acme",
        "title": "Acme Labs response",
        "content": "Acme Labs response and details. " * 10,
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    with patch.object(ActiveSeeker, "_search_tavily", return_value=([result], None)), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value="reject"):
        article, reason = seeker._search_target(cluster, target, "Acme breach", None)
    assert article is None
    assert "candidate_unverified" in (reason or "")


def test_existing_verified_official_voice_skips_search():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    cluster.articles[0].is_official_source = True
    cluster.articles[0].content = "Acme Labs issued a statement."
    assert seeker._missing_voice_targets(cluster) == []


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
