"""Tests for the slim Tavily-only Active Seeker: triggers and acceptance gates."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import numpy as np

from newsprism.config import Config, SourceConfig
from newsprism.service.locales import country_name, query_languages
from newsprism.service.seeker import ActiveSeeker, CandidateIdentity, VoiceTarget
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


def _fresh_result(result: dict) -> dict:
    """Mark a synthetic Tavily result as having current provider evidence."""
    return {**result, "published_at": datetime.now(tz=timezone.utc).isoformat()}


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
    assert "Acme Congo" in queries[-1]
    assert "Congo - Kinshasa" in queries[-1]


def test_country_query_is_scoped_to_each_named_actor():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    alphabet = VoiceTarget(region="us", label="Alphabet", role="company")
    microsoft = VoiceTarget(region="us", label="Microsoft", role="company")

    alphabet_query = seeker._build_search_queries(
        cluster, alphabet, "AI infrastructure spending", "country"
    )[-1]
    microsoft_query = seeker._build_search_queries(
        cluster, microsoft, "AI infrastructure spending", "country"
    )[-1]

    assert alphabet_query.startswith("Alphabet ")
    assert microsoft_query.startswith("Microsoft ")
    assert alphabet_query != microsoft_query


def test_deterministic_keyword_fallback_uses_headline_when_llm_fails():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].title = "Google Gemini Spark expands worldwide"

    with patch("newsprism.service.seeker.litellm.completion", side_effect=RuntimeError("bad json")):
        keyword = seeker._analyze_search_keyword(
            cluster, [VoiceTarget("us", "Google", "company")]
        )

    assert keyword == "Google Gemini Spark expands worldwide"


def test_result_url_canonicalization_unwraps_real_redirect_and_rejects_opaque_token():
    assert ActiveSeeker._canonical_result_url(
        "/goto?url=https%3A%2F%2Fwww.ford.com%2Fcompany"
    ) == "https://www.ford.com/company"
    assert ActiveSeeker._canonical_result_url("/goto?url=CAESopaque-token") == ""


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
    cluster.articles[0].origin_region = "gb"
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
    cluster.articles[0].origin_region = "gb"
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


def test_target_materiality_rejects_incidental_comparison_and_facility_entities():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].title = "FCC reviews Unitree while Nvidia is used as a comparison"
    cluster.articles[0].content = (
        "FCC reviews Unitree. Nvidia is only a market comparison. "
        "Aeon was incidentally mentioned near the Natanz nuclear facility."
    )
    cluster.impact.voice_needs = [
        VoiceNeed("FCC", "us", "government_agency", "regulator", "FCC", "required"),
        VoiceNeed("Nvidia", "us", "company", "comparison", "Nvidia", "required"),
        VoiceNeed("Aeon", "jp", "company", "directly_affected_principal", "Aeon", "incidental"),
        VoiceNeed("Natanz nuclear facility", "ir", "organization", "principal", "Natanz nuclear facility", "required"),
    ]

    assert seeker._missing_voice_targets(cluster) == [
        VoiceTarget(region="us", label="FCC", role="government_agency")
    ]


def test_existing_related_country_editorial_fills_actor_fallback():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].content = "OpenAI reported a security incident."
    cluster.impact.voice_needs = [VoiceNeed("OpenAI", "us", "company")]

    assert seeker._missing_voice_targets(cluster) == []


def test_existing_duplicate_only_satisfies_target_when_identity_already_qualifies():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].url = "https://acmelabs.com/statement"
    cluster.articles[0].title = "Acme Labs statement"
    cluster.articles[0].content = "Acme Labs statement details. " * 10
    target = VoiceTarget("us", "Acme Labs", "company")
    result = {
        "url": cluster.articles[0].url,
        "title": cluster.articles[0].title,
        "content": cluster.articles[0].content,
    }

    _accepted, rejected = seeker._accept_results(cluster, target, [result], None, "official")
    assert rejected == [("duplicate_of_existing", result["url"])]

    cluster.articles[0].is_official_source = True
    _accepted, rejected = seeker._accept_results(cluster, target, [result], None, "official")
    assert rejected == [("coverage_satisfied", result["url"])]


def test_official_identity_resolution_restricts_event_search_to_resolved_domain():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs disclosed a security incident. " * 10
    target = VoiceTarget("us", "Acme Labs", "company")
    calls: list[tuple[str, tuple[str, ...], str]] = []

    def search(_target, query, include_domains=None, request_type="search"):
        calls.append((query, tuple(include_domains or []), request_type))
        if request_type == "identity_resolution":
            return ([{
                "url": "https://acmelabs.com/",
                "title": "Acme Labs official site",
                "content": "",
            }], None)
        return ([{
            "url": "https://acmelabs.com/security-incident",
            "title": "Acme Labs security incident statement",
            "content": "Acme Labs disclosed the security incident. " * 10,
            "published_at": datetime.now(tz=timezone.utc).isoformat(),
        }], None)

    identity = CandidateIdentity(
        source_type="official_web",
        publisher_entity="Acme Labs",
        publisher_region="us",
        relationship="same_entity",
    )
    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value=identity):
        article, reason, trace = seeker._search_target(cluster, target, "security incident", None)

    assert reason is None
    assert article is not None and article.is_official_source
    assert trace[-1] == {"stage": "official", "reason": "accepted"}
    assert calls[0] == ("Acme Labs official website", (), "identity_resolution")
    assert calls[1][1] == ("acmelabs.com",)


def test_reviewed_social_binding_avoids_unrestricted_official_event_search():
    cfg = _config()
    cfg.active_search["official_account_bindings"] = {
        "x": {"exampleministry": {"entity": "Example Ministry", "region": "us"}},
    }
    seeker = ActiveSeeker(cfg)
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Example Ministry announced a policy change. " * 10
    target = VoiceTarget("us", "Example Ministry", "ministry")
    calls: list[tuple[str, tuple[str, ...], str]] = []

    def search(_target, query, include_domains=None, request_type="search"):
        calls.append((query, tuple(include_domains or []), request_type))
        return ([{
            "url": "https://x.com/exampleministry/status/123",
            "title": "Example Ministry policy statement",
            "content": "Example Ministry announced the policy change. " * 10,
            "published_at": datetime.now(tz=timezone.utc).isoformat(),
        }], None)

    with patch.object(
        ActiveSeeker,
        "_resolve_official_domains",
        return_value=([], "official_binding_not_found"),
    ), patch.object(ActiveSeeker, "_search_tavily", side_effect=search):
        article, reason, trace = seeker._search_target(
            cluster, target, "policy change", None
        )

    assert reason is None
    assert article is not None and article.is_official_source
    assert trace[-1] == {"stage": "official", "reason": "accepted"}
    assert calls == [
        (
            "site:x.com/exampleministry policy change",
            ("x.com",),
            "search_official",
        )
    ]


def test_known_publisher_country_mismatch_is_rejected_not_left_pending():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("fr", "France", "country")
    result = {
        "url": "https://example.qa/france",
        "title": "France policy update",
        "content": "France policy reporting. " * 12,
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    identity = CandidateIdentity(
        source_type="country_editorial",
        publisher_entity="Example Qatar",
        publisher_region="qa",
        relationship="covered_by_third_party",
    )
    with patch.object(ActiveSeeker, "_verify_candidate", return_value=identity):
        accepted, rejected = seeker._accept_results(
            _cluster(), target, [result], None, "country"
        )
    assert accepted == []
    assert rejected == [("not_related_country_source", result["url"])]


def test_thin_official_pdf_survives_after_entity_domain_is_resolved():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("us", "FCC", "government_agency")
    identity = CandidateIdentity(
        source_type="official_web",
        publisher_entity="FCC",
        publisher_region="us",
        relationship="same_entity",
    )
    seeker._resolved_official_bindings[("fcc", "us")] = {"fcc.gov": identity}
    result = {
        "url": "https://fcc.gov/sites/default/files/robots-nsd.pdf",
        "title": "FCC robot national security determination",
        "content": "",
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    accepted, rejected = seeker._accept_results(
        _cluster(), target, [result], None, "official"
    )
    assert len(accepted) == 1
    assert rejected == []


def test_official_search_precedes_country_fallback_and_unreviewed_local_source_stays_pending():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs announced a security incident. " * 10
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    calls: list[str] = []
    local_result = {
        "url": "https://local-paper.example/acme",
        "title": "Acme Labs responds to incident",
        "content": "Acme Labs response and details. " * 10,
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    def search(region, query, **kwargs):
        calls.append(query)
        return ([], None) if "official website" in query else ([local_result], None)

    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value="country_editorial"):
        article, reason, trace = seeker._search_target(cluster, target, "Acme breach", None)

    assert article is None
    assert reason == "candidate_pending_review"
    assert [item["stage"] for item in trace] == ["official", "country"]
    assert calls[0] == "Acme Labs official website"
    assert len(calls) >= 2


def test_reviewed_local_source_can_be_used_as_country_fallback():
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "local-paper.example": {"verdict": "country_editorial", "region": "us"},
    }
    seeker = ActiveSeeker(cfg)
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs announced a security incident. " * 10
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
        article, reason, trace = seeker._search_target(cluster, target, "Acme breach", None)
    assert reason is None
    assert article is not None and article.origin_region == "us"
    assert article.is_official_source is False
    assert trace[-1] == {"stage": "country", "reason": "accepted"}


def test_independent_configured_source_is_an_operational_country_fallback():
    cfg = _config()
    cfg.sources.append(
        SourceConfig(
            "NPR",
            "NPR",
            "https://www.npr.org",
            None,
            "rss",
            1.0,
            "en",
            region="us",
            tier="editorial",
            ownership="independent_nonprofit",
        )
    )
    seeker = ActiveSeeker(cfg)
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs announced a security incident. " * 10
    target = VoiceTarget("us", "Acme Labs", "company")
    result = _fresh_result({
        "url": "https://www.npr.org/acme-incident",
        "title": "US reporting on the Acme Labs incident",
        "content": "Current reporting about the same Acme Labs security incident. " * 10,
    })
    calls: list[tuple[str, ...]] = []

    def search(_target, _query, include_domains=None, request_type="search"):
        calls.append(tuple(include_domains or []))
        return ([], None) if request_type == "identity_resolution" else ([result], None)

    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value="reject"):
        article, reason, trace = seeker._search_target(
            cluster, target, "Acme security incident", None
        )

    assert reason is None
    assert article is not None and article.origin_region == "us"
    assert ("npr.org",) in calls
    assert trace[-1] == {"stage": "country", "reason": "accepted"}


def test_country_stage_uses_event_match_instead_of_exact_translated_entity_text():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("eu", "欧洲央行", "organization")
    local_article = _article(
        "local",
        "ECB responds to inflation data",
        region="eu",
        url="https://local.example/ecb",
    )
    local_article.content = "European Central Bank response to current inflation data. " * 8

    assert seeker._rejection_reason(
        local_article, target, [], None, stage="official"
    ) == "entity_mismatch"
    class _AlignedModel:
        @staticmethod
        def encode(*_args, **_kwargs):
            return [[1.0, 0.0]]

    with patch("newsprism.service.seeker.get_model", return_value=_AlignedModel()):
        assert seeker._rejection_reason(
            local_article, target, [], np.array([1.0, 0.0]), stage="country"
        ) == ""


def test_official_binding_alias_resolves_translated_ecb_target_without_discovery():
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "ecb.europa.eu": {
            "verdict": "official_web",
            "region": "eu",
            "entity": "European Central Bank",
            "entity_aliases": ["欧洲央行", "欧央行", "ECB"],
        }
    }
    seeker = ActiveSeeker(cfg)

    domains, reason = seeker._resolve_official_domains(
        VoiceTarget("eu", "欧洲央行", "organization")
    )

    assert reason is None
    assert domains == ["ecb.europa.eu"]


def test_unverified_candidate_is_not_accepted_as_official_or_country_voice():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs announced a security incident. " * 10
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
        article, reason, _trace = seeker._search_target(cluster, target, "Acme breach", None)
    assert article is None
    assert "not_related_country_source" in (reason or "")


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
    # A date-bounded query is not page-level evidence. Undated official or
    # background pages must not be accepted as current supplements.
    assert seeker._is_fresh(None) is False


def test_parse_url_date_extracts_date_from_common_url_patterns():
    """URL-path date fallback for the Tavily published_date=None problem."""
    seeker = ActiveSeeker(_config())
    # Major outlets embed the date in the path.
    assert seeker._parse_url_date("https://www.cnn.com/2026/07/20/world/live-news/x").date().isoformat() == "2026-07-20"
    assert seeker._parse_url_date("https://news.northeastern.edu/2026/07/20/andy-burnham").date().isoformat() == "2026-07-20"
    # No date-like segment → None (freshness gate fails closed).
    assert seeker._parse_url_date("https://www.bbc.co.uk/news/uk-politics-12345678") is None
    assert seeker._parse_url_date("https://example.com/no-date-here") is None
    assert seeker._parse_url_date(None) is None


def test_parse_url_date_supports_year_month_quarter_and_year_only_paths():
    seeker = ActiveSeeker(_config())

    assert seeker._parse_url_date("https://ustr.gov/press/2025/september/statement").date().isoformat() == "2025-09-01"
    assert seeker._parse_url_date("https://images.samsung.com/ir/2023_4Q_BusinessReport.pdf").date().isoformat() == "2023-10-01"
    assert seeker._parse_url_date("https://example.gov/archive/2021/02/report.pdf").date().isoformat() == "2021-02-01"
    assert seeker._parse_url_date("https://example.gov/2025/annual-report").date().isoformat() == "2025-01-01"


def test_undated_official_result_is_rejected_even_when_search_is_date_bounded():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("us", "Acme Labs", "company")
    identity = CandidateIdentity(
        source_type="official_web",
        publisher_entity="Acme Labs",
        publisher_region="us",
        relationship="same_entity",
    )
    seeker._resolved_official_bindings["acmelabs", "us"] = {"acmelabs.com": identity}

    accepted, rejected = seeker._accept_results(
        _cluster(),
        target,
        [{
            "url": "https://acmelabs.com/security-incident",
            "title": "Acme Labs statement on the security incident",
            "content": "Acme Labs disclosed the security incident. " * 10,
        }],
        None,
        "official",
    )

    assert accepted == []
    assert rejected == [("stale_result", "https://acmelabs.com/security-incident")]


def test_current_dated_official_background_page_is_not_a_perspective():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("us", "Microsoft", "company")
    identity = CandidateIdentity(
        source_type="official_web",
        publisher_entity="Microsoft",
        publisher_region="us",
        relationship="same_entity",
    )
    seeker._resolved_official_bindings["microsoft", "us"] = {"microsoft.com": identity}

    accepted, rejected = seeker._accept_results(
        _cluster(),
        target,
        [{
            "url": "https://www.microsoft.com/investor-relations/annual-report/2025",
            "title": "Microsoft Annual Report 2025",
            "content": "Microsoft annual report and financial results. " * 10,
            "published_date": datetime.now(tz=timezone.utc).isoformat(),
        }],
        None,
        "official",
    )

    assert accepted == []
    assert rejected == [("background_context", "https://www.microsoft.com/investor-relations/annual-report/2025")]


def test_current_official_event_evidence_remains_accepted_and_marked():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("us", "FCC", "government_agency")
    identity = CandidateIdentity(
        source_type="official_web",
        publisher_entity="FCC",
        publisher_region="us",
        relationship="same_entity",
    )
    seeker._resolved_official_bindings["fcc", "us"] = {"fcc.gov": identity}

    accepted, rejected = seeker._accept_results(
        _cluster(),
        target,
        [{
            "url": "https://www.fcc.gov/2026/08/01/statement-on-spectrum-decision",
            "title": "FCC statement on the spectrum decision",
            "content": "The FCC issued a statement on the spectrum decision. " * 10,
            "published_at": datetime.now(tz=timezone.utc).isoformat(),
        }],
        None,
        "official",
    )

    assert rejected == []
    assert len(accepted) == 1
    assert accepted[0].source_kind == "official_web"
    assert accepted[0].search_acceptance_reason == "current_event_perspective"


def test_same_conflict_search_result_is_background_below_direct_event_floor():
    seeker = ActiveSeeker(_config())
    article = _article("AP", "G7 leaders pledge further Ukraine aid")

    class _ContextOnlyModel:
        def encode(self, *_args, **_kwargs):
            return [[0.60, 0.80]]

    with patch("newsprism.service.seeker.get_model", return_value=_ContextOnlyModel()):
        role = seeker._search_evidence_role(article, np.array([1.0, 0.0]))

    assert role == "background_context"


def test_official_domain_does_not_relax_event_materiality_threshold():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("us", "Acme Labs", "company")
    identity = CandidateIdentity(
        source_type="official_web",
        publisher_entity="Acme Labs",
        publisher_region="us",
        relationship="same_entity",
    )
    seeker._resolved_official_bindings["acmelabs", "us"] = {"acmelabs.com": identity}
    article = _article(
        "acmelabs.com",
        "Acme Labs statement on an unrelated event",
        url="https://acmelabs.com/2026/08/01/unrelated-event",
    )
    article.content = "Acme Labs published a statement about an unrelated event. " * 10

    class _LowSimilarityModel:
        def encode(self, *_args, **_kwargs):
            return [[0.5, 0.8660254]]

    with patch("newsprism.service.seeker.get_model", return_value=_LowSimilarityModel()):
        reason = seeker._rejection_reason(
            article,
            target,
            [],
            [1.0, 0.0],
            stage="official",
        )

    assert reason == "event_mismatch"


def test_result_to_article_rejects_thin_content():
    seeker = ActiveSeeker(_config())
    thin = {"url": "https://x.fr/a", "title": "t", "content": "short"}
    assert seeker._result_to_article(thin, "fr") is None
    full = {"url": "https://x.fr/a", "title": "Titre", "content": "x" * 200, "published_at": None}
    article = seeker._result_to_article(full, "fr")
    assert article is not None and article.is_searched and article.search_region == "fr"


def test_country_target_skips_official_stage_and_never_labels_broadcaster_official():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    target = VoiceTarget(region="fr", label="France", role="country")
    calls: list[str] = []
    rfi_result = {
        "url": "https://rfi.fr/bordeaux",
        "title": "French mayor comments on Bordeaux wildfire",
        "content": "Bordeaux wildfire response reported by a French broadcaster. " * 8,
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    def search(searched_target, query, **kwargs):
        calls.append(query)
        assert searched_target == target
        return [rfi_result], None

    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search), \
         patch.object(
             ActiveSeeker,
             "_verify_candidate",
             return_value=CandidateIdentity(
                 source_type="official_web", publisher_entity="RFI", relationship="same_entity"
             ),
         ):
        article, reason, _trace = seeker._search_target(cluster, target, "Bordeaux wildfire", None)

    assert article is None
    assert calls
    assert all("official statement" not in query for query in calls)
    assert "not_related_country_source" in (reason or "")


def test_recovery_target_drives_official_query_and_matching_domain_acceptance():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].title = "Microsoft releases MAI-Cyber-1-Flash"
    cluster.articles[0].content = "Microsoft introduced the MAI-Cyber-1-Flash model. " * 8
    cluster.impact.voice_needs = []
    recovered = VoiceTarget(region="us", label="Microsoft", role="company")
    with patch.object(ActiveSeeker, "_recover_actor_targets", return_value=[recovered]):
        assert seeker._missing_voice_targets(cluster) == [recovered]
    assert seeker._build_search_queries(cluster, recovered, "MAI Cyber release", "official")[0].startswith(
        "Microsoft official statement"
    )

    candidate = _article(
        "microsoft.ai",
        "Microsoft posts technical details for its new model",
        url="https://microsoft.ai/news/mai-cyber",
    )
    candidate.content = "Microsoft announces MAI-Cyber-1-Flash. " * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web", publisher_entity="Microsoft", relationship="same_entity"
        ),
    ):
        accepted, rejected = seeker._accept_results(
            cluster,
            recovered,
            [_fresh_result({"url": candidate.url, "title": candidate.title, "content": candidate.content})],
            None,
            "official",
        )
    assert len(accepted) == 1
    assert rejected == []


def test_recovery_target_accepts_localized_evidence_for_canonical_actor_label():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].title = "微软发布网络安全模型"
    cluster.articles[0].content = "微软发布新的网络安全模型。" * 10

    targets = seeker._validated_actor_targets(
        cluster,
        [("Microsoft", "us", "company", "微软")],
    )

    assert targets == [VoiceTarget(region="us", label="Microsoft", role="company")]


def test_official_publisher_mismatch_is_rejected_even_when_event_matches():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget(region="us", label="Example Ministry", role="ministry")
    article = _article("microsoft.ai", "Example Ministry policy", url="https://microsoft.ai/policy")
    article.content = "Example Ministry policy announcement. " * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web", publisher_entity="Microsoft", relationship="same_entity"
        ),
    ):
        accepted, rejected = seeker._accept_results(_cluster(), target, [
            _fresh_result({"url": article.url, "title": article.title, "content": article.content})
        ], None, "official")
    assert accepted == []
    assert rejected == [("publisher_target_mismatch", article.url)]


def test_high_confidence_governmental_domain_bypasses_translated_label_mismatch():
    """Regression for the 2026-07-31 false rejection: target label is in
    Chinese ("美国白宫" == The White House) while publisher_entity is in
    English, so free-text equality never matches even though the verifier
    already judged same_entity with full confidence on whitehouse.gov."""
    seeker = ActiveSeeker(_config())
    target = VoiceTarget(region="us", label="美国白宫", role="government")
    article = _article(
        "whitehouse.gov",
        "美国白宫回应中美经贸通话",
        url="https://www.whitehouse.gov/briefing-room/statement",
    )
    article.content = "美国白宫就中美经贸通话发布声明。" * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web",
            publisher_entity="The White House",
            relationship="same_entity",
            confidence=1.0,
        ),
    ):
        accepted, rejected = seeker._accept_results(_cluster(), target, [
            _fresh_result({"url": article.url, "title": article.title, "content": article.content})
        ], None, "official")
    assert rejected == []
    assert len(accepted) == 1
    assert accepted[0].is_official_source is True


def test_high_confidence_governmental_domain_bypasses_acronym_mismatch():
    """Regression for the 2026-07-31 CENTCOM false rejection: target label is
    the bare acronym "CENTCOM" while publisher_entity is the spelled-out
    name, so free-text equality never matches even though the verifier
    already judged same_entity with full confidence on centcom.mil."""
    seeker = ActiveSeeker(_config())
    target = VoiceTarget(region="us", label="CENTCOM", role="government_agency")
    article = _article(
        "centcom.mil",
        "CENTCOM statement on regional operations",
        url="https://www.centcom.mil/press-release",
    )
    article.content = "CENTCOM released a statement on regional operations. " * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web",
            publisher_entity="U.S. Central Command (CENTCOM)",
            relationship="same_entity",
            confidence=1.0,
        ),
    ):
        accepted, rejected = seeker._accept_results(_cluster(), target, [
            _fresh_result({"url": article.url, "title": article.title, "content": article.content})
        ], None, "official")
    assert rejected == []
    assert len(accepted) == 1
    assert accepted[0].is_official_source is True


def test_high_confidence_same_entity_still_requires_governmental_domain():
    """The confidence+domain bypass must not become a general escape hatch:
    a same_entity, high-confidence verdict on a non-governmental domain
    (microsoft.ai) still falls through to the strict text-equality check, so
    an unrelated org still gets caught."""
    seeker = ActiveSeeker(_config())
    target = VoiceTarget(region="us", label="Example Ministry", role="ministry")
    article = _article("microsoft.ai", "Example Ministry policy", url="https://microsoft.ai/policy")
    article.content = "Example Ministry policy announcement. " * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web",
            publisher_entity="Microsoft",
            relationship="same_entity",
            confidence=1.0,
        ),
    ):
        accepted, rejected = seeker._accept_results(_cluster(), target, [
            _fresh_result({"url": article.url, "title": article.title, "content": article.content})
        ], None, "official")
    assert accepted == []
    assert rejected == [("publisher_target_mismatch", article.url)]


def test_relationship_mismatch_is_rejected_even_with_high_confidence_and_gov_domain():
    """The verifier's relationship field remains the primary integrity
    signal: a non-same_entity verdict is rejected regardless of confidence
    or a governmental domain — this is the protection commit 0ffca0f added
    and it must not regress."""
    seeker = ActiveSeeker(_config())
    target = VoiceTarget(region="us", label="CENTCOM", role="government_agency")
    article = _article(
        "centcom.mil", "CENTCOM mentioned in wire report", url="https://www.centcom.mil/some-page"
    )
    article.content = "A wire report references CENTCOM among other regional commands. " * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web",
            publisher_entity="Some Other Command",
            relationship="uncertain",
            confidence=0.9,
        ),
    ):
        accepted, rejected = seeker._accept_results(_cluster(), target, [
            _fresh_result({"url": article.url, "title": article.title, "content": article.content})
        ], None, "official")
    assert accepted == []
    assert rejected == [("publisher_target_mismatch", article.url)]


def test_registry_entity_aliases_accept_translated_or_acronym_target_label():
    """3b: a reviewed source_verdicts binding can list entity_aliases so one
    domain entry matches the target under any accepted name form (English
    canonical name, Chinese translation, acronym) instead of requiring the
    target label to exactly equal a single stored entity string."""
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "whitehouse.gov": {
            "verdict": "official_web",
            "region": "us",
            "entity": "The White House",
            "entity_aliases": ["美国白宫", "白宫", "White House"],
        },
        "shared-government.example": {
            "verdict": "official_web",
            "region": "us",
            "entity": "Example Ministry",
        },
    }
    seeker = ActiveSeeker(cfg)
    zh_target = VoiceTarget(region="us", label="美国白宫", role="government")
    identity, reason = seeker._registry_identity(
        "https://www.whitehouse.gov/briefing-room/statement", zh_target, "official"
    )
    assert reason is None
    assert identity is not None and identity.relationship == "same_entity"

    # A non-governmental registry domain (no .gov/.mil suffix) keeps the
    # strict alias requirement: an unrelated target label is hard-rejected
    # rather than left to fall through to the verifier. Only a *governmental*
    # domain's alias miss falls through — see
    # test_registry_alias_miss_on_governmental_domain_falls_through_not_hard_rejects.
    unrelated_target = VoiceTarget(region="us", label="Other Ministry", role="government")
    _, unrelated_reason = seeker._registry_identity(
        "https://shared-government.example/statement", unrelated_target, "official"
    )
    assert unrelated_reason == "publisher_binding_unverified"


def test_registry_alias_miss_on_governmental_domain_falls_through_not_hard_rejects():
    """A reviewed governmental-domain binding whose alias list doesn't happen
    to cover this particular target-label phrasing must not hard-block the
    candidate (the impact LLM's voice_needs labels vary in translation and
    phrasing far more than any fixed alias list can enumerate) — it should
    fall through to the verifier-based judgment in _official_identity_reason
    instead. Non-governmental registry domains keep the strict alias
    requirement (see test_official_registry_requires_exact_entity_binding...).
    """
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "treasury.gov": {
            "verdict": "official_web",
            "region": "us",
            "entity": "U.S. Department of the Treasury",
            "entity_aliases": ["美国财政部"],
        },
    }
    seeker = ActiveSeeker(cfg)
    # Label not present in the alias list at all.
    target = VoiceTarget(region="us", label="美财政部", role="ministry")
    identity, reason = seeker._registry_identity(
        "https://home.treasury.gov/press-release", target, "official"
    )
    assert identity is None and reason is None

    article = _article(
        "home.treasury.gov",
        "美财政部就中美经贸问题发表声明",
        url="https://home.treasury.gov/press-release",
    )
    article.content = "美财政部就中美经贸问题发表声明。" * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web",
            publisher_entity="U.S. Department of the Treasury",
            relationship="same_entity",
            confidence=1.0,
        ),
    ):
        accepted, rejected = seeker._accept_results(_cluster(), target, [
            _fresh_result({"url": article.url, "title": article.title, "content": article.content})
        ], None, "official")
    assert rejected == []
    assert len(accepted) == 1


def test_governmental_domain_tail_bypass_accepts_exact_match_without_confidence():
    """Reproduces the fourth 2026-07-31 production case: target label
    "U.S. Department of State" and publisher_entity are textually identical,
    but the verifier didn't populate confidence (so the high-confidence fast
    path doesn't fire) and the old code fell through to the domain-vs-label
    equality tail, where registered.domain("state.gov") == "state" never
    equals the full department name. On a governmental domain that tail must
    be skipped once the entity text already matched."""
    seeker = ActiveSeeker(_config())
    target = VoiceTarget(region="us", label="U.S. Department of State", role="government_agency")
    article = _article(
        "state.gov",
        "U.S. Department of State issues statement",
        url="https://www.state.gov/statement",
    )
    article.content = "The U.S. Department of State issued a statement on the talks. " * 10
    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web",
            publisher_entity="U.S. Department of State",
            relationship="same_entity",
            # confidence intentionally omitted (None) — matches production evidence.
        ),
    ):
        accepted, rejected = seeker._accept_results(_cluster(), target, [
            _fresh_result({"url": article.url, "title": article.title, "content": article.content})
        ], None, "official")
    assert rejected == []
    assert len(accepted) == 1


def test_ambiguous_official_binding_is_queued_for_review():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    target = VoiceTarget(region="us", label="Example Ministry", role="ministry")
    result = {
        "url": "https://shared-government.example/statement",
        "title": "Example Ministry issues statement",
        "content": "Example Ministry issues a detailed statement. " * 10,
        "published_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    with patch.object(
        ActiveSeeker,
        "_verify_candidate",
        return_value=CandidateIdentity(
            source_type="official_web",
            publisher_entity="Example Ministry",
            relationship="same_entity",
        ),
    ), patch.object(ActiveSeeker, "_record_candidate_review") as record_review:
        accepted, rejected = seeker._accept_results(
            cluster, target, [result], None, "official"
        )

    assert accepted == []
    assert rejected == [("publisher_binding_unverified", result["url"])]
    assert record_review.call_args.args[4:] == (
        "pending_review",
        "publisher_binding_unverified",
    )


def test_official_registry_requires_exact_entity_binding_and_social_account_is_exact():
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "shared-government.example": {"verdict": "official_web", "entity": "Example Ministry", "region": "us"},
        "missing-entity.example": {"verdict": "official_web", "region": "us"},
    }
    cfg.active_search["official_account_bindings"] = {
        "x": {"exampleministry": {"entity": "Example Ministry", "region": "us"}},
    }
    seeker = ActiveSeeker(cfg)
    target = VoiceTarget(region="us", label="Example Ministry", role="ministry")
    identity, reason = seeker._registry_identity("https://shared-government.example/statement", target, "official")
    assert identity is not None and identity.relationship == "same_entity" and reason is None
    _, wrong_entity_reason = seeker._registry_identity(
        "https://shared-government.example/statement", VoiceTarget("us", "Other Ministry", "ministry"), "official"
    )
    assert wrong_entity_reason == "publisher_binding_unverified"
    _, missing_entity_reason = seeker._registry_identity("https://missing-entity.example/a", target, "official")
    assert missing_entity_reason == "publisher_binding_unverified"
    assert seeker._social_account_binding("https://x.com/ExampleMinistry/status/1", target)
    assert not seeker._social_account_binding("https://x.com/OtherMinistry/status/1", target)
    assert seeker._social_account("https://youtube.com/watch?v=abc") is None


def test_country_name_voice_need_cannot_become_actor_target():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    cluster.articles[0].title = "France and Example Ministry discuss policy"
    cluster.articles[0].content = "France and Example Ministry discuss policy. " * 8
    cluster.impact.voice_needs = [
        VoiceNeed(label="France", country="fr", kind="country"),
        VoiceNeed(label="Example Ministry", country="fr", kind="ministry"),
    ]
    assert seeker._missing_voice_targets(cluster) == [
        VoiceTarget(region="fr", label="Example Ministry", role="ministry")
    ]


# ─── Country-stage relaxation (undated results + entity-free fallback) ───────

def test_country_stage_accepts_undated_reviewed_source():
    """Undated Tavily results are not proof of staleness in the country stage:
    the provider query is already time-bounded (days=3). A reviewed
    country-editorial domain that matches the event still passes."""
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "local-paper.example": {"verdict": "country_editorial", "region": "us"},
    }
    seeker = ActiveSeeker(cfg)
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs announced a security incident. " * 10
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    result = {
        "url": "https://local-paper.example/acme",
        "title": "Acme Labs responds to incident",
        "content": "Acme Labs response and details. " * 10,
        # no published_at / published_date / date → undated result
    }
    with patch.object(ActiveSeeker, "_search_tavily", return_value=([result], None)), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value="country_editorial"):
        article, reason, trace = seeker._search_target(cluster, target, "Acme breach", None)
    assert reason is None
    assert article is not None and article.origin_region == "us"
    assert trace[-1] == {"stage": "country", "reason": "accepted"}


def test_country_stage_undated_relaxation_can_be_disabled():
    cfg = _config()
    cfg.active_search["country_allow_undated"] = False
    seeker = ActiveSeeker(cfg)
    target = VoiceTarget("us", "Acme Labs", "company")
    article = _article("local", "Acme Labs responds", region="us")
    article.published_at = None
    assert seeker._rejection_reason(article, target, [], None, stage="country") == "stale_result"


def test_official_stage_still_rejects_undated():
    """The official stage keeps the strict rule: undated official pages are
    usually background material (fact sheets, evergreen pages)."""
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("us", "Acme Labs", "company")
    article = _article("local", "Acme Labs responds", region="us")
    article.published_at = None
    assert seeker._rejection_reason(article, target, [], None, stage="official") == "stale_result"
    # Same undated article passes the country stage (entity is mentioned).
    assert seeker._rejection_reason(article, target, [], None, stage="country") == ""


def test_country_query_can_drop_entity_for_fallback():
    seeker = ActiveSeeker(_config())
    target = VoiceTarget("us", "Acme Labs", "company")
    cluster = _cluster()
    scoped = seeker._build_search_queries(cluster, target, "security incident", "country")
    entity_free = seeker._build_search_queries(
        cluster, target, "security incident", "country", entity_scoped=False
    )
    assert scoped and all("Acme" in query for query in scoped)
    assert entity_free and all("Acme" not in query for query in entity_free)
    assert "United States" in entity_free[0]


def test_entity_free_country_fallback_runs_after_entity_scoped_failure():
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "local-paper.example": {"verdict": "country_editorial", "region": "us"},
    }
    seeker = ActiveSeeker(cfg)
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs announced a security incident. " * 10
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    good = _fresh_result({
        "url": "https://local-paper.example/incident",
        "title": "US local coverage of the Acme Labs security incident",
        "content": "Current reporting about the Acme Labs security incident in the US. " * 10,
    })
    calls: list[tuple[str, str]] = []

    def search(_target, query, include_domains=None, request_type="search"):
        calls.append((query, request_type))
        if request_type == "identity_resolution":
            return ([], None)
        if request_type == "search_country_fallback":
            return ([good], None)
        return ([], None)  # entity-scoped country queries find nothing

    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value="reject"):
        article, reason, trace = seeker._search_target(
            cluster, target, "security incident", None
        )

    assert reason is None
    assert article is not None and article.origin_region == "us"
    assert trace[-1] == {"stage": "country", "reason": "accepted"}
    fallback_queries = [query for query, request_type in calls if request_type == "search_country_fallback"]
    assert fallback_queries, "entity-free country fallback query was never issued"
    assert "Acme" not in fallback_queries[0]


def test_entity_free_country_fallback_can_be_disabled():
    cfg = _config()
    cfg.active_search["source_verdicts"] = {
        "local-paper.example": {"verdict": "country_editorial", "region": "us"},
    }
    cfg.active_search["country_entity_free_fallback"] = False
    seeker = ActiveSeeker(cfg)
    cluster = _cluster()
    cluster.articles[0].origin_region = "gb"
    cluster.articles[0].content = "Acme Labs announced a security incident. " * 10
    cluster.impact.voice_needs = [VoiceNeed(label="Acme Labs", country="us", kind="company")]
    target = seeker._missing_voice_targets(cluster)[0]
    calls: list[str] = []

    def search(_target, query, include_domains=None, request_type="search"):
        calls.append(request_type)
        return ([], None)

    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search):
        _article, reason, _trace = seeker._search_target(
            cluster, target, "security incident", None
        )
    assert reason == "country_fallback_not_found"
    assert "search_country_fallback" not in calls
