"""Tests for the slim Tavily-only Active Seeker: triggers and acceptance gates."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

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
        }], None)

    identity = CandidateIdentity(
        source_type="official_web",
        publisher_entity="Acme Labs",
        publisher_region="us",
        relationship="same_entity",
    )
    with patch.object(ActiveSeeker, "_search_tavily", side_effect=search), \
         patch.object(ActiveSeeker, "_verify_candidate", return_value=identity):
        article, reason = seeker._search_target(cluster, target, "security incident", None)

    assert reason is None
    assert article is not None and article.is_official_source
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
        }], None)

    with patch.object(
        ActiveSeeker,
        "_resolve_official_domains",
        return_value=([], "official_binding_not_found"),
    ), patch.object(ActiveSeeker, "_search_tavily", side_effect=search):
        article, reason = seeker._search_target(
            cluster, target, "policy change", None
        )

    assert reason is None
    assert article is not None and article.is_official_source
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
        article, reason = seeker._search_target(cluster, target, "Acme breach", None)

    assert article is None
    assert reason == "candidate_pending_review"
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
        article, reason = seeker._search_target(cluster, target, "Acme breach", None)
    assert reason is None
    assert article is not None and article.origin_region == "us"
    assert article.is_official_source is False


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
        article, reason = seeker._search_target(cluster, target, "Acme breach", None)
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
        article, reason = seeker._search_target(cluster, target, "Bordeaux wildfire", None)

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
            [{"url": candidate.url, "title": candidate.title, "content": candidate.content}],
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
            {"url": article.url, "title": article.title, "content": article.content}
        ], None, "official")
    assert accepted == []
    assert rejected == [("publisher_target_mismatch", article.url)]


def test_ambiguous_official_binding_is_queued_for_review():
    seeker = ActiveSeeker(_config())
    cluster = _cluster()
    target = VoiceTarget(region="us", label="Example Ministry", role="ministry")
    result = {
        "url": "https://shared-government.example/statement",
        "title": "Example Ministry issues statement",
        "content": "Example Ministry issues a detailed statement. " * 10,
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
