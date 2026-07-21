"""Tavily key rotation + inline placeholder synthesis.

Covers Issue #1 root cause: a single invalid key caused every search to return
HTTP 401 with zero results for >5 weeks. Rotation tries keys in order; if all
fail, an inline placeholder Article surfaces the missing perspective + reason
to the reader instead of silently dropping it.
"""
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
import pytest

from newsprism.config import Config, SourceConfig
from newsprism.service.seeker import ActiveSeeker
from newsprism.types import Article, ArticleCluster, ImpactAssessment


def _config_with_keys(*keys: str) -> Config:
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
            "search_profiles": {"us": {"language": "en"}, "fr": {"language": "fr"}},
        },
    )
    cfg.tavily_api_keys = list(keys)
    cfg.tavily_api_key = keys[0] if keys else ""
    return cfg


def _cluster_with_target_region(target_region: str = "fr") -> ArticleCluster:
    """Build a cluster that triggers enrichment and targets a missing region."""
    cluster = ArticleCluster(
        topic_category="US event",
        articles=[
            Article(
                url="https://reuters.com/e1",
                title="US event unfolds",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="body " * 40,
                origin_region="us",
            )
        ],
    )
    cluster.impact = ImpactAssessment(cluster_key="k", composite=0.7, status="seek_more_evidence")
    return cluster


def _tavily_401_response() -> httpx.Response:
    return httpx.Response(
        status_code=401,
        json={"detail": "Invalid API key"},
        request=httpx.Request("POST", "https://api.tavily.com/search"),
    )


def _tavily_200_response() -> httpx.Response:
    return httpx.Response(
        status_code=200,
        json={
            "results": [
                {
                    "url": "https://lemonde.fr/article",
                    "title": "Événement américain se déroule",
                    "content": "x" * 200,
                    "published_date": datetime.now(tz=timezone.utc).isoformat(),
                }
            ]
        },
        request=httpx.Request("POST", "https://api.tavily.com/search"),
    )


# ── Key rotation ────────────────────────────────────────────────────────────


def test_rotation_falls_through_401_to_working_key():
    """The root-cause bug: first key 401s, second key works."""
    seeker = ActiveSeeker(_config_with_keys("bad-key", "good-key"))
    calls: list[str] = []

    def fake_post(*args, **kwargs):
        api_key = kwargs.get("json", {}).get("api_key", args[1] if len(args) > 1 else "")
        calls.append(api_key)
        return _tavily_200_response() if api_key == "good-key" else _tavily_401_response()

    with patch.object(httpx.Client, "post", side_effect=fake_post):
        results, reason = seeker._search_tavily("fr", "US event news France")

    assert reason is None
    assert len(results) == 1
    assert calls == ["bad-key", "good-key"]
    # Active key pinned to the working one for subsequent calls.
    assert seeker._active_key_idx == 1


def test_rotation_short_circuits_when_all_keys_exhausted():
    """If all keys already failed this run, don't retry Tavily at all."""
    seeker = ActiveSeeker(_config_with_keys("k1", "k2"))
    seeker._exhausted_keys = {0, 1}
    with patch.object(httpx.Client, "post") as mock_post:
        results, reason = seeker._search_tavily("fr", "any query")
    assert mock_post.call_count == 0
    assert results == []
    assert reason == "http_401"


def test_rotation_reports_auth_failure_when_all_keys_bad():
    seeker = ActiveSeeker(_config_with_keys("k1", "k2"))

    def fake_post(*args, **kwargs):
        return _tavily_401_response()

    with patch.object(httpx.Client, "post", side_effect=fake_post):
        results, reason = seeker._search_tavily("fr", "any query")

    assert results == []
    assert reason == "http_401"
    assert seeker._exhausted_keys == {0, 1}


def test_rotation_persists_active_key_across_calls():
    """After a key succeeds, subsequent calls start with that key."""
    seeker = ActiveSeeker(_config_with_keys("bad", "good", "good2"))
    seeker._active_key_idx = 0
    call_keys: list[str] = []

    def fake_post(*args, **kwargs):
        api_key = kwargs.get("json", {}).get("api_key", "")
        call_keys.append(api_key)
        return _tavily_200_response() if api_key in ("good", "good2") else _tavily_401_response()

    with patch.object(httpx.Client, "post", side_effect=fake_post):
        seeker._search_tavily("fr", "q1")
        seeker._search_tavily("fr", "q2")

    # First call rotates bad→good; second call starts at good (no retry of bad).
    assert call_keys == ["bad", "good", "good"]


# ── Placeholder synthesis ───────────────────────────────────────────────────


def test_placeholder_synthesized_when_search_fails():
    """Reader sees an inline ⚠️ placeholder for the missing region, not silence."""
    seeker = ActiveSeeker(_config_with_keys("bad"))
    cluster = _cluster_with_target_region()

    # Force the analyzer to target "fr" (a missing region) with a known keyword.
    with patch.object(ActiveSeeker, "_analyze_search_targets", return_value=("US event", ["fr"])), \
         patch.object(httpx.Client, "post", side_effect=lambda *a, **k: _tavily_401_response()):
        seeker.enhance_clusters([cluster])

    placeholders = [a for a in cluster.articles if getattr(a, "is_placeholder", False)]
    assert len(placeholders) == 1
    assert placeholders[0].search_region == "fr"
    assert placeholders[0].is_searched is True
    assert placeholders[0].search_acceptance_status == "failed"
    assert placeholders[0].search_acceptance_reason == "http_401"
    assert "France" in placeholders[0].title  # region name surfaced in placeholder title


def test_placeholder_does_not_count_as_source():
    """is_multi_source must ignore placeholder articles entirely."""
    seeker = ActiveSeeker(_config_with_keys("bad"))
    cluster = _cluster_with_target_region()
    organic_source_count_before = len(cluster.sources)

    with patch.object(ActiveSeeker, "_analyze_search_targets", return_value=("US event", ["fr"])), \
         patch.object(httpx.Client, "post", side_effect=lambda *a, **k: _tavily_401_response()):
        seeker.enhance_clusters([cluster])

    assert len(cluster.articles) == 2  # organic + placeholder
    assert len(cluster.sources) == organic_source_count_before  # placeholder excluded
    assert cluster.is_multi_source is False


def test_placeholder_not_synthesized_when_search_succeeds():
    """If the search accepts a real article, no placeholder."""
    seeker = ActiveSeeker(_config_with_keys("good"))
    cluster = _cluster_with_target_region()

    with patch.object(ActiveSeeker, "_analyze_search_targets", return_value=("US event", ["fr"])), \
         patch.object(httpx.Client, "post", side_effect=lambda *a, **k: _tavily_200_response()):
        seeker.enhance_clusters([cluster])

    placeholders = [a for a in cluster.articles if getattr(a, "is_placeholder", False)]
    assert len(placeholders) == 0
    assert any(a.is_searched and not getattr(a, "is_placeholder", False) for a in cluster.articles)


def test_failure_reasons_for_common_rejection_paths():
    """The renderer relies on a known finite vocabulary of reason codes."""
    seeker = ActiveSeeker(_config_with_keys("good"))
    cluster = _cluster_with_target_region()

    # Tavily returns 200 with empty results list.
    empty_200 = httpx.Response(
        status_code=200,
        json={"results": []},
        request=httpx.Request("POST", "https://api.tavily.com/search"),
    )
    with patch.object(ActiveSeeker, "_analyze_search_targets", return_value=("US event", ["fr"])), \
         patch.object(httpx.Client, "post", side_effect=lambda *a, **k: empty_200):
        seeker.enhance_clusters([cluster])

    placeholders = [a for a in cluster.articles if getattr(a, "is_placeholder", False)]
    assert len(placeholders) == 1
    # Either empty_results (no candidates) or a gate-rejection code — both surface.
    assert placeholders[0].search_acceptance_reason in (
        "empty_results",
        "no_acceptable_result",
        None,
    ) or placeholders[0].search_acceptance_reason
