"""Tests for Active Perspective Seeker module."""
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from newsprism.config import Config, SourceConfig
from newsprism.repo import init_db
from newsprism.service.seeker import ActiveSeeker
from newsprism.types import Article, ArticleCluster


@pytest.fixture
def mock_config():
    """Create a mock Config with test sources."""
    cfg = MagicMock(spec=Config)
    cfg.tavily_api_key = "test-tavily-key"
    cfg.brightdata_api_key = ""
    cfg.evaluator_model = "deepseek/deepseek-chat"
    cfg.litellm_api_key = "test-litellm-key"
    cfg.litellm_base_url = "https://api.deepseek.com"
    cfg.x_bearer_token = ""
    cfg.youtube_api_key = ""
    cfg.active_search = {
        "result_max_age_hours": 72,
        "min_content_chars": 20,
        "max_results_per_region": 1,
        "min_query_token_overlap": 0.34,
        "min_cluster_title_overlap": 0.08,
        "max_existing_title_overlap": 0.82,
        "semantic_match_threshold": 0.95,
        "telemetry_enabled": False,
        "cost_tracking": {
            "billing": {
                "brightdata_serp": {
                    "pricing_mode": "per_result",
                    "unit_cost_usd": 0.0015,
                    "estimated_results_per_request": 4,
                },
                "tavily_search": {
                    "pricing_mode": "per_request",
                    "unit_cost_usd": 0.008,
                },
                "x_recent_search": {
                    "pricing_mode": "per_request",
                    "unit_cost_usd": 0.02,
                },
                "x_user_lookup": {
                    "pricing_mode": "per_request",
                    "unit_cost_usd": 0.01,
                },
                "x_user_timeline": {
                    "pricing_mode": "per_request",
                    "unit_cost_usd": 0.02,
                },
            }
        },
        "search_profiles": {
            "us": {"language": "en"},
            "jp": {"language": "ja", "x_final_fallback": True},
            "cn": {"language": "zh"},
            "ch": {"language": "de"},
        },
        "official_web_sources": {
            "jp": [
                {
                    "platform": "rss",
                    "source_name": "Japan MOFA RSS",
                    "region": "jp",
                    "url": "https://example.com/jp/rss.xml",
                    "max_results": 4,
                }
            ]
        },
        "official_social_sources": {
            "jp": [
                {
                    "platform": "x",
                    "source_name": "Japan MOFA",
                    "user_id": "mofa-jp",
                    "username": "MofaJapan_en",
                    "region": "jp",
                    "official": True,
                }
            ]
        },
    }
    cfg.sources = [
        SourceConfig(
            name="Reuters",
            name_en="Reuters",
            url="https://reuters.com",
            rss_url="https://reuters.com/rss",
            type="rss",
            weight=1.0,
            language="en",
            region="us",
        ),
        SourceConfig(
            name="NHKニュース",
            name_en="NHK News",
            url="https://nhk.or.jp",
            rss_url="https://nhk.or.jp/rss",
            type="rss",
            weight=1.0,
            language="ja",
            region="jp",
        ),
        SourceConfig(
            name="澎湃新闻",
            name_en="The Paper",
            url="https://thepaper.cn",
            rss_url="https://thepaper.cn/rss",
            type="rss",
            weight=0.95,
            language="zh",
            region="cn",
        ),
    ]
    return cfg


@pytest.fixture
def seeker(mock_config):
    return ActiveSeeker(mock_config)


class TestRegionConfig:
    def test_build_region_config_creates_entries(self, seeker):
        assert "us" in seeker.region_config
        assert "jp" in seeker.region_config
        assert "cn" in seeker.region_config
        assert "ch" in seeker.region_config

    def test_region_config_has_correct_language(self, seeker):
        assert seeker.region_config["jp"].language == "ja"
        assert seeker.region_config["cn"].language == "zh"
        assert seeker.region_config["us"].language == "en"
        assert seeker.region_config["ch"].language == "de"

    def test_region_config_has_native_query_suffix(self, seeker):
        assert seeker.region_config["jp"].native_query_suffix == "ニュース"
        assert seeker.region_config["cn"].native_query_suffix == "新闻"
        assert seeker.region_config["us"].native_query_suffix == "news"

    def test_region_config_has_trusted_domains(self, seeker):
        assert "nhk.or.jp" in seeker.region_config["jp"].trusted_domains
        assert "thepaper.cn" in seeker.region_config["cn"].trusted_domains

    def test_region_config_prefers_dominant_language_for_mixed_region(self):
        cfg = MagicMock(spec=Config)
        cfg.tavily_api_key = "test-tavily-key"
        cfg.brightdata_api_key = ""
        cfg.evaluator_model = "deepseek/deepseek-chat"
        cfg.litellm_api_key = "test-litellm-key"
        cfg.litellm_base_url = "https://api.deepseek.com"
        cfg.x_bearer_token = ""
        cfg.youtube_api_key = ""
        cfg.active_search = {"search_profiles": {"us": {"language": "en"}}}
        cfg.sources = [
            SourceConfig(
                name="VOA中文",
                name_en="VOA Chinese",
                url="https://www.voachinese.com",
                rss_url="https://www.voachinese.com/rss",
                type="rss",
                weight=1.0,
                language="zh",
                region="us",
            ),
            SourceConfig(
                name="Reuters",
                name_en="Reuters",
                url="https://www.reuters.com",
                rss_url="https://www.reuters.com/rss",
                type="rss",
                weight=1.0,
                language="en",
                region="us",
            ),
            SourceConfig(
                name="AP News",
                name_en="AP News",
                url="https://apnews.com",
                rss_url="https://apnews.com/rss",
                type="rss",
                weight=1.0,
                language="en",
                region="us",
            ),
        ]

        mixed_seeker = ActiveSeeker(cfg)

        assert mixed_seeker.region_config["us"].language == "en"

    def test_region_config_supports_search_only_region(self, seeker):
        assert seeker.region_config["ch"].language == "de"
        assert seeker.region_config["ch"].trusted_domains == []


class TestNativeQueryConstruction:
    def test_build_native_query_japanese(self, seeker):
        assert seeker._build_native_query("jp", "semiconductor export") == "semiconductor export ニュース"

    def test_build_native_query_chinese(self, seeker):
        assert seeker._build_native_query("cn", "chip restrictions") == "chip restrictions 新闻"

    def test_build_native_query_english_fallback(self, seeker):
        query = seeker._build_native_query("us", "AI regulation")
        assert "AI regulation" in query
        assert "news" in query.lower()

    @patch("newsprism.service.seeker.litellm.completion")
    def test_build_search_queries_localizes_non_english_regions(self, mock_completion, seeker):
        mock_completion.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="半導体輸出規制"))]
        )
        cluster = ArticleCluster(
            topic_category="Tech",
            articles=[
                Article(
                    url="https://reuters.com/1",
                    title="Japan weighs semiconductor export controls",
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="US and Japan discuss semiconductor export controls.",
                )
            ],
        )

        queries = seeker._build_search_queries(cluster, "jp", "semiconductor export controls")

        assert queries == [
            "半導体輸出規制 ニュース",
            "semiconductor export controls news Japan",
        ]

    def test_keyword_tokens_support_cjk_queries(self, seeker):
        tokens = seeker._keyword_tokens("半導体輸出規制 ニュース")
        assert tokens
        assert any("半導" in token for token in tokens)


class TestSourceRegionLookup:
    def test_get_source_region_known(self, seeker):
        assert seeker._get_source_region("Reuters") == "us"

    def test_get_source_region_unknown(self, seeker):
        assert seeker._get_source_region("Unknown Source") == "unknown"


class TestTwoStageDetection:
    @patch("newsprism.service.seeker.litellm.completion")
    def test_semantic_check_returns_true_when_missing(self, mock_completion, seeker):
        mock_completion.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content='{"perspective_covered": false}'))]
        )
        cluster = ArticleCluster(
            topic_category="Geopolitics",
            articles=[
                Article(
                    url="https://reuters.com/1",
                    title="US-China chip talks progress",
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Content about US-China talks",
                )
            ],
        )

        assert seeker._is_perspective_missing(cluster, "jp") is True

    @patch("newsprism.service.seeker.litellm.completion")
    def test_semantic_check_returns_false_when_covered(self, mock_completion, seeker):
        mock_completion.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content='{"perspective_covered": true}'))]
        )
        cluster = ArticleCluster(
            topic_category="Geopolitics",
            articles=[
                Article(
                    url="https://reuters.com/1",
                    title="Japan quotes officials on chip talks",
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Content quoting Japanese officials",
                )
            ],
        )

        assert seeker._is_perspective_missing(cluster, "jp") is False

    @patch("newsprism.service.seeker.litellm.completion")
    def test_semantic_check_prompt_rejects_third_country_quotes_as_coverage(self, mock_completion, seeker):
        mock_completion.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content='{"perspective_covered": false}'))]
        )
        cluster = ArticleCluster(
            topic_category="Geopolitics",
            articles=[
                Article(
                    url="https://meduza.io/news/1",
                    title="Reuters: US intelligence sees no immediate collapse risk in Iran",
                    source_name="Медуза",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Russian outlet citing Reuters on US intelligence assessment.",
                    origin_region="ru",
                )
            ],
        )

        assert seeker._is_perspective_missing(cluster, "us") is True
        prompt = mock_completion.call_args.kwargs["messages"][0]["content"]
        assert "[Медуза / ru]" in prompt
        assert "Do NOT count translated or syndicated reporting" in prompt


class TestSearchAndFetch:
    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    @patch("newsprism.service.seeker.ActiveSeeker._search_brightdata")
    @patch("newsprism.service.seeker.ActiveSeeker._localize_search_keyword")
    @patch("newsprism.service.seeker.ActiveSeeker._semantic_event_match")
    def test_search_tries_localized_query_before_english_fallback(
        self, mock_semantic, mock_localize, mock_brightdata, mock_tavily, seeker
    ):
        mock_semantic.return_value = 1.0
        mock_localize.return_value = "半導体輸出規制"
        mock_brightdata.return_value = []
        mock_tavily.side_effect = [
            [],
            [
                {
                    "url": "https://nhk.or.jp/news/123",
                    "title": "半導体輸出規制を協議",
                    "content": "日本政府が半導体輸出規制を協議している。" + "x" * 120,
                    "published_at": datetime.now(tz=timezone.utc).isoformat(),
                    "origin_region": "jp",
                    "source_name": "NHKニュース",
                }
            ],
        ]
        cluster = ArticleCluster(
            topic_category="Tech",
            articles=[
                Article(
                    url="https://reuters.com/1",
                    title="Japan weighs semiconductor export controls",
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="US and Japan discuss semiconductor export controls.",
                )
            ],
        )

        results, provider = seeker._search_and_fetch(cluster, "jp", "semiconductor export controls")

        assert len(results) == 1
        assert provider == "Tavily"
        assert mock_tavily.call_args_list[0].args == ("jp", "半導体輸出規制 ニュース")
        assert mock_tavily.call_args_list[1].args == ("jp", "semiconductor export controls news Japan")

    def test_enrich_cluster_logs_canonical_and_localized_queries(self, seeker, caplog):
        cluster = ArticleCluster(topic_category="Tech", articles=[])

        with (
            patch.object(seeker, "_analyze_missing_perspectives", return_value=(["jp"], "semiconductor export controls")),
            patch.object(
                seeker,
                "_build_search_queries",
                return_value=["半導体輸出規制 ニュース", "semiconductor export controls news Japan"],
            ),
            patch.object(seeker, "_search_and_fetch", return_value=([], "")),
            caplog.at_level("INFO"),
        ):
            seeker._enrich_cluster(cluster)

        assert "canonical_query='semiconductor export controls'" in caplog.text
        assert "search_queries=['半導体輸出規制 ニュース', 'semiconductor export controls news Japan']" in caplog.text

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_sets_is_searched_flag(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://nhk.or.jp/news/123",
                "title": "Japan weighs chip export controls",
                "content": "Japan is weighing chip export controls in response to US policy." + "x" * 80,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "jp",
            }
        ]
        cluster = ArticleCluster(
            topic_category="Tech",
            articles=[
                Article(
                    url="https://reuters.com/1",
                    title="US chip export policy",
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="US chip export policy update",
                )
            ],
        )

        results, provider = seeker._search_and_fetch(cluster, "jp", "chip export")

        assert len(results) == 1
        assert provider == "Tavily"
        assert results[0].is_searched is True
        assert results[0].search_region == "jp"
        assert results[0].origin_region == "jp"
        assert results[0].source_kind == "news"

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_deduplicates_by_url(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://reuters.com/1",
                "title": "US chip export policy",
                "content": "Duplicate content" + "x" * 60,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "us",
            },
            {
                "url": "https://nhk.or.jp/news/456",
                "title": "Japan chip export response",
                "content": "Japan responds to chip export policy." + "y" * 80,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "jp",
            },
        ]
        cluster = ArticleCluster(
            topic_category="Tech",
            articles=[
                Article(
                    url="https://reuters.com/1",
                    title="US chip export policy",
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Existing content",
                )
            ],
        )

        results, provider = seeker._search_and_fetch(cluster, "jp", "chip export")

        assert len(results) == 1
        assert provider == "Tavily"
        assert results[0].url == "https://nhk.or.jp/news/456"

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_limits_to_one_result_per_region(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": f"https://nhk.or.jp/news/{idx}",
                "title": f"Japan export Article {idx}",
                "content": "Japan export controls statement " + ("x" * 120),
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "jp",
            }
            for idx in range(5)
        ]

        results, provider = seeker._search_and_fetch(ArticleCluster(topic_category="Tech", articles=[]), "jp", "Japan export")

        assert len(results) == 1
        assert provider == "Tavily"
        assert results[0].url == "https://nhk.or.jp/news/0"

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_deduplicates_accepted_results_by_source(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://apnews.com/article/1",
                "title": "US lawmakers question Iran war costs",
                "content": "US lawmakers question the strategic and fiscal costs of the Iran war." + "x" * 120,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "us",
                "source_name": "AP News",
            },
            {
                "url": "https://apnews.com/article/2",
                "title": "US Senate blocks measure tied to Iran war",
                "content": "US Senate blocks a measure tied to the Iran war after heated debate." + "y" * 120,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "us",
                "source_name": "AP News",
            },
        ]

        results, provider = seeker._search_and_fetch(ArticleCluster(topic_category="World News", articles=[]), "us", "Iran war")

        assert len(results) == 1
        assert provider == "Tavily"
        assert results[0].url == "https://apnews.com/article/1"

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_rejects_third_country_results(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://reuters.com/world/chips",
                "title": "US officials discuss chip exports",
                "content": "US officials discuss chip export controls." + "x" * 120,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "us",
            }
        ]

        results = seeker._search_and_fetch(ArticleCluster(topic_category="Tech", articles=[]), "jp", "chip export")

        assert results == ([], "")

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_rejects_stale_results(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://nhk.or.jp/news/old",
                "title": "Japan chip export archive",
                "content": "Old Japanese report about chip export controls." + "x" * 120,
                "published_at": (datetime.now(tz=timezone.utc) - timedelta(days=10)).isoformat(),
                "origin_region": "jp",
            }
        ]

        results = seeker._search_and_fetch(ArticleCluster(topic_category="Tech", articles=[]), "jp", "chip export")

        assert results == ([], "")
        assert seeker.rejection_counts["stale"] == 1

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_rejects_unknown_freshness_for_non_official_results(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://nhk.or.jp/news/no-date",
                "title": "Japan chip export controls response",
                "content": "Japan chip export controls response and policy details." + "x" * 120,
                "origin_region": "jp",
            }
        ]

        results = seeker._search_and_fetch(ArticleCluster(topic_category="Tech", articles=[]), "jp", "chip export")

        assert results == ([], "")
        assert seeker.rejection_counts["unknown_freshness"] == 1

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_rejects_generic_result_pages(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://www.reuters.com/company/general-motors-co/",
                "title": "General Motors Co | Reuters",
                "content": "General Motors company profile and latest stock information." + "x" * 120,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "jp",
                "source_name": "Reuters",
            },
            {
                "url": "https://www.asahi.com/topics/word/産業用ロボット.html",
                "title": "産業用ロボットの最新ニュース：朝日新聞",
                "content": "Topic page collecting industrial robot news." + "x" * 120,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "jp",
                "source_name": "朝日新聞",
            },
        ]

        results = seeker._search_and_fetch(ArticleCluster(topic_category="Tech", articles=[]), "jp", "industrial robot")

        assert results == ([], "")
        assert seeker.rejection_counts["generic_page"] == 2

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_rejects_repeated_angle(self, mock_tavily, seeker):
        mock_tavily.return_value = [
            {
                "url": "https://nhk.or.jp/news/repeat",
                "title": "Japan chip export controls response",
                "content": "Japan chip export controls response and policy details." + "x" * 120,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "origin_region": "jp",
            }
        ]
        cluster = ArticleCluster(
            topic_category="Tech",
            articles=[
                Article(
                    url="https://nhk.or.jp/news/original",
                    title="Japan chip export controls response",
                    source_name="NHKニュース",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Existing Japanese coverage." + "x" * 120,
                )
            ],
        )

        results = seeker._search_and_fetch(cluster, "jp", "chip export")

        assert results == ([], "")

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    def test_search_falls_back_to_official_social(self, mock_tavily, seeker):
        mock_tavily.return_value = []
        seeker.official_web_sources = {}
        provider = MagicMock()
        provider.fetch_recent.return_value = [
            {
                "url": "https://x.com/MofaJapan_en/status/123",
                "title": "Japan statement on chip export controls",
                "content": "Japan issues an official statement on chip export controls and regional cooperation.",
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "source_name": "Japan MOFA",
                "origin_region": "jp",
                "source_kind": "official_social",
                "platform": "x",
                "account_id": "mofa-jp",
                "is_official_source": True,
            }
        ]
        seeker.social_providers["x"] = provider

        results, provider = seeker._search_and_fetch(ArticleCluster(topic_category="Tech", articles=[]), "jp", "chip export")

        assert len(results) == 1
        assert provider == "official_social"
        assert results[0].source_kind == "official_social"
        assert results[0].platform == "x"
        assert results[0].is_official_source is True

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    @patch("newsprism.service.seeker.ActiveSeeker._search_brightdata")
    @patch("newsprism.service.seeker.ActiveSeeker._search_official_web")
    def test_search_falls_back_to_official_web_before_x(
        self,
        mock_official_web,
        mock_brightdata,
        mock_tavily,
        seeker,
    ):
        mock_brightdata.return_value = []
        mock_tavily.return_value = []
        mock_official_web.return_value = [
            {
                "url": "https://example.com/jp/official-1",
                "title": "Japan foreign ministry statement on chip export controls",
                "content": "Official Japanese ministry statement on export controls and regional coordination." + "x" * 60,
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "source_name": "Japan MOFA Web",
                "origin_region": "jp",
                "source_kind": "official_web",
                "is_official_source": True,
                "searched_provider": "official_web_rss",
            }
        ]
        seeker.social_providers["x"] = MagicMock()

        results, provider = seeker._search_and_fetch(
            ArticleCluster(topic_category="Tech", articles=[]),
            "jp",
            "chip export",
        )

        assert len(results) == 1
        assert provider == "official_web"
        assert results[0].source_kind == "official_web"
        seeker.social_providers["x"].fetch_recent.assert_not_called()

    @patch("newsprism.service.seeker.ActiveSeeker._search_tavily")
    @patch("newsprism.service.seeker.ActiveSeeker._search_brightdata")
    @patch("newsprism.service.seeker.ActiveSeeker._search_official_web")
    def test_search_stops_without_x_when_region_policy_disables_final_fallback(
        self,
        mock_official_web,
        mock_brightdata,
        mock_tavily,
        seeker,
    ):
        mock_brightdata.return_value = []
        mock_tavily.return_value = []
        mock_official_web.return_value = []
        seeker.search_profiles["jp"]["x_final_fallback"] = False
        provider = MagicMock()
        seeker.social_providers["x"] = provider

        results = seeker._search_and_fetch(ArticleCluster(topic_category="Tech", articles=[]), "jp", "chip export")

        assert results == ([], "")
        provider.fetch_recent.assert_not_called()


class TestTelemetryAndCaching:
    def test_official_web_rss_parses_and_records_event_once(self, tmp_path, mock_config):
        db_path = tmp_path / "telemetry.db"
        init_db(db_path)
        mock_config.active_search = {
            **mock_config.active_search,
            "telemetry_enabled": True,
            "telemetry_db_path": str(db_path),
        }
        seeker = ActiveSeeker(mock_config)
        seeker.official_web_sources = {
            "jp": [
                {
                    "platform": "rss",
                    "source_name": "Japan MOFA RSS",
                    "region": "jp",
                    "url": "https://example.com/jp/rss.xml",
                    "max_results": 3,
                }
            ]
        }

        response = MagicMock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.text = """<?xml version="1.0" encoding="UTF-8"?>
        <rss><channel><item>
          <title>Japan statement on export controls</title>
          <link>https://example.com/jp/statement-1</link>
          <description>Official statement on export controls and regional cooperation.</description>
          <pubDate>Fri, 27 Mar 2026 10:00:00 GMT</pubDate>
        </item></channel></rss>"""
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        client.get.return_value = response

        with patch("newsprism.service.seeker.httpx.Client", return_value=client):
            first = seeker._search_official_web("jp")
            second = seeker._search_official_web("jp")

        assert len(first) == 1
        assert first == second
        assert first[0]["source_kind"] == "official_web"
        assert first[0]["searched_provider"] == "official_web_rss"
        assert client.get.call_count == 1

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT provider, request_type, result_count FROM search_request_events ORDER BY id"
            ).fetchall()
        assert rows == [("official_web", "rss", 1)]

    def test_official_web_static_html_fetches_article_content(self, seeker):
        html_response = MagicMock()
        html_response.status_code = 200
        html_response.raise_for_status.return_value = None
        html_response.text = """
        <html><body>
          <div class="news-list-item">
            <a class="story-link" href="/story-1">Brief update</a>
            <span class="story-date">Today</span>
          </div>
        </body></html>
        """

        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        client.get.return_value = html_response

        source = {
            "platform": "static_html",
            "source_name": "Japan MOFA Web",
            "region": "jp",
            "url": "https://example.com/news",
            "item_selector": ".news-list-item",
            "title_selector": ".story-link",
            "link_selector": ".story-link",
            "date_selector": ".story-date",
            "max_results": 3,
        }

        with (
            patch("newsprism.service.seeker.httpx.Client", return_value=client),
            patch.object(
                seeker,
                "_fetch_official_web_article_text",
                return_value="Detailed statement on export controls and regional coordination." + "x" * 80,
            ) as mock_article_text,
        ):
            results = seeker._fetch_official_web_static_html("jp", source)

        assert len(results) == 1
        assert results[0]["source_kind"] == "official_web"
        assert results[0]["searched_provider"] == "official_web_static_html"
        assert results[0]["url"] == "https://example.com/story-1"
        mock_article_text.assert_called_once_with("https://example.com/story-1", seeker._official_web_headers(source))

    def test_tavily_search_records_event_once_per_cache_key(self, tmp_path, mock_config):
        db_path = tmp_path / "telemetry.db"
        init_db(db_path)
        mock_config.active_search = {
            **mock_config.active_search,
            "telemetry_enabled": True,
            "telemetry_db_path": str(db_path),
        }
        seeker = ActiveSeeker(mock_config)

        response = MagicMock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "results": [
                {
                    "url": "https://nhk.or.jp/news/123",
                    "title": "Japan chip export response",
                    "raw_content": "Japan reacts to chip export controls." + "x" * 100,
                    "published_at": datetime.now(tz=timezone.utc).isoformat(),
                }
            ]
        }
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        client.post.return_value = response

        with patch("newsprism.service.seeker.httpx.Client", return_value=client):
            first = seeker._search_tavily("jp", "chip export")
            second = seeker._search_tavily("jp", "chip export")

        assert client.post.call_count == 1
        assert first == second
        assert first[0]["searched_provider"] == "tavily_search"

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT provider, request_type, result_count FROM search_request_events"
            ).fetchall()
        assert rows == [("tavily_search", "search", 1)]

    def test_official_x_source_uses_user_id_without_lookup(self, tmp_path, mock_config):
        db_path = tmp_path / "telemetry.db"
        init_db(db_path)
        mock_config.x_bearer_token = "test-x-token"
        mock_config.active_search = {
            **mock_config.active_search,
            "telemetry_enabled": True,
            "telemetry_db_path": str(db_path),
        }
        seeker = ActiveSeeker(mock_config)

        response = MagicMock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "data": [
                {
                    "id": "tweet-1",
                    "text": "Official statement on export controls",
                    "created_at": datetime.now(tz=timezone.utc).isoformat(),
                }
            ]
        }
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        client.get.return_value = response

        with patch("newsprism.service.seeker.httpx.Client", return_value=client):
            results = seeker._search_official_social("jp", "chip export")

        request_urls = [call.args[0] for call in client.get.call_args_list]
        assert request_urls == ["https://api.x.com/2/users/mofa-jp/tweets"]
        assert results[0]["searched_provider"] == "x_user_timeline"

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT provider, request_type, account_id FROM search_request_events ORDER BY id"
            ).fetchall()
        assert rows == [("x", "user_timeline", "mofa-jp")]
