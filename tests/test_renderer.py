"""Tests for renderer searched article attribution."""
from datetime import datetime, timezone
import json
import os

from lxml import html as lxml_html
import pytest

from newsprism.runtime.renderer import HtmlRenderer, _REGION_FLAG
from newsprism.types import Article, ArticleCluster, ClusterSummary, PerspectiveGroup


@pytest.fixture
def renderer():
    return HtmlRenderer(
        output_dir="output",
        template_dir="templates",
        template_name="design-a",
        source_regions={
            "Reuters": "us",
            "NHKニュース": "jp",
            "澎湃新闻": "cn",
        },
    )


@pytest.fixture
def premium_renderer():
    return HtmlRenderer(
        output_dir="output",
        template_dir="templates",
        template_name="design-premium",
        source_regions={
            "Reuters": "us",
            "BBC": "gb",
            "NHKニュース": "jp",
            "联合早报": "sg",
            "澎湃新闻": "cn",
        },
    )


class TestSourceFlag:
    def test_flag_for_organic_article(self, renderer):
        assert renderer._source_flag("Reuters") == "🇺🇸"

    def test_flag_for_searched_article_with_region(self, renderer):
        assert renderer._source_flag("Unknown Source", search_region="jp") == "🇯🇵"

    def test_flag_for_searched_article_unknown_region(self, renderer):
        assert renderer._source_flag("Unknown Source", search_region="xx") == "🌐"

    def test_flag_for_unknown_source_no_region(self, renderer):
        assert renderer._source_flag("Unknown Source") == ""


class TestPerspectivesContext:
    def test_perspectives_include_provenance_metadata(self, renderer):
        organic_article = Article(
            url="https://reuters.com/1",
            title="US chip policy",
            source_name="Reuters",
            published_at=datetime.now(tz=timezone.utc),
            content="Content about chips",
        )
        searched_news = Article(
            url="https://nhk.or.jp/1",
            title="Japan chip response",
            source_name="NHKニュース",
            published_at=datetime.now(tz=timezone.utc),
            content="Japanese news response to chip policy.",
            is_searched=True,
            search_region="jp",
            origin_region="jp",
            searched_provider="tavily_search",
        )
        official_web = Article(
            url="https://example.com/jp/statement",
            title="Japan ministry statement on export controls",
            source_name="Japan MOFA Web",
            published_at=datetime.now(tz=timezone.utc),
            content="Official web statement on export controls and regional coordination.",
            is_searched=True,
            search_region="jp",
            source_kind="official_web",
            is_official_source=True,
            origin_region="jp",
            searched_provider="official_web_rss",
        )
        official_social = Article(
            url="https://x.com/mofa/status/1",
            title="Official statement on export controls",
            source_name="Japan MOFA",
            published_at=datetime.now(tz=timezone.utc),
            content="Official statement on export controls and regional coordination.",
            is_searched=True,
            search_region="jp",
            source_kind="official_social",
            platform="x",
            is_official_source=True,
            origin_region="jp",
            searched_provider="x_user_timeline",
        )
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Tech-General",
                articles=[organic_article, searched_news, official_web, official_social],
            ),
            summary="**Test headline**\n\nBody text here.",
            perspectives={
                "Reuters": "US perspective",
                "NHKニュース": "Japanese reporting",
                "Japan MOFA Web": "Official Japanese website statement",
                "Japan MOFA": "Official Japanese statement",
            },
            grouped_perspectives=[
                PerspectiveGroup(
                    sources=["Reuters"],
                    perspective="US perspective",
                ),
                PerspectiveGroup(
                    sources=["NHKニュース", "Japan MOFA Web", "Japan MOFA"],
                    perspective="Japanese reaction and official stance",
                ),
            ],
        )

        grouped = renderer._build_grouped_perspectives(summary)
        perspectives_list = renderer._build_perspectives_list(summary)

        assert len(grouped) == 2
        assert grouped[1]["label"] == "🔍NHKニュース · 搜索补充 / 🔍Japan MOFA Web · 官方网站 / 🔍Japan MOFA · 官方X"

        reuters_p = next(p for p in perspectives_list if p["source"] == "Reuters")
        assert reuters_p["is_searched"] is False
        assert reuters_p["provenance_label"] is None
        assert reuters_p["flag"] == "🇺🇸"

        nhk_p = next(p for p in perspectives_list if p["source"] == "NHKニュース")
        assert nhk_p["is_searched"] is True
        assert nhk_p["search_region"] == "jp"
        assert nhk_p["source_kind"] == "news"
        assert nhk_p["searched_provider"] == "tavily_search"
        assert nhk_p["provenance_label"] == "搜索补充"

        web_p = next(p for p in perspectives_list if p["source"] == "Japan MOFA Web")
        assert web_p["is_official_source"] is True
        assert web_p["source_kind"] == "official_web"
        assert web_p["searched_provider"] == "official_web_rss"
        assert web_p["provenance_label"] == "官方网站"
        assert web_p["represented_region"] == "jp"

        mofa_p = next(p for p in perspectives_list if p["source"] == "Japan MOFA")
        assert mofa_p["is_official_source"] is True
        assert mofa_p["source_kind"] == "official_social"
        assert mofa_p["platform"] == "x"
        assert mofa_p["searched_provider"] == "x_user_timeline"
        assert mofa_p["provenance_label"] == "官方X"
        assert mofa_p["represented_region"] == "jp"

    def test_render_without_perspectives_stays_readable(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/story",
                        title="Single-source story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Single-source story body.",
                    )
                ],
            ),
            summary="**Single-source story**\n\nBody text here.",
            perspectives={},
        )

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert payload["clusters"][0]["headline"] == "Single-source story"
        assert "Body text here." in payload["clusters"][0]["summary"]
        assert "个视角" not in html
        assert payload["clusters"][0]["perspectives_list"] == []

    def test_premium_single_source_renders_source_row_with_link(self, premium_renderer, tmp_path):
        premium_renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/single",
                        title="Single-source story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Single-source story body.",
                    )
                ],
            ),
            summary="**Single-source story**\n\nBody text here.",
            perspectives={},
        )

        html_path = premium_renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "Reuters" in html
        assert "查看 1 个视角" not in html
        assert 'href="https://reuters.com/single"' in html
        assert payload["clusters"][0]["source_groups"][0]["url"] == "https://reuters.com/single"
        assert payload["clusters"][0]["has_expandable_perspectives"] is False

    def test_premium_multi_source_renders_grouped_perspectives(self, premium_renderer, tmp_path):
        premium_renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/group",
                        title="Tariff response",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="US angle.",
                    ),
                    Article(
                        url="https://bbc.com/group",
                        title="Tariff response UK",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="UK angle.",
                    ),
                    Article(
                        url="https://zaobao.com/group",
                        title="Tariff response SG",
                        source_name="联合早报",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Singapore angle.",
                    ),
                ],
            ),
            summary="**关税冲击扩大**\n\n全球市场正评估最新影响。",
            perspectives={
                "Reuters": "Western market angle",
                "BBC": "Western market angle",
                "联合早报": "Asian trade angle",
            },
            grouped_perspectives=[
                PerspectiveGroup(
                    sources=["Reuters", "BBC"],
                    perspective="英美媒体共同聚焦关税对西方市场与政策预期的冲击。",
                ),
                PerspectiveGroup(
                    sources=["联合早报"],
                    perspective="亚洲媒体更强调区域贸易链与出口预期承压。",
                ),
            ],
        )

        html_path = premium_renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "查看 2 个视角" in html
        assert "Reuters" in html
        assert "BBC" in html
        assert "联合早报" in html
        assert "视角差异：" in html
        assert "persp-sep" not in html
        assert 'class="persp-angle" title="英美媒体共同聚焦关税对西方市场与政策预期的冲击。">英美媒体共同聚焦关税对西方市场与政策预期的冲击。</span>' in html
        assert ".persp-item {" in html
        assert ".persp-line {" in html
        assert ".card.is-expanded .source-chips {" in html
        assert 'a + a::before {' in html
        assert 'content: " / ";' in html
        assert "grid-template-columns: minmax(160px, 240px) minmax(0, 1fr);" in html
        assert "max-width: 62ch;" in html
        assert "overflow-wrap: anywhere;" in html
        assert len(payload["clusters"][0]["grouped_perspectives"]) == 2
        assert payload["clusters"][0]["source_groups"][0]["label"] == "Reuters / BBC"
        assert payload["clusters"][0]["distinct_perspective_count"] == 2
        assert payload["clusters"][0]["perspective_preview"]
        assert payload["clusters"][0]["has_expandable_perspectives"] is True

    def test_premium_invalid_perspective_groups_are_suppressed(self, premium_renderer, tmp_path):
        premium_renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Robotics",
                articles=[
                    Article(
                        url="https://reuters.com/robotics",
                        title="Robotics tariff dispute",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="US robotics trade policy.",
                    ),
                    Article(
                        url="https://asahi.com/offtopic",
                        title="Factory accident unrelated",
                        source_name="朝日新聞",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Off-topic article.",
                        is_searched=True,
                        search_region="jp",
                        origin_region="jp",
                    ),
                ],
            ),
            summary="**机器人反倾销争议升温**\n\n各方继续评估产业链影响。",
            perspectives={
                "Reuters": "聚焦机器人贸易摩擦对产业链和市场预期的影响。",
                "朝日新聞": "报道韩国工厂工业机器人致死事故，关注机器人安全风险及调查进展，与反倾销议题无关。",
            },
            grouped_perspectives=[
                PerspectiveGroup(
                    sources=["Reuters"],
                    perspective="聚焦机器人贸易摩擦对产业链和市场预期的影响。",
                ),
                PerspectiveGroup(
                    sources=["朝日新聞"],
                    perspective="报道韩国工厂工业机器人致死事故，关注机器人安全风险及调查进展，与反倾销议题无关。",
                ),
            ],
        )

        html_path = premium_renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "与反倾销议题无关" not in html
        assert "查看 1 个视角" not in html
        assert "朝日新聞" not in html
        assert payload["clusters"][0]["distinct_perspective_count"] == 1
        assert payload["clusters"][0]["suppressed_group_count"] == 1
        assert payload["clusters"][0]["has_expandable_perspectives"] is False
        assert len(payload["clusters"][0]["grouped_perspectives"]) == 1

    def test_empty_render_keeps_existing_latest_symlink(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        previous_date = "2026-03-26"
        (tmp_path / previous_date).mkdir()
        (tmp_path / "latest").symlink_to(previous_date)

        html_path = renderer.render([], datetime(2026, 3, 27, tzinfo=timezone.utc).date())
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert payload["cluster_count"] == 0
        assert payload["hot_topic_story_count"] == 0
        assert os.readlink(tmp_path / "latest") == previous_date

    def test_non_empty_render_updates_latest_symlink(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/fresh",
                        title="Fresh story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Fresh story body.",
                    )
                ],
            ),
            summary="**Fresh story**\n\nBody text here.",
            perspectives={},
        )

        renderer.render([summary], datetime(2026, 3, 27, tzinfo=timezone.utc).date())

        assert os.readlink(tmp_path / "latest") == "2026-03-27"


class TestRegionFlagMapping:
    def test_common_regions_have_flags(self):
        assert _REGION_FLAG["cn"] == "🇨🇳"
        assert _REGION_FLAG["us"] == "🇺🇸"
        assert _REGION_FLAG["jp"] == "🇯🇵"
        assert _REGION_FLAG["kr"] == "🇰🇷"
        assert _REGION_FLAG["ru"] == "🇷🇺"
        assert _REGION_FLAG["de"] == "🇩🇪"
        assert _REGION_FLAG["gb"] == "🇬🇧"


class TestPremiumHotTopics:
    def test_premium_template_renders_macro_topic_family_tabs_and_keeps_json_main_only(self, premium_renderer, tmp_path):
        premium_renderer.output_dir = tmp_path

        hot_summary_1 = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/hot",
                        title="US strike on Iran",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Conflict coverage",
                    )
                ],
            ),
            summary="**美军打击伊朗目标**\n\n局势持续升温。",
            perspectives={"Reuters": "US angle"},
            storyline_key="us-iran",
            storyline_name="美伊冲突",
            storyline_role="core",
            storyline_confidence=0.82,
            macro_topic_key="us-iran",
            macro_topic_name="美伊冲突",
            macro_topic_icon_key="war",
        )
        hot_summary_2 = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://bbc.com/hot",
                        title="Hormuz risk rises",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Conflict coverage UK",
                    )
                ],
            ),
            summary="**霍尔木兹风险上升**\n\n英国持续关注中东紧张局势。",
            perspectives={"BBC": "UK angle"},
            storyline_key="us-iran",
            storyline_name="美伊冲突",
            storyline_role="spillover",
            storyline_confidence=0.82,
            macro_topic_key="us-iran",
            macro_topic_name="美伊冲突",
            macro_topic_icon_key="war",
        )
        main_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Tech-General",
                articles=[
                    Article(
                        url="https://reuters.com/main",
                        title="AI market update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="AI market update body",
                    )
                ],
            ),
            summary="**AI市场更新**\n\n常规科技新闻。",
            perspectives={},
        )

        html_path = premium_renderer.render(
            [main_summary],
            datetime.now(tz=timezone.utc).date(),
            hot_topics=[
                {
                    "dom_id": "hot-topic-1",
                    "macro_topic_key": "us-iran",
                    "macro_topic_name": "美伊冲突",
                    "topic_icon_key": "war",
                    "summaries": [hot_summary_1, hot_summary_2],
                }
            ],
        )
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))
        tree = lxml_html.fromstring(html)

        assert "美伊冲突" in html
        assert "热点专题-美伊冲突" not in html
        assert "onclick=\"filterView(this,'hot','hot-topic-1')\"" in html
        assert 'data-hot-target="hot-topic-1"' in html
        assert "美军打击伊朗目标" in html
        assert "霍尔木兹风险上升" in html
        assert "AI市场更新" in html
        assert payload["cluster_count"] == 1
        assert len(payload["clusters"]) == 1
        assert payload["clusters"][0]["headline"] == "AI市场更新"
        assert payload["hot_topic_count"] == 1
        assert payload["hot_topic_story_count"] == 2
        assert payload["hot_topics"][0]["macro_topic_name"] == "美伊冲突"
        assert payload["hot_topics"][0]["storyline_key"] == "us-iran"
        assert payload["hot_topics"][0]["storyline_name"] == "美伊冲突"
        assert payload["hot_topics"][0]["anchor_labels"] == []
        assert payload["hot_topics"][0]["scope_summary"] == "聚焦 1 条核心事件，延伸 1 条直接外溢。"
        assert payload["hot_topics"][0]["preview_clusters"][0]["headline"] == "美军打击伊朗目标"
        assert len(payload["hot_topics"][0]["clusters"]) == 2
        assert payload["hot_topics"][0]["clusters"][0]["storyline_role"] == "core"
        assert payload["hot_topics"][0]["clusters"][1]["storyline_role"] == "spillover"
        assert "今日焦点结构" in html
        assert "进入专题" in html
        assert 'data-hot-stage="true"' in html
        assert 'rel="icon" href="/favicon.ico"' in html
        assert (tmp_path / "favicon.ico").exists()
        assert payload["clusters"][0]["storyline_display_mode"] == "main"
        assert payload["hot_topics"][0]["clusters"][0]["storyline_display_mode"] == "hot_topic"
        assert tree.xpath('//*[@data-hot-stage="true"]//*[@data-hot-id="hot-topic-1"]')
        assert not tree.xpath('//*[@data-all-feed="true"]//*[@data-hot-id="hot-topic-1"]')
        assert tree.xpath('//button[contains(@class, "hot-tab") and @data-hot-target="hot-topic-1"]')
        assert tree.xpath('//button[contains(@class, "overview-cta") and @data-hot-target="hot-topic-1"]')

    def test_premium_template_sanitizes_hot_topic_name_and_icon_fallback(self, premium_renderer, tmp_path):
        premium_renderer.output_dir = tmp_path

        hot_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/hot2",
                        title="Global trade tension",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Trade tension coverage",
                    )
                ],
            ),
            summary="**全球贸易紧张升级**\n\n多国关注关税升级。",
            perspectives={},
            macro_topic_key="global-trade",
            macro_topic_name="热点专题-全球贸易紧张升级",
            macro_topic_icon_key="invalid",
        )

        html_path = premium_renderer.render(
            [],
            datetime.now(tz=timezone.utc).date(),
            hot_topics=[
                {
                    "dom_id": "hot-topic-1",
                    "macro_topic_key": "global-trade",
                    "macro_topic_name": "热点专题-全球贸易紧张升级",
                    "topic_icon_key": "invalid",
                    "summaries": [hot_summary],
                }
            ],
        )
        html = html_path.read_text(encoding="utf-8")

        assert "全球贸易紧张升级" in html
        assert "热点专题-全球贸易紧张升级" not in html
        assert '<span class="hot-icon">🌍</span>' in html

    def test_premium_template_renders_focus_storyline_without_hotspot_tab(self, premium_renderer, tmp_path):
        premium_renderer.output_dir = tmp_path

        focus_summary_1 = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/focus1",
                        title="Trump trip delayed after Iran conflict",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="US reporting",
                    )
                ],
            ),
            summary="**特朗普访华行程因伊朗战争推迟至5月14-15日**\n\n访问计划调整。",
            perspectives={"Reuters": "US angle"},
            storyline_key="trump-china-visit",
            storyline_name="特朗普访华",
            storyline_role="core",
        )
        focus_summary_2 = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://bbc.com/focus2",
                        title="White House confirms Trump China visit timing",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="UK reporting",
                    )
                ],
            ),
            summary="**特朗普将于5月中旬访华，中美领导人将举行会晤**\n\n白宫确认访问时点。",
            perspectives={"BBC": "UK angle"},
            storyline_key="trump-china-visit",
            storyline_name="特朗普访华",
            storyline_role="spillover",
        )
        main_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Tech-General",
                articles=[
                    Article(
                        url="https://reuters.com/main2",
                        title="AI market update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="AI market update body",
                    )
                ],
            ),
            summary="**AI市场更新**\n\n常规科技新闻。",
            perspectives={},
        )

        html_path = premium_renderer.render(
            [main_summary],
            datetime.now(tz=timezone.utc).date(),
            focus_storylines=[
                {
                    "dom_id": "focus-storyline-1",
                    "storyline_key": "trump-china-visit",
                    "storyline_name": "特朗普访华",
                    "topic_icon_key": "globe",
                    "summaries": [focus_summary_1, focus_summary_2],
                }
            ],
        )
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "主线追踪 · 特朗普访华" in html
        assert "今日焦点结构" not in html
        assert "进入专题" not in html
        assert "class=\"cat-tab hot-tab\"" not in html
        assert 'data-all-feed="true"' in html
        assert 'data-focus-storyline="true"' in html
        assert "cat-block" not in html
        assert ".focus-storyline-block {" in html
        assert ".focus-storyline-block .source-chips .src-chip:nth-child(n+4)" in html
        assert ".focus-storyline-block .card-summary {" in html
        assert '<span class="card-index">1.</span>特朗普访华行程因伊朗战争推迟至5月14-15日' in html
        assert '<span class="card-index">2.</span>特朗普将于5月中旬访华，中美领导人将举行会晤' in html
        assert '<span class="card-index">3.</span>AI市场更新' in html
        assert payload["focus_storyline_count"] == 1
        assert payload["focus_storyline_story_count"] == 2
        assert payload["focus_storylines"][0]["storyline_name"] == "特朗普访华"
        assert payload["focus_storylines"][0]["clusters"][0]["storyline_display_mode"] == "focus_storyline"
        assert payload["focus_storylines"][0]["clusters"][0]["display_rank"] == 1
        assert payload["focus_storylines"][0]["clusters"][1]["display_rank"] == 2
        assert payload["clusters"][0]["storyline_display_mode"] == "main"
        assert payload["clusters"][0]["display_rank"] == 3

    def test_premium_mobile_header_only_pins_logo_and_date(self, premium_renderer, tmp_path):
        premium_renderer.output_dir = tmp_path

        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Tech-General",
                articles=[
                    Article(
                        url="https://reuters.com/mobile-header",
                        title="AI market update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="AI market update body",
                    )
                ],
            ),
            summary="**AI市场更新**\n\n常规科技新闻。",
            perspectives={},
        )

        html_path = premium_renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")

        assert "@media (max-width: 640px) {" in html
        assert '<div class="mobile-brand-bar" aria-hidden="true">' in html
        assert ".mobile-brand-bar {\n        position: sticky;" in html
        assert ".site-header {\n        background: transparent;" in html
        assert "position: static;" in html
        assert ".header-brand {\n        display: none;" in html
        assert ".header-stats {\n        flex-wrap: nowrap;" in html
        assert "overflow-x: auto;" in html
        assert ".all-overview {\n        gap: 10px;" in html
        assert ".overview-list {\n        display: grid;" in html
        assert "grid-auto-flow: column;" in html
        assert ".overview-anchors {\n        display: none;" in html
        assert '<div class="header-stats" aria-label="report stats">' in html
        assert '<nav class="cat-tabs">' in html
        assert '<span class="logo">NewsPrism</span>' in html
