"""Tests for renderer searched article attribution."""
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path

from lxml import html as lxml_html
import pytest

from newsprism.config import load_config
from newsprism.runtime.renderer import HtmlRenderer, _REGION_FLAG, _broad_category
from newsprism.types import Article, ArticleCluster, ClusterSummary, PerspectiveGroup


@pytest.fixture
def renderer():
    return HtmlRenderer(
        output_dir="output",
        template_dir="templates",
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

    def test_repeated_source_perspective_links_follow_article_order(self, renderer):
        articles = [
            Article(
                url="https://www.thehindu.com/cuba",
                title="U.S. indicts former Cuban President",
                source_name="The Hindu",
                published_at=datetime.now(tz=timezone.utc),
                content="Cuba indictment body.",
            ),
            Article(
                url="https://www.thehindu.com/oil",
                title="Britain eases sanctions on Russian oil",
                source_name="The Hindu",
                published_at=datetime.now(tz=timezone.utc),
                content="Oil sanctions body.",
            ),
            Article(
                url="https://www.theguardian.com/cuba",
                title="US indicts former Cuban president",
                source_name="The Guardian",
                published_at=datetime.now(tz=timezone.utc),
                content="Guardian Cuba body.",
            ),
            Article(
                url="https://www.theguardian.com/war-powers",
                title="Senate advances Iran war powers resolution",
                source_name="The Guardian",
                published_at=datetime.now(tz=timezone.utc),
                content="Guardian war powers body.",
            ),
        ]
        summary = ClusterSummary(
            cluster=ArticleCluster(topic_category="World News", articles=articles),
            summary="**Merged story**\n\nBody text here.",
            grouped_perspectives=[
                PerspectiveGroup(sources=["The Hindu"], perspective="Cuba indictment angle."),
                PerspectiveGroup(sources=["The Hindu"], perspective="Oil sanctions angle."),
                PerspectiveGroup(sources=["The Guardian"], perspective="Guardian Cuba angle."),
                PerspectiveGroup(sources=["The Guardian"], perspective="Guardian war powers angle."),
            ],
        )

        grouped = renderer._build_grouped_perspectives(summary)

        assert grouped[0]["sources"][0]["url"] == "https://www.thehindu.com/cuba"
        assert grouped[1]["sources"][0]["url"] == "https://www.thehindu.com/oil"
        assert grouped[2]["sources"][0]["url"] == "https://www.theguardian.com/cuba"
        assert grouped[3]["sources"][0]["url"] == "https://www.theguardian.com/war-powers"

    def test_repeated_source_perspective_exhaustion_avoids_wrong_link(self, renderer):
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://www.ithome.com/0/1.htm",
                        title="Google Antigravity update",
                        source_name="IT之家",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Antigravity body.",
                    ),
                    Article(
                        url="https://www.ithome.com/0/2.htm",
                        title="Google Wear OS update",
                        source_name="IT之家",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Wear OS body.",
                    ),
                    Article(
                        url="https://reuters.com/google",
                        title="Google updates AI products",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Reuters body.",
                    ),
                ],
            ),
            summary="**Ambiguous source story**\n\nBody text here.",
            grouped_perspectives=[
                PerspectiveGroup(sources=["IT之家"], perspective="Antigravity perspective."),
                PerspectiveGroup(sources=["IT之家"], perspective="Wear OS perspective."),
                PerspectiveGroup(sources=["IT之家"], perspective="Extra ambiguous perspective."),
            ],
        )

        grouped = renderer._build_grouped_perspectives(summary)

        assert grouped[0]["sources"][0]["url"] == "https://www.ithome.com/0/1.htm"
        assert grouped[1]["sources"][0]["url"] == "https://www.ithome.com/0/2.htm"
        assert grouped[2]["sources"][0]["url"] is None
        assert grouped[2]["sources"][0]["ambiguous_url_count"] == 2

    def test_zaobao_mirror_links_are_labeled_as_mirror(self, renderer):
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://www.zaochenbao.com/news/china/202605/2072227.html",
                        title="俄军人被指在华秘密接受训练后 投入乌克兰战场",
                        source_name="联合早报",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Zaobao mirror body.",
                    ),
                    Article(
                        url="https://reuters.com/china-training",
                        title="China secretly trained Russian soldiers",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Reuters body.",
                    ),
                ],
            ),
            summary="**Story**\n\nBody text here.",
            grouped_perspectives=[
                PerspectiveGroup(sources=["联合早报"], perspective="新加坡中文报道角度。"),
                PerspectiveGroup(sources=["Reuters"], perspective="路透社报道角度。"),
            ],
        )

        grouped = renderer._build_grouped_perspectives(summary)
        zaobao = grouped[0]["sources"][0]

        assert zaobao["url"] == "https://www.zaochenbao.com/news/china/202605/2072227.html"
        assert zaobao["provenance_label"] == "转载镜像"
        assert zaobao["provenance_label_en"] == "Mirror"
        assert grouped[0]["label"] == "联合早报 · 转载镜像"

    def test_zaobao_prefers_configured_rss_over_newsnow_mirror(self):
        cfg = load_config()
        zaobao = next(source for source in cfg.sources if source.name == "联合早报")

        assert zaobao.newsnow_id is None
        assert zaobao.rss_url == "https://www.zaobao.com.sg/rss/china"

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
        assert "<strong>1</strong> 条常规追踪" in html
        assert "&lt;strong&gt;" not in html
        assert "个视角" not in html
        assert "data-lang-choice=\"en\"" not in html
        assert payload["available_languages"] == ["zh"]
        assert payload["clusters"][0]["perspectives_list"] == []
        assert "**Single-source story**" not in html
        assert payload["clusters"][0]["summary"] == "Body text here."

    def test_render_exports_quality_and_storyline_state(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/story",
                        title="Checked story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Checked story body.",
                    )
                ],
            ),
            summary="**Checked story**\n\nBody text here.",
            perspectives={},
            quality_status="publishable",
            quality_score=0.76,
            quality_flags=["single_source"],
            confirmed_claims=["Checked story"],
            evidence_summary="1 source assessed.",
            storyline_state="emerging",
        )

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))
        cluster = payload["clusters"][0]

        assert cluster["quality_status"] == "publishable"
        assert cluster["quality_score"] == 0.76
        assert cluster["confirmed_claims"] == ["Checked story"]
        assert cluster["storyline_state"] == "emerging"
        assert "多源确认" not in html
        assert "Evidence checked" not in html

    def test_render_with_english_content_enables_language_toggle(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/english",
                        title="English-ready story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="English-ready story body.",
                    )
                ],
            ),
            summary="**中文标题**\n\n中文摘要内容。",
            summary_en="**English Headline**\n\nEnglish summary content.",
            short_topic_name="中文专题",
            short_topic_name_en="English Topic",
            perspectives={},
        )

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert 'data-lang-choice="en"' in html
        assert '>汉</button>' in html
        assert '>ENG</button>' in html
        assert "English summary content." in html
        assert "localStorage.setItem('newsprism-language', lang)" in html
        assert payload["available_languages"] == ["zh", "en"]
        assert payload["default_language"] == "zh"
        assert payload["clusters"][0]["headline_en"] == "English Headline"
        assert "English summary content." in payload["clusters"][0]["summary_en"]

    def test_render_includes_duplicate_audit_metadata(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/duplicate-audit",
                        title="Trump China visit",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Story body.",
                    )
                ],
            ),
            summary="**特朗普访华安排**\n\n摘要内容。",
            perspectives={},
        )
        summary.event_signature = {"entities": ["特朗普", "中国"], "actions": ["visit"], "times": [], "contexts": ["trump-china-visit"]}
        summary.duplicate_action = "merged"
        summary.duplicate_reason = "shared_context:trump-china-visit"
        summary.duplicate_confidence = 0.88

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert payload["clusters"][0]["event_signature"]["contexts"] == ["trump-china-visit"]
        assert payload["clusters"][0]["duplicate_action"] == "merged"
        assert payload["clusters"][0]["duplicate_reason"] == "shared_context:trump-china-visit"
        assert payload["clusters"][0]["duplicate_confidence"] == 0.88

    def test_render_does_not_show_routine_multi_source_confirmation_preview(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/one",
                        title="One angle",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="One angle body.",
                    ),
                    Article(
                        url="https://bbc.com/one",
                        title="One angle confirmed",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="One angle confirmed body.",
                    ),
                ],
            ),
            summary="**One angle story**\n\nBody text here.",
            perspectives={
                "Reuters": "Both sources report the same core fact.",
                "BBC": "Both sources report the same core fact.",
            },
            grouped_perspectives=[
                PerspectiveGroup(sources=["Reuters", "BBC"], perspective="Both sources report the same core fact."),
            ],
            quality_status="publishable",
        )

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "多源确认：" not in html
        assert "Multi-source confirmation" not in html
        assert "视角差异：" not in html
        assert payload["clusters"][0]["distinct_perspective_count"] == 1
        assert payload["clusters"][0]["source_confirmation_preview"] == ""
        assert payload["clusters"][0]["source_confirmation_preview_en"] == ""

    def test_render_does_not_confirm_sources_when_quality_needs_review(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.example/story",
                        title="Same fact disputed",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Same fact body.",
                    ),
                    Article(
                        url="https://bbc.example/story",
                        title="Same fact disputed",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Same fact body.",
                    ),
                ],
            ),
            summary="**Same fact disputed**\n\nBody text here.",
            perspectives={
                "Reuters": "Both sources report the same framing.",
                "BBC": "Both sources report the same framing.",
            },
            grouped_perspectives=[
                PerspectiveGroup(
                    sources=["Reuters", "BBC"],
                    perspective="Both sources report the same framing.",
                ),
            ],
            quality_status="needs_review",
            quality_score=0.41,
            quality_flags=["summary_claim_gap"],
        )

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "存在争议" in html
        assert "Disputed" in html
        assert "Needs review" not in html
        assert "多源确认：" not in html
        assert payload["clusters"][0]["source_confirmation_preview"] == ""

    def test_render_keeps_needs_evidence_status_public(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.example/evidence",
                        title="Needs evidence",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Needs evidence body.",
                    )
                ],
            ),
            summary="**Needs evidence story**\n\nBody text here.",
            perspectives={},
            quality_status="seek_more_evidence",
            quality_score=0.36,
        )

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")

        assert "待补证据" in html
        assert "Needs evidence" in html

    def test_focus_storyline_input_is_ignored_in_public_report(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        focus_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Business & Finance",
                articles=[
                    Article(
                        url="https://example.com/spacex-ipo",
                        title="SpaceX 招股书披露：IPO 后马斯克将拥有 79% 投票控制权",
                        source_name="IT之家",
                        published_at=datetime.now(tz=timezone.utc),
                        content="SpaceX IPO control story.",
                    )
                ],
            ),
            summary="**SpaceX IPO招股书披露马斯克将保留绝对控制权**\n\n摘要内容。",
            short_topic_name="SpaceX上市控制权",
            storyline_name="SpaceX招股书披",
        )
        main_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Tech-General",
                articles=[
                    Article(
                        url="https://example.com/ai-market",
                        title="AI market update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="AI market update body.",
                    )
                ],
            ),
            summary="**AI市场更新**\n\n常规科技新闻。",
            perspectives={},
        )

        html_path = renderer.render(
            [main_summary],
            datetime.now(tz=timezone.utc).date(),
            focus_storylines=[
                {
                    "storyline_key": "single-33",
                    "storyline_name": "SpaceX招股书披",
                    "topic_icon_key": "globe",
                    "member_count": 2,
                    "summaries": [focus_summary],
                }
            ],
        )
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "focus_storylines" not in payload
        assert payload["focus_storyline_count"] == 0
        assert payload["focus_storyline_story_count"] == 0
        assert payload["total_cluster_count"] == 1
        assert 'data-focus-storyline="true"' not in html
        assert "SpaceX IPO招股书披露马斯克将保留绝对控制权" not in html
        assert "主线追踪 · SpaceX上市控制权" not in html
        assert "主线追踪 · SpaceX招股书披" not in html
        assert "AI市场更新" in html

    def test_single_source_renders_source_row_with_link(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
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

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "Reuters" in html
        assert "查看 1 个视角" not in html
        assert 'href="https://reuters.com/single"' in html
        assert payload["clusters"][0]["source_groups"][0]["url"] == "https://reuters.com/single"
        assert payload["clusters"][0]["has_expandable_perspectives"] is False

    def test_multi_source_renders_grouped_perspectives(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
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

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "查看 2 个视角" in html
        assert "Reuters" in html
        assert "BBC" in html
        assert "联合早报" in html
        assert "视角差异：" in html
        assert "persp-sep" not in html
        assert 'class="persp-angle" title="英美媒体共同聚焦关税对西方市场与政策预期的冲击。"' in html
        assert "英美媒体共同聚焦关税对西方市场与政策预期的冲击。" in html
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

    def test_invalid_perspective_groups_are_suppressed(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
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

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
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

    def test_staged_render_skips_latest_symlink_update(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        previous_date = "2026-03-26"
        (tmp_path / previous_date).mkdir()
        (tmp_path / "latest").symlink_to(previous_date)
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/staged",
                        title="Staged story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Staged body.",
                    )
                ],
            ),
            summary="**Staged story**\n\nBody text here.",
            perspectives={},
        )

        html_path = renderer.render(
            [summary],
            datetime(2026, 3, 27, tzinfo=timezone.utc).date(),
            report_subdir="staging",
            update_latest=False,
        )

        assert html_path == tmp_path / "staging" / "2026-03-27" / "index.html"
        assert os.readlink(tmp_path / "latest") == previous_date


class TestRegionFlagMapping:
    def test_common_regions_have_flags(self):
        assert _REGION_FLAG["cn"] == "🇨🇳"
        assert _REGION_FLAG["us"] == "🇺🇸"
        assert _REGION_FLAG["jp"] == "🇯🇵"
        assert _REGION_FLAG["kr"] == "🇰🇷"
        assert _REGION_FLAG["ru"] == "🇷🇺"
        assert _REGION_FLAG["de"] == "🇩🇪"
        assert _REGION_FLAG["gb"] == "🇬🇧"


class TestHotTopics:
    def test_template_renders_macro_topic_family_tabs_and_keeps_json_main_only(self, renderer, tmp_path):
        renderer.output_dir = tmp_path

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

        html_path = renderer.render(
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
        assert payload["hot_topics"][0]["scope_summary"] == "收纳 2 条相关报道，其中 1 条为核心事件。"
        assert "直接外溢" not in html
        assert "direct spillover" not in html
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

    def test_template_sanitizes_hot_topic_name_and_icon_fallback(self, renderer, tmp_path):
        renderer.output_dir = tmp_path

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

        html_path = renderer.render(
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
        assert '<span class="hot-icon"><span class="emoji" aria-hidden="true">🌍</span></span>' in html

    def test_template_repairs_invalid_hot_topic_name_from_current_family(self, renderer, tmp_path):
        renderer.output_dir = tmp_path

        hot_summary_1 = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://example.com/zelensky",
                        title="Zelenskyy warns of impending massive Russian attack on Ukraine",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Russia Ukraine military escalation body.",
                    )
                ],
            ),
            summary="**泽连斯基警告俄罗斯即将发动大规模攻击**\n\n俄军袭击扩大，乌方打击俄境内油库。",
            perspectives={},
            short_topic_name="俄军大规模攻击预警",
            macro_topic_name="ロシア最大級の製油所",
            macro_topic_name_en="Russia's largest refinery",
            storyline_name="ロシア最大級の製油所",
        )
        hot_summary_2 = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://example.com/belarus",
                        title="Zelensky demands Belarus remove border drone repeaters",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Ukraine Belarus drone escalation body.",
                    )
                ],
            ),
            summary="**泽连斯基要求卢卡申科一周内拆除边境设备**\n\n乌克兰警告白俄罗斯移除俄军无人机中继设备。",
            perspectives={},
            short_topic_name="乌要求白俄拆设备",
            macro_topic_name="ロシア最大級の製油所",
            macro_topic_name_en="Russia's largest refinery",
            storyline_name="ロシア最大級の製油所",
        )
        hot_summary_3 = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://example.com/zaporizhzhia",
                        title="Russian attack on Zaporizhzhia kills civilians as Ukraine strikes Crimea",
                        source_name="The Hindu",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Russia Ukraine strikes body.",
                    )
                ],
            ),
            summary="**俄军袭击扎波罗热致5死，乌方打击克里米亚**\n\n俄乌互袭升级。",
            perspectives={},
            short_topic_name="俄乌互袭升级",
            macro_topic_name="ロシア最大級の製油所",
            macro_topic_name_en="Russia's largest refinery",
            storyline_name="ロシア最大級の製油所",
        )

        html_path = renderer.render(
            [],
            datetime.now(tz=timezone.utc).date(),
            hot_topics=[
                {
                    "dom_id": "hot-topic-1",
                    "macro_topic_key": "single-36",
                    "macro_topic_name": "ロシア最大級の製油所",
                    "macro_topic_name_en": "Russia's largest refinery",
                    "storyline_name": "ロシア最大級の製油所",
                    "topic_icon_key": "war",
                    "summaries": [hot_summary_1, hot_summary_2, hot_summary_3],
                }
            ],
        )
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert payload["hot_topics"][0]["macro_topic_name"] == "俄乌军事升级"
        assert payload["hot_topics"][0]["macro_topic_name_en"] == "Russia-Ukraine military escalation"
        assert "ロシア最大級の製油所" not in html
        assert "Russia&#39;s largest refinery" not in html
        assert '<span data-lang-zh>俄乌军事升级</span><span data-lang-en>Russia-Ukraine military escalation</span>' in html

    def test_template_wraps_emoji_with_fallback_spans(self, renderer, tmp_path):
        renderer.output_dir = tmp_path

        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/organic",
                        title="US policy response",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="US organic angle.",
                    ),
                    Article(
                        url="https://example.com/searched",
                        title="Japan policy response",
                        source_name="Unknown Source",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Japanese searched angle.",
                        is_searched=True,
                        search_region="jp",
                        origin_region="jp",
                    )
                ],
            ),
            summary="**日本政策回应升级**\n\n日本方面继续评估地区影响。",
            perspectives={
                "Reuters": "US organic angle",
                "Unknown Source": "Japanese searched angle",
            },
            grouped_perspectives=[
                PerspectiveGroup(
                    sources=["Reuters"],
                    perspective="US organic angle",
                ),
                PerspectiveGroup(
                    sources=["Unknown Source"],
                    perspective="Japanese searched angle",
                )
            ],
        )

        hot_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/hot-emoji",
                        title="Hormuz risk rises",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Conflict coverage",
                    )
                ],
            ),
            summary="**霍尔木兹风险上升**\n\n英国持续关注中东紧张局势。",
            perspectives={"Reuters": "UK angle"},
            macro_topic_key="middle-east",
            macro_topic_name="中东局势",
            macro_topic_icon_key="war",
        )

        html_path = renderer.render(
            [summary],
            datetime.now(tz=timezone.utc).date(),
            hot_topics=[
                {
                    "dom_id": "hot-topic-1",
                    "macro_topic_key": "middle-east",
                    "macro_topic_name": "中东局势",
                    "topic_icon_key": "war",
                    "summaries": [hot_summary],
                }
            ],
        )
        html = html_path.read_text(encoding="utf-8")
        tree = lxml_html.fromstring(html)

        assert "--font-sans: 'Outfit', 'Noto Sans SC', 'Noto Sans', 'PingFang SC', 'Microsoft YaHei', 'Segoe UI', Arial, sans-serif;" in html
        assert "--font-emoji: 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol', 'Noto Color Emoji', 'Twemoji Mozilla', sans-serif;" in html
        assert "body {\n      font-family: var(--font-sans);" in html
        assert ".emoji {\n      display: inline-block;" in html
        assert "font-variant-emoji: emoji;" in html
        assert tree.xpath('//button[contains(@class, "cat-tab") and not(contains(@class, "hot-tab"))]//span[@class="emoji" and text()="🌍"]')
        assert tree.xpath('//button[contains(@class, "hot-tab")]//span[@class="emoji" and text()="⚠️"]')
        assert tree.xpath('//div[contains(@class, "overview-title")]//span[@class="emoji" and text()="⚠️"]')
        assert tree.xpath('//a[contains(@class, "src-chip-link")]//span[@class="emoji" and text()="🇯🇵"]')
        assert tree.xpath('//a[contains(@class, "src-chip-link")]//span[@class="emoji" and text()="🔍"]')
        assert tree.xpath('//*[contains(@class, "persp-src")]//span[@class="emoji" and text()="🇯🇵"]')
        assert tree.xpath('//*[contains(@class, "persp-src")]//span[@class="emoji" and text()="🔍"]')
        assert not tree.xpath('//span[@class="emoji" and contains(text(), "搜索补充")]')

    def test_template_does_not_render_focus_storyline_section(self, renderer, tmp_path):
        renderer.output_dir = tmp_path

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

        html_path = renderer.render(
            [main_summary],
            datetime.now(tz=timezone.utc).date(),
            focus_storylines=[
                {
                    "dom_id": "compat-line-1",
                    "storyline_key": "trump-china-visit",
                    "storyline_name": "特朗普访华",
                    "topic_icon_key": "globe",
                    "summaries": [focus_summary_1, focus_summary_2],
                }
            ],
        )
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert "主线追踪 · 特朗普访华" not in html
        assert "今日焦点结构" not in html
        assert "进入专题" not in html
        assert "class=\"cat-tab hot-tab\"" not in html
        assert 'data-all-feed="true"' in html
        assert 'data-focus-storyline="true"' not in html
        assert "cat-block" not in html
        assert ".focus-storyline-block {" not in html
        assert ".focus-storyline-block .source-chips .src-chip:nth-child(n+4)" not in html
        assert ".focus-storyline-block .card-summary {" not in html
        assert "特朗普访华行程因伊朗战争推迟至5月14-15日" not in html
        assert "特朗普将于5月中旬访华，中美领导人将举行会晤" not in html
        assert "AI市场更新" in html
        assert "focus_storylines" not in payload
        assert payload["focus_storyline_count"] == 0
        assert payload["focus_storyline_story_count"] == 0
        assert payload["clusters"][0]["storyline_display_mode"] == "main"
        assert payload["clusters"][0]["display_rank"] == 1

    def test_renderer_uses_public_category_vocabulary_and_locale_labels(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        legacy_sports = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/sports",
                        title="Global sports event draws public attention",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Sports body.",
                    )
                ],
            ),
            summary="**全球赛事引发关注**\n\n赛事带来广泛讨论。",
            perspectives={},
        )
        legacy_sports.display_category = "体育运动"
        canonical_world = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World",
                articles=[
                    Article(
                        url="https://reuters.com/world",
                        title="World policy update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="World body.",
                    )
                ],
            ),
            summary="**国际政策更新**\n\n国际政策继续调整。",
            perspectives={},
        )
        canonical_business = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Business",
                articles=[
                    Article(
                        url="https://reuters.com/business",
                        title="Business market update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Business body.",
                    )
                ],
            ),
            summary="**商业市场更新**\n\n市场继续调整。",
            perspectives={},
        )
        canonical_tech = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Technology",
                articles=[
                    Article(
                        url="https://reuters.com/tech",
                        title="Technology update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Tech body.",
                    )
                ],
            ),
            summary="**科技进展更新**\n\n科技公司发布进展。",
            perspectives={},
        )
        canonical_science = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Science & Health",
                articles=[
                    Article(
                        url="https://reuters.com/science",
                        title="Science and health update",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Science body.",
                    )
                ],
            ),
            summary="**科学健康更新**\n\n研究继续推进。",
            perspectives={},
        )

        html_path = renderer.render(
            [legacy_sports, canonical_world, canonical_business, canonical_tech, canonical_science],
            datetime.now(tz=timezone.utc).date(),
        )
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert _broad_category("World News", "体育运动") == "Culture & Sports"
        assert payload["clusters"][0]["broad_category"] == "Culture & Sports"
        assert payload["clusters"][0]["broad_category_en"] == "Culture & Sports"
        assert '<span data-lang-zh>国际</span><span data-lang-en>World</span>' in html
        assert '<span data-lang-zh>商业</span><span data-lang-en>Business</span>' in html
        assert '<span data-lang-zh>科技</span><span data-lang-en>Technology</span>' in html
        assert '<span data-lang-zh>科学健康</span><span data-lang-en>Science &amp; Health</span>' in html
        assert '<span data-lang-zh>文化体育</span><span data-lang-en>Culture &amp; Sports</span>' in html
        assert "filterView(this,'category','Culture &amp; Sports')" in html
        assert "体育运动" not in html

    def test_mobile_header_only_pins_logo_and_date(self, renderer, tmp_path):
        renderer.output_dir = tmp_path

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

        html_path = renderer.render([summary], datetime.now(tz=timezone.utc).date())
        html = html_path.read_text(encoding="utf-8")

        assert "@media (max-width: 640px) {" in html
        assert '<div class="mobile-brand-bar">' in html
        assert ".mobile-brand-bar {\n        position: sticky;" in html
        assert ".site-header {\n        background: transparent;" in html
        assert "position: static;" in html
        assert ".header-brand {\n        display: none;" in html
        assert ".header-tools {\n        justify-content: flex-start;" in html
        assert "overflow-x: auto;" in html
        assert ".all-overview {\n        gap: 10px;" in html
        assert ".overview-list {\n        display: grid;" in html
        assert "grid-auto-flow: column;" in html
        assert ".overview-anchors {\n        display: none;" in html
        assert '<div class="footer-stats" aria-label="report stats">' in html
        assert '<nav class="cat-tabs">' in html
        assert '<a class="logo" href="./" aria-label="Refresh NewsPrism" title="Refresh NewsPrism" onclick="window.location.reload(); return false;">NewsPrism</a>' in html
        assert 'data-theme-choice="system" aria-label="System theme" title="System theme" onclick="setTheme(\'system\')">🖥</button>' in html
        assert 'data-theme-choice="light" aria-label="Light theme" title="Light theme" onclick="setTheme(\'light\')">☀️</button>' in html
        assert 'data-theme-choice="dark" aria-label="Dark theme" title="Dark theme" onclick="setTheme(\'dark\')">🌙</button>' in html
        assert 'class="back-to-top" onclick="scrollToTop()"' in html
        tree = lxml_html.fromstring(html)
        assert not tree.xpath('//*[@class="site-header"]//*[@aria-label="report stats"]')
        assert tree.xpath('//footer//*[@aria-label="report stats"]')
        assert len(tree.xpath('//a[@class="logo" and @href="./" and contains(@onclick, "window.location.reload")]')) == 2
        assert not tree.xpath('//*[@class="site-header"]//*[@aria-label="report day selector"]')
        assert tree.xpath('//footer//*[@aria-label="report day selector"]')
        assert tree.xpath('//footer//button[contains(@class, "back-to-top")]')
        assert not tree.xpath('//button[contains(@class, "positive-tab")]')

    def test_positive_section_renders_after_main_feed_before_focus_overview(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        main_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Tech-General",
                articles=[
                    Article(
                        url="https://reuters.com/main-positive",
                        title="Main story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Main story body.",
                    )
                ],
            ),
            summary="**常规追踪故事**\n\n常规追踪内容。",
            perspectives={},
        )
        positive_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Culture",
                articles=[
                    Article(
                        url="https://bbc.com/fun",
                        title="Museum opens a playful exhibition",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Culture body.",
                    ),
                    Article(
                        url="https://bbc.com/fun-duplicate",
                        title="Museum opens a playful exhibition update",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Culture body update.",
                    ),
                ],
            ),
            summary="**博物馆开放趣味新展**\n\n新展面向家庭观众，互动项目轻松有趣。",
            perspectives={},
        )
        positive_summary.positive_energy_reason = "轻松文化好消息"
        positive_summary.positive_energy_reason_en = "Light culture"
        positive_summary.positive_energy_score = 0.91
        positive_summary.positive_energy_category = "craft_life"
        positive_summary.positive_energy_source = "BBC"
        hot_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/hot-focus",
                        title="Hot topic",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Hot topic body.",
                    )
                ],
            ),
            summary="**热点专题故事**\n\n热点专题内容。",
            perspectives={},
            storyline_role="core",
        )

        html_path = renderer.render(
            [main_summary],
            date(2026, 3, 27),
            hot_topics=[
                {
                    "dom_id": "hot-topic-1",
                    "macro_topic_key": "focus",
                    "macro_topic_name": "今日焦点",
                    "topic_icon_key": "globe",
                    "summaries": [hot_summary],
                }
            ],
            positive_summaries=[positive_summary],
        )
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))
        tree = lxml_html.fromstring(html)

        all_feed = tree.xpath('//*[@data-all-feed="true"]')[0]
        positive_section = tree.xpath('//*[@data-positive-energy="true"]')[0]
        focus_overview = tree.xpath('//*[@data-all-overview="true"]')[0]

        assert focus_overview.sourceline < all_feed.sourceline < positive_section.sourceline
        assert "今日好消息" in html
        assert "Good News" in html
        assert "今日正能量" not in html
        assert "Positive highlights" not in html
        assert payload["positive_story_count"] == 1
        assert payload["cluster_count"] == 1
        assert payload["total_cluster_count"] == 3
        assert payload["clusters"][0]["headline"] == "常规追踪故事"
        assert payload["positive_stories"][0]["headline"] == "博物馆开放趣味新展"
        assert payload["positive_stories"][0]["positive_reason"] == "轻松文化好消息"
        assert payload["positive_stories"][0]["positive_reason_en"] == "Light culture"
        assert payload["positive_stories"][0]["positive_score"] == 0.91
        assert payload["positive_stories"][0]["positive_category"] == "craft_life"
        assert payload["positive_stories"][0]["positive_source"] == "BBC"
        assert tree.xpath('//button[contains(@class, "positive-tab") and contains(@onclick, "positive")]')
        assert not tree.xpath('//*[@data-main-feed-card and contains(., "博物馆开放趣味新展")]')
        assert len(tree.xpath('//*[@id="positive-1"]//a[@href="https://bbc.com/fun-duplicate"]')) == 1
        assert len(tree.xpath('//*[@id="positive-1"]//a[contains(@class, "src-chip-link")]')) == 1

    def test_positive_only_source_language_content_does_not_enable_language_toggle(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        positive_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Positive Energy",
                articles=[
                    Article(
                        url="https://bbc.com/puppy",
                        title="Adorable puppy rescued by volunteers",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Adorable puppy rescued by volunteers and reunited with a family.",
                    )
                ],
            ),
            summary="**Adorable puppy rescued by volunteers**\n\nAdorable puppy rescued by volunteers and reunited with a family.",
            summary_en="**Adorable puppy rescued by volunteers**\n\nAdorable puppy rescued by volunteers and reunited with a family.",
            perspectives={},
        )
        positive_summary.positive_energy_reason = "可爱治愈"
        positive_summary.positive_energy_reason_en = "Cute"
        positive_summary.positive_energy_score = 0.92

        html_path = renderer.render([], date(2026, 5, 8), positive_summaries=[positive_summary])
        html = html_path.read_text(encoding="utf-8")
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))
        tree = lxml_html.fromstring(html)

        assert payload["english_available"] is False
        assert payload["available_languages"] == ["zh"]
        assert not tree.xpath('//*[contains(@class, "language-toggle")]')
        assert payload["positive_stories"][0]["positive_reason_en"] == "Cute"
        assert "bilingual_text(c.positive_reason, c.positive_reason)" not in html

    def test_positive_only_normalized_bilingual_content_keeps_language_toggle(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        positive_summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="Positive Energy",
                articles=[
                    Article(
                        url="https://bbc.com/puppy",
                        title="Adorable puppy rescued by volunteers",
                        source_name="BBC",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Adorable puppy rescued by volunteers and reunited with a family.",
                    )
                ],
            ),
            summary="**志愿者救助可爱小狗**\n\n志愿者救下一只可爱小狗，并帮助它与新家庭团聚。",
            summary_en="**Adorable puppy rescued by volunteers**\n\nAdorable puppy rescued by volunteers and reunited with a family.",
            perspectives={},
        )
        positive_summary.positive_energy_reason = "可爱治愈"
        positive_summary.positive_energy_reason_en = "Cute"
        positive_summary.positive_energy_score = 0.92

        html_path = renderer.render([], date(2026, 5, 8), positive_summaries=[positive_summary])
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))
        tree = lxml_html.fromstring(html_path.read_text(encoding="utf-8"))

        assert payload["english_available"] is True
        assert payload["available_languages"] == ["zh", "en"]
        assert tree.xpath('//*[contains(@class, "language-toggle")]')
        assert payload["positive_stories"][0]["headline"] == "志愿者救助可爱小狗"

    def test_day_selector_marks_current_and_missing_days(self, renderer, tmp_path):
        renderer.output_dir = tmp_path
        previous = tmp_path / "2026-03-26"
        previous.mkdir()
        (previous / "index.html").write_text("previous", encoding="utf-8")
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/day-selector",
                        title="Day selector story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Day selector body.",
                    )
                ],
            ),
            summary="**日期选择故事**\n\n内容。",
            perspectives={},
        )

        html_path = renderer.render([summary], date(2026, 3, 27))
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))
        tree = lxml_html.fromstring(html_path.read_text(encoding="utf-8"))

        # Past days only — today no longer self-links.
        assert [day["date"] for day in payload["day_links"]] == ["2026-03-26", "2026-03-25", "2026-03-24"]
        assert all(day["active"] is False for day in payload["day_links"])
        assert payload["day_links"][0]["available"] is True
        assert payload["day_links"][0]["href"] == "/p/1/"
        assert payload["day_links"][1]["available"] is False
        assert payload["day_links"][1]["href"] is None
        assert payload["day_links"][2]["available"] is False

        # Footer holds the selector; HTML never leaks the YYYY-MM-DD path.
        assert not tree.xpath('//*[@class="header-tools"]//*[@aria-label="report day selector"]')
        assert tree.xpath('//footer//*[@aria-label="report day selector"]')
        assert tree.xpath('//a[contains(@class, "day-link") and @href="/p/1/"]')
        assert tree.xpath('//a[contains(@class, "day-link") and @href="/p/1/"]//*[contains(@class, "date-part") and contains(., "03月26日")]')
        assert tree.xpath('//span[contains(@class, "day-link") and contains(@class, "disabled")]')
        assert not tree.xpath('//a[contains(@href, "2026-")]/@href[contains(., "../")]')

        # Symlink rotation: /p/1 must point to the existing past day, /p/2 must be skipped.
        link_one = tmp_path / "p" / "1"
        assert link_one.is_symlink()
        import os
        assert os.readlink(str(link_one)) == "../2026-03-26"
        assert not (tmp_path / "p" / "2").exists()

    def test_day_selector_uses_production_dir_when_rendering_to_staging(self, renderer, tmp_path):
        # Regression: when render() is invoked with report_subdir (staging),
        # past-day availability must be probed against the production output
        # dir, not the staging subdir.
        renderer.output_dir = tmp_path
        previous = tmp_path / "2026-03-26"
        previous.mkdir()
        (previous / "index.html").write_text("previous", encoding="utf-8")
        summary = ClusterSummary(
            cluster=ArticleCluster(
                topic_category="World News",
                articles=[
                    Article(
                        url="https://reuters.com/staging-day-selector",
                        title="Staging day selector story",
                        source_name="Reuters",
                        published_at=datetime.now(tz=timezone.utc),
                        content="Staging body.",
                    )
                ],
            ),
            summary="**暂存日期选择故事**\n\n内容。",
            perspectives={},
        )

        html_path = renderer.render(
            [summary],
            date(2026, 3, 27),
            report_subdir="staging",
            update_latest=False,
        )
        payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

        assert payload["day_links"][0]["date"] == "2026-03-26"
        assert payload["day_links"][0]["available"] is True
        assert payload["day_links"][0]["href"] == "/p/1/"


def test_render_copies_fonts_to_output(tmp_path):
    """Renderer copies static/fonts/ to output/fonts/ on render."""
    static_fonts = Path(__file__).resolve().parent.parent / "newsprism" / "static" / "fonts"
    if not (static_fonts / "fonts.css").exists():
        pytest.skip("Font files not downloaded yet — run scripts/download_fonts.py")

    renderer = HtmlRenderer(output_dir=str(tmp_path), template_dir="templates")
    summary = ClusterSummary(
        cluster=ArticleCluster(
            topic_category="World News",
            articles=[
                Article(
                    url="https://example.com/font-test",
                    title="Font copy test",
                    source_name="BBC",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Font copy test body.",
                )
            ],
        ),
        summary="**Font copy test**\n\nBody.",
        perspectives={},
    )
    renderer.render([summary], date(2026, 5, 19), update_latest=False)

    output_fonts = tmp_path / "fonts"
    assert output_fonts.is_dir()
    assert (output_fonts / "fonts.css").exists()
    woff2_files = list(output_fonts.glob("*.woff2"))
    assert len(woff2_files) > 0


def test_render_seeker_placeholder_appears_with_flag_and_reason_tooltip(renderer, tmp_path):
    """Issue #1: when a regional perspective search fails, the reader sees a
    flat inline ⚠️ placeholder with the country flag + failure reason tooltip
    instead of silent absence.
    """
    renderer.output_dir = tmp_path
    organic = Article(
        url="https://reuters.com/event",
        title="Major event headline",
        source_name="Reuters",
        published_at=datetime.now(tz=timezone.utc),
        content="Major event body.",
        origin_region="us",
    )
    placeholder = Article(
        url="placeholder:fr:event-key",
        title="待补充：France视角",
        source_name="[France视角待补]",
        published_at=datetime.now(tz=timezone.utc),
        content="",
        is_searched=True,
        search_region="fr",
        origin_region="fr",
        searched_provider="tavily_search",
    )
    placeholder.is_placeholder = True
    placeholder.search_acceptance_status = "failed"
    placeholder.search_acceptance_reason = "http_401"
    cluster = ArticleCluster(topic_category="World News", articles=[organic, placeholder])
    summary = ClusterSummary(
        cluster=cluster,
        summary="**Major event headline**\n\nBody.",
        perspectives={},
    )

    html_path = renderer.render([summary], date(2026, 7, 21), update_latest=False)
    html = html_path.read_text(encoding="utf-8")
    payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

    # Placeholder is in the article list with the right shape.
    articles = payload["clusters"][0]["articles"]
    placeholder_rows = [a for a in articles if a.get("is_placeholder")]
    assert len(placeholder_rows) == 1
    assert placeholder_rows[0]["search_acceptance_status"] == "failed"
    assert placeholder_rows[0]["search_acceptance_reason"] == "http_401"

    # Rendered HTML includes the placeholder source name + the 🇫🇷 flag +
    # the ⚠️ marker + the failure-detail label, flat (no card/lift).
    assert "[France视角待补]" in html
    assert "🇫🇷" in html
    assert "⚠️" in html
    assert "鉴权失败" in html  # bilingual short label from _placeholder_failure_label


def test_render_shared_storyline_tag_for_main_lane_same_key_clusters(renderer, tmp_path):
    """Issue #2 rec #4 fallback: when 2+ main-lane clusters share a
    storyline_key (they didn't claim a tab because of max_topic_tabs), each
    card gets a flat shared-storyline tag above its title so the reader can
    see the connection.
    """
    renderer.output_dir = tmp_path
    cluster_a = ArticleCluster(
        topic_category="World News",
        articles=[Article(
            url="https://reuters.com/a",
            title="Conflict event A",
            source_name="Reuters",
            published_at=datetime.now(tz=timezone.utc),
            content="A body.",
        )],
    )
    cluster_a.storyline_key = "russia-ukraine-abcd1234"
    cluster_a.storyline_name = "俄乌冲突"
    cluster_a.macro_topic_key = "russia-ukraine-abcd1234"
    cluster_a.macro_topic_name = "俄乌冲突"
    cluster_b = ArticleCluster(
        topic_category="World News",
        articles=[Article(
            url="https://reuters.com/b",
            title="Conflict event B",
            source_name="BBC",
            published_at=datetime.now(tz=timezone.utc),
            content="B body.",
        )],
    )
    cluster_b.storyline_key = "russia-ukraine-abcd1234"
    cluster_b.storyline_name = "俄乌冲突"
    cluster_b.macro_topic_key = "russia-ukraine-abcd1234"
    cluster_b.macro_topic_name = "俄乌冲突"

    summaries = [
        ClusterSummary(cluster=cluster_a, summary="**A**\n\nBody.", perspectives={}),
        ClusterSummary(cluster=cluster_b, summary="**B**\n\nBody.", perspectives={}),
    ]
    # The renderer reads summary.storyline_key/name (set by editorial_planner
    # from cluster attributes); mirror that here.
    for s in summaries:
        s.storyline_key = s.cluster.storyline_key
        s.storyline_name = s.cluster.storyline_name
        s.macro_topic_key = s.cluster.macro_topic_key
        s.macro_topic_name = s.cluster.macro_topic_name

    html_path = renderer.render(summaries, date(2026, 7, 21), update_latest=False)
    html = html_path.read_text(encoding="utf-8")
    payload = json.loads((html_path.parent / "data.json").read_text(encoding="utf-8"))

    # Both clusters carry the shared label.
    for c in payload["clusters"]:
        assert c["shared_storyline_label"] == "俄乌冲突"
    # The tag is rendered exactly twice in the HTML (once per main-lane card).
    assert html.count('class="shared-storyline-tag"') == 2


def test_render_does_not_add_shared_tag_when_only_one_cluster_per_key(renderer, tmp_path):
    """Sanity: a single-member storyline in the main lane does NOT get a tag."""
    renderer.output_dir = tmp_path
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[Article(
            url="https://reuters.com/solo",
            title="Solo event",
            source_name="Reuters",
            published_at=datetime.now(tz=timezone.utc),
            content="Solo body.",
        )],
    )
    cluster.storyline_key = "russia-ukraine-abcd1234"
    cluster.storyline_name = "俄乌冲突"
    cluster.macro_topic_key = "russia-ukraine-abcd1234"
    cluster.macro_topic_name = "俄乌冲突"
    summary = ClusterSummary(cluster=cluster, summary="**Solo**\n\nBody.", perspectives={})

    html_path = renderer.render([summary], date(2026, 7, 21), update_latest=False)
    html = html_path.read_text(encoding="utf-8")
    assert 'class="shared-storyline-tag"' not in html
