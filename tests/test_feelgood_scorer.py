from datetime import datetime, timezone

from newsprism.config import Config, SourceConfig
from newsprism.service.feelgood_scorer import FeelgoodScorer
from newsprism.types import Article


def _source(name: str, language: str = "en", weight: float = 1.0) -> SourceConfig:
    return SourceConfig(
        name=name,
        name_en=name,
        url=f"https://{name.lower().replace(' ', '')}.example",
        rss_url=None,
        type="rss",
        weight=weight,
        language=language,
        region="us",
    )


def _cfg() -> Config:
    return Config(
        raw={},
        sources=[
            _source("Animal Feed", weight=1.2),
            _source("Sports Feed"),
            _source("Science Feed"),
            _source("City Feed"),
            _source("中文源", language="zh"),
        ],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={"positive_energy": {"local_min_score": 0.42, "source_diversity": True}},
        active_search={},
        feelgood_keywords={
            "themes": {
                "cute": {"weight": 0.22, "keywords": ["cute", "adorable", "puppy", "熊猫", "可爱", "治愈"]},
                "heartwarming": {"weight": 0.24, "keywords": ["heartwarming", "reunion", "kindness", "暖心", "团圆"]},
                "sports_moment": {"weight": 0.14, "keywords": ["comeback", "fan", "mascot", "绝杀"]},
                "science_wonder": {"weight": 0.16, "keywords": ["NASA", "space", "discovery", "太空"]},
            },
            "entity_weights": {
                "animal": 0.18,
                "sports": 0.08,
                "science_org": 0.08,
                "city_community": 0.08,
            },
            "blockers": {
                "keywords": [
                    "war",
                    "lawsuit",
                    "market",
                    "crash",
                    "drought",
                    "fears",
                    "战争",
                    "诉讼",
                    "市场",
                    "坠毁",
                    "干旱",
                    "担忧",
                ],
            },
        },
        topic_equivalence={},
    )


def _article(source: str, title: str, content: str | None = None, url: str | None = None) -> Article:
    return Article(
        url=url or f"https://example.com/{source}/{title}",
        title=title,
        source_name=source,
        published_at=datetime.now(tz=timezone.utc),
        content=content or f"{title}. " * 5,
        topics=["Culture"],
        id=100,
    )


def test_feelgood_scorer_selects_from_existing_articles():
    scorer = FeelgoodScorer(_cfg())
    selected = scorer.select_articles(
        [
            _article("City Feed", "Neighbors plan heartwarming reunion in city park"),
            _article("Animal Feed", "Adorable puppy rescued and reunited with family"),
            _article("Sports Feed", "Fan and mascot celebrate funny comeback win"),
            _article("Science Feed", "NASA shares stunning space discovery"),
        ],
        limit=4,
    )

    assert len(selected) == 4
    assert selected[0].positive_energy_score >= selected[-1].positive_energy_score
    assert any("Adorable puppy" in summary.summary for summary in selected)
    assert {summary.cluster.articles[0].source_name for summary in selected} <= {
        "City Feed",
        "Animal Feed",
        "Sports Feed",
        "Science Feed",
    }


def test_feelgood_scorer_does_not_require_external_feelgood_sources():
    cfg = _cfg()
    assert not hasattr(cfg, "feelgood_sources")
    scorer = FeelgoodScorer(cfg)

    selected = scorer.select_articles(
        [_article("Animal Feed", "Adorable puppy rescued by volunteers")],
        limit=1,
    )

    assert len(selected) == 1
    assert selected[0].cluster.articles[0].source_name == "Animal Feed"


def test_feelgood_scorer_rejects_sources_outside_configured_catalog():
    scorer = FeelgoodScorer(_cfg())

    selected = scorer.select_articles(
        [_article("External Feelgood", "Adorable puppy rescued by volunteers")],
        limit=1,
    )

    assert selected == []


def test_feelgood_scorer_blocks_pseudo_positive_policy_conflict_and_market_news():
    scorer = FeelgoodScorer(_cfg())
    selected = scorer.select_articles(
        [
            _article("City Feed", "Happy investors cheer market rebound"),
            _article("City Feed", "Cute mascot appears in lawsuit over team rule"),
            _article("Science Feed", "Space company celebrates after test crash"),
            _article("Science Feed", "Water returns to wetlands as drought fears loom"),
            _article("Animal Feed", "Adorable puppy rescued by volunteers"),
        ],
        limit=5,
    )

    assert [summary.cluster.articles[0].title for summary in selected] == [
        "Adorable puppy rescued by volunteers"
    ]


def test_feelgood_scorer_matches_chinese_themes_and_keeps_no_english_summary_for_zh_source():
    scorer = FeelgoodScorer(_cfg())
    selected = scorer.select_articles(
        [
            _article("中文源", "熊猫宝宝首次亮相 可爱治愈", "熊猫宝宝在公园首次亮相，游客说这是今天最暖心的惊喜。"),
            _article("中文源", "社区志愿者帮走失老人团圆", "志愿者接力寻找家人，故事暖心。"),
        ],
        limit=2,
    )

    assert len(selected) == 2
    assert selected[0].positive_energy_reason
    assert selected[0].positive_energy_reason_en
    assert selected[0].summary_en is None
    assert all(summary.positive_energy_score >= 0.42 for summary in selected)


def test_feelgood_scorer_deduplicates_urls_titles_and_prefers_domain_diversity():
    scorer = FeelgoodScorer(_cfg())
    selected = scorer.select_articles(
        [
            _article("Animal Feed", "Adorable puppy rescued by volunteers", url="https://same.example/a?utm=1"),
            _article("Animal Feed", "Adorable puppy rescued by volunteers update", url="https://same.example/a?utm=2"),
            _article("Sports Feed", "Fan and mascot celebrate comeback", url="https://same.example/b"),
            _article("Science Feed", "NASA shares stunning space discovery", url="https://other.example/c"),
        ],
        limit=2,
    )

    assert len(selected) == 2
    assert {summary.cluster.articles[0].url.split("/")[2] for summary in selected} == {
        "same.example",
        "other.example",
    }


def test_feelgood_scorer_sets_english_summary_and_bilingual_reason_for_english_source():
    scorer = FeelgoodScorer(_cfg())
    selected = scorer.select_articles(
        [_article("Animal Feed", "Adorable puppy rescued by volunteers")],
        limit=1,
    )

    assert selected[0].summary_en is None
    assert selected[0].positive_energy_reason == "可爱治愈、萌宠动物、救助故事"
    assert selected[0].positive_energy_reason_en == "Cute, Animals, Rescue story"
