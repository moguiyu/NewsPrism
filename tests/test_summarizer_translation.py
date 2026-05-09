from datetime import datetime, timezone
import json

from newsprism.config import load_config
from newsprism.service.summarizer import Summarizer
from newsprism.types import Article, ArticleCluster, ClusterSummary, PerspectiveGroup


def _build_summary() -> ClusterSummary:
    return ClusterSummary(
        cluster=ArticleCluster(
            topic_category="World News",
            articles=[
                Article(
                    url="https://reuters.com/1",
                    title="Story A",
                    source_name="Reuters",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Story A body",
                ),
                Article(
                    url="https://bbc.com/1",
                    title="Story B",
                    source_name="BBC",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Story B body",
                ),
            ],
        ),
        summary="**中文标题**\n\n中文摘要内容。",
        perspectives={
            "Reuters": "美方视角",
            "BBC": "英方视角",
        },
        grouped_perspectives=[
            PerspectiveGroup(
                sources=["Reuters"],
                perspective="美方视角",
            ),
            PerspectiveGroup(
                sources=["BBC"],
                perspective="英方视角",
            ),
        ],
        short_topic_name="中东局势",
        storyline_name="访华主线",
        macro_topic_name="中东局势",
    )


def test_translate_report_content_populates_english_fields(monkeypatch):
    summarizer = Summarizer(load_config())
    summary = _build_summary()
    hot_topics = [{"macro_topic_name": "中东局势", "storyline_name": "中东局势"}]
    focus_storylines = [{"storyline_name": "访华主线"}]

    def fake_json_completion(system_prompt: str, user_prompt: str, max_tokens: int, temperature: float = 0.1) -> str:
        if "Chinese news digest JSON" in user_prompt:
            return json.dumps(
                {
                    "headline": "English Headline",
                    "body": "English summary content.",
                    "short_topic_name": "Middle East",
                    "perspective_groups": [
                        {"sources": ["Reuters"], "perspective": "US angle"},
                        {"sources": ["BBC"], "perspective": "UK angle"},
                    ],
                }
            )
        if "label: 中东局势" in user_prompt:
            return json.dumps({"translation": "Middle East"})
        if "label: 访华主线" in user_prompt:
            return json.dumps({"translation": "China Visit"})
        raise AssertionError(f"Unexpected prompt: {user_prompt}")

    monkeypatch.setattr(summarizer, "_json_completion", fake_json_completion)

    assert summarizer.translate_report_content(
        [summary],
        hot_topics=hot_topics,
        focus_storylines=focus_storylines,
    ) is True

    assert summary.summary_en == "**English Headline**\n\nEnglish summary content."
    assert [group.perspective for group in summary.grouped_perspectives_en] == ["US angle", "UK angle"]
    assert summary.short_topic_name_en == "Middle East"
    assert summary.storyline_name_en == "China Visit"
    assert summary.macro_topic_name_en == "Middle East"
    assert hot_topics[0]["macro_topic_name_en"] == "Middle East"
    assert focus_storylines[0]["storyline_name_en"] == "China Visit"


def test_translate_report_content_keeps_english_when_perspective_groups_drift(monkeypatch):
    summarizer = Summarizer(load_config())
    summary = _build_summary()

    def fake_json_completion(system_prompt: str, user_prompt: str, max_tokens: int, temperature: float = 0.1) -> str:
        if "Chinese news digest JSON" in user_prompt:
            return json.dumps(
                {
                    "headline": "English Headline",
                    "body": "English summary content.",
                    "short_topic_name": "Middle East",
                    "perspective_groups": [
                        {"sources": ["Reuters", "BBC"], "perspective": "Merged angle"},
                    ],
                }
            )
        if "label: 访华主线" in user_prompt:
            return json.dumps({"translation": "China Visit"})
        if "label: 中东局势" in user_prompt:
            return json.dumps({"translation": "Middle East"})
        raise AssertionError(f"Unexpected prompt: {user_prompt}")

    monkeypatch.setattr(summarizer, "_json_completion", fake_json_completion)

    assert summarizer.translate_report_content([summary]) is True

    assert summary.summary_en == "**English Headline**\n\nEnglish summary content."
    assert [group.sources for group in summary.grouped_perspectives_en] == [["Reuters"], ["BBC"]]
    assert [group.perspective for group in summary.grouped_perspectives_en] == [
        "Merged angle",
        "This source reports a similar angle to the main summary.",
    ]


def test_translate_report_content_clears_partial_english_on_failure(monkeypatch):
    summarizer = Summarizer(load_config())
    summary = _build_summary()
    hot_topics = [{"macro_topic_name": "中东局势", "storyline_name": "中东局势"}]
    focus_storylines = [{"storyline_name": "访华主线"}]

    def failing_json_completion(system_prompt: str, user_prompt: str, max_tokens: int, temperature: float = 0.1) -> str:
        raise RuntimeError("translation service unavailable")

    monkeypatch.setattr(summarizer, "_json_completion", failing_json_completion)

    assert summarizer.translate_report_content(
        [summary],
        hot_topics=hot_topics,
        focus_storylines=focus_storylines,
    ) is False

    assert summary.summary_en is None
    assert summary.grouped_perspectives_en == []
    assert summary.short_topic_name_en is None
    assert summary.storyline_name_en is None
    assert summary.macro_topic_name_en is None
    assert "macro_topic_name_en" not in hot_topics[0]
    assert "storyline_name_en" not in focus_storylines[0]


def test_normalize_positive_energy_summaries_translates_source_language_primary(monkeypatch):
    summarizer = Summarizer(load_config())
    summary = ClusterSummary(
        cluster=ArticleCluster(
            topic_category="Positive Energy",
            articles=[
                Article(
                    url="https://guardian.example/positive",
                    title="King Charles features in surprise birthday tribute",
                    source_name="The Guardian",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Animals carry a centennial card to David Attenborough in a playful tribute.",
                )
            ],
        ),
        summary="**King Charles features in surprise birthday tribute**\n\nAnimals carry a centennial card to David Attenborough in a playful tribute.",
        perspectives={},
    )

    def fake_json_completion(system_prompt: str, user_prompt: str, max_tokens: int, temperature: float = 0.1) -> str:
        assert "Normalize this 今日正能量" in user_prompt
        return json.dumps(
            {
                "headline_zh": "英王为阿滕伯勒送上生日惊喜",
                "body_zh": "一支轻松短片让动物接力送上百岁生日贺卡，向大卫·阿滕伯勒致意。",
                "headline_en": "King Charles sends surprise birthday tribute to Attenborough",
                "body_en": "A playful film shows animals relaying a centennial card to David Attenborough.",
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(summarizer, "_json_completion", fake_json_completion)

    normalized = summarizer.normalize_positive_energy_summaries([summary], include_english=True)

    assert normalized == [summary]
    assert summary.summary.startswith("**英王为阿滕伯勒送上生日惊喜**")
    assert "轻松短片" in summary.summary
    assert summary.summary_en == (
        "**King Charles sends surprise birthday tribute to Attenborough**\n\n"
        "A playful film shows animals relaying a centennial card to David Attenborough."
    )


def test_normalize_positive_energy_summaries_drops_untranslated_source_language(monkeypatch):
    summarizer = Summarizer(load_config())
    summary = ClusterSummary(
        cluster=ArticleCluster(
            topic_category="Positive Energy",
            articles=[
                Article(
                    url="https://spidersweb.example/watch",
                    title="Nie musisz wydawać fortuny",
                    source_name="Spider's Web",
                    published_at=datetime.now(tz=timezone.utc),
                    content="Polish source text.",
                )
            ],
        ),
        summary="**Nie musisz wydawać fortuny**\n\nPolish source text.",
        perspectives={},
    )

    def fake_json_completion(system_prompt: str, user_prompt: str, max_tokens: int, temperature: float = 0.1) -> str:
        return json.dumps(
            {
                "headline_zh": "Nie musisz wydawać fortuny",
                "body_zh": "Polish source text.",
                "headline_en": "You do not need to spend a fortune",
                "body_en": "Polish source text.",
            }
        )

    monkeypatch.setattr(summarizer, "_json_completion", fake_json_completion)

    assert summarizer.normalize_positive_energy_summaries([summary], include_english=True) == []


def test_classify_positive_energy_parses_json_with_wrapping(monkeypatch):
    summarizer = Summarizer(load_config())
    summary = _build_summary()

    def fake_json_completion(system_prompt: str, user_prompt: str, max_tokens: int, temperature: float = 0.1) -> str:
        assert "今日正能量" in user_prompt
        assert "good_fit=true" in user_prompt
        assert "仅仅“没有坏消息”不算" in user_prompt
        return (
            "```json\n"
            + json.dumps(
                {
                    "items": [
                        {
                            "cluster_index": 1,
                            "good_fit": True,
                            "positive": True,
                            "fun": False,
                            "low_conflict": True,
                            "confidence": 0.86,
                            "reason": "建设性进展",
                        }
                    ]
                },
                ensure_ascii=False,
            )
            + "\n```"
        )

    monkeypatch.setattr(summarizer, "_json_completion", fake_json_completion)

    result = summarizer.classify_positive_energy([summary])

    assert result == [
        {
            "cluster_index": 1,
            "good_fit": True,
            "positive": True,
            "fun": False,
            "low_conflict": True,
            "confidence": 0.86,
            "reason": "建设性进展",
        }
    ]


def test_classify_positive_energy_retries_after_malformed_json(monkeypatch):
    summarizer = Summarizer(load_config())
    summary = _build_summary()
    calls = {"count": 0}

    def fake_json_completion(system_prompt: str, user_prompt: str, max_tokens: int, temperature: float = 0.1) -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            return "not json"
        return json.dumps(
            {
                "items": [
                    {
                        "cluster_index": 1,
                        "good_fit": True,
                        "positive": False,
                        "fun": True,
                        "low_conflict": True,
                        "confidence": 0.74,
                        "reason": "轻松有趣",
                    }
                ]
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(summarizer, "_json_completion", fake_json_completion)

    result = summarizer.classify_positive_energy([summary])

    assert calls["count"] == 2
    assert result[0]["good_fit"] is True
    assert result[0]["fun"] is True
    assert result[0]["reason"] == "轻松有趣"
