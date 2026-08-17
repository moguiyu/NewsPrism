"""Summarizer token-efficiency helpers: bounded input and partial salvage."""
from __future__ import annotations

from datetime import datetime, timezone

from newsprism.config import Config
from newsprism.service.summarizer import Summarizer
from newsprism.types import Article, ArticleCluster


def _config() -> Config:
    return Config(
        raw={}, sources=[], topics={}, schedule={}, collection={}, filter={},
        clustering={}, dedup={}, summarizer={"max_tokens": 1200, "article_content_chars": 600},
        output={}, active_search={},
    )


def _cluster() -> ArticleCluster:
    article = Article(
        url="https://example.com/a",
        title="Event title",
        source_name="Reuters",
        published_at=datetime.now(timezone.utc),
        content="x" * 5000,
    )
    return ArticleCluster(topic_category="Event", articles=[article])


def test_format_articles_respects_content_cap_and_drops_url():
    summarizer = Summarizer(_config())
    text = summarizer._format_articles(_cluster())
    assert len("x" * 600) == 600
    assert "x" * 600 in text
    assert "x" * 601 not in text
    assert "https://example.com/a" not in text


def test_salvage_batch_summary_items_recovers_complete_items():
    summarizer = Summarizer(_config())
    raw = (
        '{"clusters": ['
        '{"index": 0, "headline": "A", "body": "body a", "short_topic_name": "A", '
        '"topic_icon_key": "globe", "perspective_groups": []}, '
        '{"index": 1, "headline": "B", "body": "body b", "short_topic_name": "B", '
        '"topic_icon_key": "globe", "perspective_groups": []}, '
        '{"index": 2, "headline": "trunc'
    )
    items = summarizer._salvage_batch_summary_items(raw)
    assert [item.index for item in items] == [0, 1]
    assert items[0].headline == "A"


def test_batch_summary_retries_non_chinese_item_per_cluster(monkeypatch):
    from types import SimpleNamespace

    import litellm

    summarizer = Summarizer(_config())
    cluster = _cluster()
    payload = (
        '{"clusters": [{"index": 0, "headline": "English headline", '
        '"body": "This body is entirely in English and not Chinese.", '
        '"short_topic_name": "English", "topic_icon_key": "globe", '
        '"perspective_groups": []}]}'
    )

    def fake_completion(**kwargs):
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=payload))])

    monkeypatch.setattr(litellm, "completion", fake_completion)
    calls: list[bool] = []

    def fake_per_cluster(cluster_arg, require_chinese=False):
        calls.append(require_chinese)
        from newsprism.types import ClusterSummary

        return ClusterSummary(cluster=cluster_arg, summary="**中文标题**\n\n这是中文正文。", perspectives={})

    monkeypatch.setattr(summarizer, "_summarize_cluster", fake_per_cluster)

    results = summarizer._batch_summarize([cluster])
    assert calls == [True]
    assert results[0].summary == "**中文标题**\n\n这是中文正文。"


def test_build_prompt_can_force_chinese_output():
    summarizer = Summarizer(_config())
    cluster = _cluster()
    prompt = summarizer._build_prompt(cluster, summarizer._format_articles(cluster), require_chinese=True)
    assert "强制要求" in prompt
    assert "简体中文" in prompt
