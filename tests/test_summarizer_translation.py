"""Tests for batched English translation and perspective-group normalization."""
from datetime import datetime, timezone
from types import SimpleNamespace

import litellm

from newsprism.config import Config
from newsprism.service.summarizer import Summarizer
from newsprism.types import Article, ArticleCluster, ClusterSummary, PerspectiveGroup


def _config() -> Config:
    return Config(
        raw={}, sources=[], topics={}, schedule={}, collection={}, filter={},
        clustering={}, dedup={}, summarizer={"max_tokens": 1200}, output={}, active_search={},
    )


def _summary(headline: str, body: str, groups: list[PerspectiveGroup]) -> ClusterSummary:
    cluster = ArticleCluster(
        topic_category="evt",
        articles=[
            Article(url="a", title="t", source_name="Reuters", published_at=datetime.now(timezone.utc), content="c"),
            Article(url="b", title="t", source_name="BBC News", published_at=datetime.now(timezone.utc), content="c"),
        ],
    )
    summary = ClusterSummary(cluster=cluster, summary=f"**{headline}**\n\n{body}")
    summary.grouped_perspectives = groups
    summary.short_topic_name = "中东局势"
    summary.storyline_name = "中东战事"
    return summary


def test_translate_report_content_batched(monkeypatch):
    summarizer = Summarizer(_config())
    summary = _summary("美军空袭伊朗", "美国对伊朗目标发动空袭。", [PerspectiveGroup(sources=["Reuters", "BBC News"], perspective="客观报道")])

    payload = (
        '{"items": [{"index": 0, "headline": "US strikes Iran", "body": "The US struck Iranian targets.", '
        '"short_topic_name": "Middle East", "perspective_groups": [{"sources": ["Reuters", "BBC News"], '
        '"perspective": "Factual reporting"}]}], "labels": {"中东战事": "Middle East war"}}'
    )

    def fake_completion(**kwargs):
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=payload))])

    monkeypatch.setattr(litellm, "completion", fake_completion)
    ok = summarizer.translate_report_content([summary], hot_topics=[], focus_storylines=[])

    assert ok is True
    assert summary.summary_en == "**US strikes Iran**\n\nThe US struck Iranian targets."
    assert summary.grouped_perspectives_en[0].sources == ["Reuters", "BBC News"]
    assert summary.storyline_name_en == "Middle East war"


def test_translate_report_content_degrades_to_chinese_on_failure(monkeypatch):
    summarizer = Summarizer(_config())
    summary = _summary("美军空袭伊朗", "美国对伊朗目标发动空袭。", [])

    def boom(**kwargs):
        raise RuntimeError("provider down")

    monkeypatch.setattr(litellm, "completion", boom)
    ok = summarizer.translate_report_content([summary], hot_topics=[], focus_storylines=[])

    assert ok is False
    assert summary.summary_en is None


def test_normalize_perspective_groups_backfills_missing_sources():
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="evt",
        articles=[
            Article(url="a", title="t", source_name="Reuters", published_at=datetime.now(timezone.utc), content="c"),
            Article(url="b", title="t", source_name="BBC News", published_at=datetime.now(timezone.utc), content="c"),
        ],
    )
    from newsprism.service.summarizer import PerspectiveGroupItem

    groups = summarizer._normalize_perspective_groups(
        cluster,
        [PerspectiveGroupItem(sources=["Reuters"], perspective="角度A")],
        [],
    )
    covered = {source for group in groups for source in group.sources}
    assert covered == {"Reuters", "BBC News"}
