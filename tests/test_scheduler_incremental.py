"""Tests for incremental clustering wiring in the scheduler."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from newsprism.config import Config
from newsprism.runtime.scheduler import Scheduler
from newsprism.types import Article, ArticleCluster, Cluster


def _config() -> Config:
    return Config(
        raw={}, sources=[], topics={}, schedule={}, collection={}, filter={},
        clustering={"incremental_enabled": True}, dedup={}, summarizer={},
        output={}, active_search={},
    )


def _article(url: str) -> Article:
    return Article(
        url=url,
        title="Title",
        source_name="Source",
        published_at=datetime.now(tz=timezone.utc),
        content="Content",
    )


def test_previous_clusters_for_incremental_converts_db_clusters(monkeypatch):
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = _config()

    records = [
        Cluster(
            topic_category="Event One",
            article_ids=[1, 2],
            summary="**One**\n\nBody",
            perspectives={},
            report_date="2026-08-21",
            storyline_key="story-1",
            storyline_name="Story One",
            storyline_role="core",
            storyline_confidence=0.9,
            storyline_state="emerging",
        )
    ]
    articles = [_article("https://example.com/1"), _article("https://example.com/2")]
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.get_clusters_for_date",
        lambda _date: records,
    )
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.get_articles_by_ids",
        lambda _ids: articles,
    )

    result = scheduler._previous_clusters_for_incremental(date(2026, 8, 22))
    assert len(result) == 1
    assert result[0].topic_category == "Event One"
    assert result[0].articles == articles
    assert result[0].storyline_key == "story-1"
    assert result[0].storyline_name == "Story One"


def test_previous_clusters_for_incremental_returns_empty_when_disabled():
    cfg = _config()
    cfg.clustering["incremental_enabled"] = False
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = cfg

    assert scheduler._previous_clusters_for_incremental(date(2026, 8, 22)) == []
