from datetime import datetime, timezone
import logging

from newsprism.config import Config
from newsprism.repo.db import (
    delete_clusters_for_date,
    get_clusters_for_date,
    get_report_article_ids,
    get_recent_clusters,
    init_db,
    insert_article,
    insert_cluster,
    reset_articles_clustered,
)
from newsprism.runtime.scheduler import Scheduler
from newsprism.types import Article, Cluster


def _article(title: str) -> Article:
    return Article(
        url=f"https://example.com/{title}",
        title=title,
        source_name="Reuters",
        published_at=datetime.now(tz=timezone.utc),
        content=f"{title} body",
    )


def _cfg() -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={"max_clusters_per_report": 20, "time_window_hours": 48},
        dedup={"window_days": 3},
        summarizer={},
        output={},
        active_search={},
        topic_equivalence={},
    )


def test_replay_helpers_select_only_target_report_articles(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path=db_path)

    article_ids = []
    for title in ("a1", "a2", "a3", "a4"):
        article_ids.append(insert_article(_article(title), db_path=db_path))

    insert_cluster(
        Cluster(
            topic_category="World News",
            article_ids=[article_ids[0], article_ids[1]],
            summary="today-one",
            perspectives={},
            report_date="2026-03-14",
        ),
        db_path=db_path,
    )
    insert_cluster(
        Cluster(
            topic_category="Tech-General",
            article_ids=[article_ids[1], article_ids[2]],
            summary="today-two",
            perspectives={},
            report_date="2026-03-14",
        ),
        db_path=db_path,
    )
    insert_cluster(
        Cluster(
            topic_category="Finance",
            article_ids=[article_ids[3]],
            summary="older",
            perspectives={},
            report_date="2026-03-13",
        ),
        db_path=db_path,
    )

    target_ids = get_report_article_ids("2026-03-14", db_path=db_path)
    assert target_ids == sorted([article_ids[0], article_ids[1], article_ids[2]])

    reset_count = reset_articles_clustered(target_ids, db_path=db_path)
    assert reset_count == 3

    deleted = delete_clusters_for_date("2026-03-14", db_path=db_path)
    assert deleted == 2
    assert len(get_clusters_for_date("2026-03-14", db_path=db_path)) == 0
    assert len(get_clusters_for_date("2026-03-13", db_path=db_path)) == 1


def test_get_recent_clusters_excludes_anchor_date(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path=db_path)

    for report_date in ("2026-03-12", "2026-03-13", "2026-03-14"):
        insert_cluster(
            Cluster(
                topic_category="World News",
                article_ids=[],
                summary=report_date,
                perspectives={},
                report_date=report_date,
            ),
            db_path=db_path,
        )

    recent = get_recent_clusters(days=3, anchor_date="2026-03-14", db_path=db_path)
    assert [cluster.report_date for cluster in recent] == ["2026-03-13", "2026-03-12"]


def test_scheduler_replay_dry_run_logs_and_skips_mutation(monkeypatch, caplog):
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = _cfg()

    monkeypatch.setattr("newsprism.runtime.scheduler.get_report_article_ids", lambda _: [1, 2, 3])
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.get_clusters_for_date",
        lambda _: [
            Cluster(topic_category="World News", article_ids=[1, 2], summary="a", perspectives={}, report_date="2026-03-14"),
            Cluster(topic_category="World News", article_ids=[3], summary="b", perspectives={}, report_date="2026-03-14"),
        ],
    )

    async def fail_publish(*args, **kwargs):
        raise AssertionError("publish should not run in dry-run")

    scheduler.publish = fail_publish
    monkeypatch.setattr("newsprism.runtime.scheduler.delete_clusters_for_date", lambda _: (_ for _ in ()).throw(AssertionError("delete should not run")))
    monkeypatch.setattr("newsprism.runtime.scheduler.reset_articles_clustered", lambda _: (_ for _ in ()).throw(AssertionError("reset should not run")))

    import asyncio

    with caplog.at_level(logging.INFO):
        asyncio.run(scheduler.replay(report_date=datetime(2026, 3, 14, tzinfo=timezone.utc).date(), dry_run=True))

    assert "Replay dry-run" in caplog.text


def test_scheduler_replay_resets_target_articles_and_republishes(monkeypatch):
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = _cfg()

    target_articles = [_article("story-a"), _article("story-b")]
    target_articles[0].id = 11
    target_articles[1].id = 22

    monkeypatch.setattr("newsprism.runtime.scheduler.get_report_article_ids", lambda _: [11, 22])
    monkeypatch.setattr(
        "newsprism.runtime.scheduler.get_clusters_for_date",
        lambda _: [Cluster(topic_category="World News", article_ids=[11, 22], summary="a", perspectives={}, report_date="2026-03-14")],
    )

    calls: dict[str, object] = {}
    monkeypatch.setattr("newsprism.runtime.scheduler.delete_clusters_for_date", lambda _: 1)
    monkeypatch.setattr("newsprism.runtime.scheduler.reset_articles_clustered", lambda ids: len(ids))
    monkeypatch.setattr("newsprism.runtime.scheduler.get_articles_by_ids", lambda ids: target_articles if ids == [11, 22] else [])

    async def capture_publish(report_date=None, articles_override=None, push_after_render=True):
        calls["report_date"] = report_date
        calls["articles_override"] = articles_override
        calls["push_after_render"] = push_after_render

    scheduler.publish = capture_publish

    import asyncio

    replay_date = datetime(2026, 3, 14, tzinfo=timezone.utc).date()
    asyncio.run(scheduler.replay(report_date=replay_date, dry_run=False))

    assert calls["report_date"] == replay_date
    assert calls["articles_override"] == target_articles
    assert calls["push_after_render"] is True


def test_scheduler_publish_cleans_existing_report_before_collecting_unclustered(monkeypatch):
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = _cfg()

    import asyncio

    scheduler._pipeline_lock = asyncio.Lock()
    calls: dict[str, object] = {}
    monkeypatch.setattr("newsprism.runtime.scheduler.get_report_article_ids", lambda _: [11, 22])

    def reset(ids):
        calls["reset_ids"] = ids
        return len(ids)

    def delete(report_date):
        calls["deleted_report_date"] = report_date
        return 2

    def get_unclustered_articles(max_age_hours):
        calls["get_unclustered_after_cleanup"] = "reset_ids" in calls and "deleted_report_date" in calls
        return []

    monkeypatch.setattr("newsprism.runtime.scheduler.reset_articles_clustered", reset)
    monkeypatch.setattr("newsprism.runtime.scheduler.delete_clusters_for_date", delete)
    monkeypatch.setattr("newsprism.runtime.scheduler.get_unclustered_articles", get_unclustered_articles)

    asyncio.run(scheduler.publish(report_date=datetime(2026, 3, 14, tzinfo=timezone.utc).date(), push_after_render=False))

    assert calls["reset_ids"] == [11, 22]
    assert calls["deleted_report_date"] == "2026-03-14"
    assert calls["get_unclustered_after_cleanup"] is True
