import asyncio
from datetime import datetime, timezone

from newsprism.config import Config, SourceConfig
from newsprism.runtime.scheduler import Scheduler
from newsprism.service.collector import Collector
from newsprism.service.filter import TopicTagger
from newsprism.types import Article, RawArticle


def _cfg(collection: dict, schedule: dict | None = None, sources: list[SourceConfig] | None = None) -> Config:
    return Config(
        raw={},
        sources=sources or [],
        topics={},
        schedule=schedule or {"timezone": "Europe/Warsaw"},
        collection=collection,
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={},
        active_search={},
        topic_equivalence={},
    )


def _source(name: str, tier: str = "tech") -> SourceConfig:
    return SourceConfig(
        name=name,
        name_en=name,
        url=f"https://example.com/{name}",
        rss_url=f"https://example.com/{name}.xml",
        type="rss",
        weight=1.0,
        language="en",
        region="us",
        tier=tier,
    )


def _raw(source_name: str) -> RawArticle:
    return RawArticle(
        url=f"https://example.com/{source_name}/story",
        title=f"{source_name} story",
        source_name=source_name,
        published_at=datetime.now(tz=timezone.utc),
        content="body " * 100,
    )


def _article(source_name: str, title: str, content: str = "body") -> Article:
    return Article(
        url=f"https://example.com/{source_name}/{title}",
        title=title,
        source_name=source_name,
        published_at=datetime.now(tz=timezone.utc),
        content=content,
    )


def test_positive_energy_pre_filter_rescues_portal_whale_calf_story():
    cfg = _cfg(
        collection={},
        sources=[_source("Portal", tier="portal")],
    )
    tagger = TopicTagger(cfg)
    article = _article("Portal", "Rare whale calf spotted playing near coast")

    tagged = tagger.tag_all([article])

    assert tagged == [article]
    assert article.topics == ["Positive Energy"]


def test_positive_energy_pre_filter_keeps_neutral_portal_story_dropped():
    cfg = _cfg(
        collection={},
        sources=[_source("Portal", tier="portal")],
    )
    tagger = TopicTagger(cfg)
    article = _article("Portal", "City council reviews parking fees")

    assert tagger.tag_all([article]) == []


def test_positive_energy_pre_filter_excludes_conflict_with_positive_word():
    cfg = _cfg(
        collection={},
        sources=[_source("Portal", tier="portal")],
    )
    tagger = TopicTagger(cfg)
    article = _article("Portal", "Happy reunion follows shooting investigation")

    assert tagger.tag_all([article]) == []


def test_positive_energy_pre_filter_merges_with_normal_topic_once():
    cfg = _cfg(
        collection={},
        sources=[_source("Portal", tier="portal")],
    )
    cfg.topics = {"Culture": ["festival"]}
    tagger = TopicTagger(cfg)
    article = _article("Portal", "Cute festival brings joy downtown")

    tagged = tagger.tag_all([article])

    assert tagged == [article]
    assert article.topics == ["Culture", "Positive Energy"]


def test_collect_delta_uses_configured_delta_sources(monkeypatch):
    cfg = _cfg(
        collection={
            "full_max_age_hours": 8,
            "delta_max_age_hours": 3,
            "delta_source_names": ["Alpha", "Gamma"],
        },
        sources=[_source("Alpha"), _source("Beta"), _source("Gamma")],
    )
    collector = Collector(cfg)

    calls: list[tuple[str, int]] = []

    def fake_collect_source(src, max_age_hours):
        calls.append((src.name, max_age_hours))
        return [_raw(src.name)]

    monkeypatch.setattr(collector, "_collect_source", fake_collect_source)

    articles = asyncio.run(collector.collect_all(mode="delta"))

    assert [name for name, _ in calls] == ["Alpha", "Gamma"]
    assert all(max_age == 3 for _, max_age in calls)
    assert [article.source_name for article in articles] == ["Alpha", "Gamma"]


def test_collect_full_skips_seeded_daily_retry_sources_outside_retry_hour(monkeypatch):
    cfg = _cfg(
        collection={
            "full_max_age_hours": 8,
            "backoff": {
                "enabled": True,
                "failure_threshold": 9,
                "rolling_window_runs": 18,
                "restore_success_streak": 3,
                "daily_retry_hours_local": [18],
                "seed_daily_retry_source_names": ["RetryOnly"],
            },
        },
        sources=[_source("Regular"), _source("RetryOnly")],
    )
    collector = Collector(cfg)

    calls: list[str] = []

    def fake_collect_source(src, max_age_hours):
        calls.append(src.name)
        return [_raw(src.name)]

    monkeypatch.setattr(collector, "_collect_source", fake_collect_source)
    monkeypatch.setattr(
        collector,
        "_local_now",
        lambda: datetime(2026, 4, 21, 10, 0, tzinfo=collector.schedule_timezone),
    )

    asyncio.run(collector.collect_all(mode="full"))

    assert calls == ["Regular"]

    calls.clear()
    monkeypatch.setattr(
        collector,
        "_local_now",
        lambda: datetime(2026, 4, 21, 18, 0, tzinfo=collector.schedule_timezone),
    )

    asyncio.run(collector.collect_all(mode="full"))

    assert calls == ["Regular", "RetryOnly"]


def test_collect_backoff_enters_retry_mode_and_clears_after_successes(monkeypatch):
    cfg = _cfg(
        collection={
            "full_max_age_hours": 8,
            "backoff": {
                "enabled": True,
                "failure_threshold": 2,
                "rolling_window_runs": 4,
                "restore_success_streak": 2,
                "daily_retry_hours_local": [18],
            },
        },
        sources=[_source("Flaky")],
    )
    collector = Collector(cfg)

    outcomes = [[], [], [_raw("Flaky")], [_raw("Flaky")]]
    calls: list[str] = []

    def fake_collect_source(src, max_age_hours):
        calls.append(src.name)
        return outcomes.pop(0)

    monkeypatch.setattr(collector, "_collect_source", fake_collect_source)
    monkeypatch.setattr(
        collector,
        "_local_now",
        lambda: datetime(2026, 4, 21, 10, 0, tzinfo=collector.schedule_timezone),
    )

    asyncio.run(collector.collect_all(mode="full"))
    asyncio.run(collector.collect_all(mode="full"))
    assert collector._source_state["Flaky"].in_daily_retry is True

    calls.clear()
    asyncio.run(collector.collect_all(mode="full"))
    assert calls == []

    monkeypatch.setattr(
        collector,
        "_local_now",
        lambda: datetime(2026, 4, 21, 18, 0, tzinfo=collector.schedule_timezone),
    )
    asyncio.run(collector.collect_all(mode="full"))
    assert collector._source_state["Flaky"].in_daily_retry is True
    asyncio.run(collector.collect_all(mode="full"))
    assert collector._source_state["Flaky"].in_daily_retry is False


def test_scheduler_registers_full_delta_publish_and_push_jobs(monkeypatch):
    cfg = _cfg(
        collection={},
        schedule={
            "timezone": "Europe/Warsaw",
            "full_collect_cron": "15 0,4,16,20 * * *",
            "prepublish_collect_cron": "20 7 * * *",
            "publish_cron": "30 7 * * *",
            "push_cron": "0 8 * * *",
        },
    )
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = cfg
    scheduler._apscheduler = None

    def fake_cleanup():
        return None

    scheduler._cleanup_old_staging = fake_cleanup

    captured_job_ids: list[str] = []

    class FakeScheduler:
        def __init__(self, timezone=None):
            self.timezone = timezone

        def add_job(self, func, trigger, id):
            captured_job_ids.append(id)

        def start(self):
            return None

    class FakeEvent:
        async def wait(self):
            return None

    monkeypatch.setattr("newsprism.runtime.scheduler.AsyncIOScheduler", FakeScheduler)
    monkeypatch.setattr("newsprism.runtime.scheduler.asyncio.Event", FakeEvent)

    asyncio.run(scheduler._run_scheduler())

    assert captured_job_ids == ["collect_full", "collect_delta", "publish_stage", "push_daily"]
