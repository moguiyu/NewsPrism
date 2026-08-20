import asyncio
from datetime import datetime, timezone

from newsprism.config import Config, SourceConfig, load_config
from newsprism.runtime.scheduler import Scheduler
from newsprism.service.collector import Collector
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

    assert captured_job_ids == [
        "collect_full",
        "collect_delta",
        "publish_stage",
        "push_daily",
        "calibrate_weekly",
        "retention_weekly",
        "output_retention_weekly",
    ]


def test_config_sets_utc_deepseek_processing_window():
    cfg = load_config("config/config.yaml")

    assert cfg.schedule["llm_processing_timezone"] == "UTC"
    assert cfg.schedule["prepublish_collect_cron"] == "5 5 * * *"
    assert cfg.schedule["publish_cron"] == "20 5 * * *"
    assert cfg.clustering["llm_max_articles_per_call"] == 60


def test_scheduler_uses_processing_timezone_only_for_delta_and_publish(monkeypatch):
    cfg = _cfg(
        collection={},
        schedule={
            "timezone": "Europe/Warsaw",
            "llm_processing_timezone": "UTC",
            "full_collect_cron": "15 0,4,16,20 * * *",
            "prepublish_collect_cron": "5 5 * * *",
            "publish_cron": "20 5 * * *",
            "push_cron": "0 8 * * *",
        },
    )
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = cfg
    scheduler._apscheduler = None
    scheduler._cleanup_old_staging = lambda: None

    captured_triggers = {}

    class FakeScheduler:
        def __init__(self, timezone=None):
            self.timezone = timezone

        def add_job(self, func, trigger, id):
            captured_triggers[id] = trigger

        def start(self):
            return None

    class FakeEvent:
        async def wait(self):
            return None

    monkeypatch.setattr("newsprism.runtime.scheduler.AsyncIOScheduler", FakeScheduler)
    monkeypatch.setattr("newsprism.runtime.scheduler.asyncio.Event", FakeEvent)

    asyncio.run(scheduler._run_scheduler())

    assert str(captured_triggers["collect_full"].timezone) == "Europe/Warsaw"
    assert str(captured_triggers["collect_delta"].timezone) == "UTC"
    assert str(captured_triggers["publish_stage"].timezone) == "UTC"
    assert str(captured_triggers["push_daily"].timezone) == "Europe/Warsaw"
    assert str(captured_triggers["calibrate_weekly"].timezone) == "Europe/Warsaw"
    assert str(captured_triggers["retention_weekly"].timezone) == "Europe/Warsaw"
    assert str(captured_triggers["output_retention_weekly"].timezone) == "Europe/Warsaw"


def test_scheduler_processing_timezone_falls_back_to_schedule_timezone(monkeypatch):
    cfg = _cfg(
        collection={},
        schedule={
            "timezone": "Europe/Warsaw",
            "prepublish_collect_cron": "20 7 * * *",
            "publish_cron": "30 7 * * *",
            "push_cron": "0 8 * * *",
        },
    )
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.cfg = cfg
    scheduler._apscheduler = None
    scheduler._cleanup_old_staging = lambda: None

    captured_triggers = {}

    class FakeScheduler:
        def __init__(self, timezone=None):
            self.timezone = timezone

        def add_job(self, func, trigger, id):
            captured_triggers[id] = trigger

        def start(self):
            return None

    class FakeEvent:
        async def wait(self):
            return None

    monkeypatch.setattr("newsprism.runtime.scheduler.AsyncIOScheduler", FakeScheduler)
    monkeypatch.setattr("newsprism.runtime.scheduler.asyncio.Event", FakeEvent)

    asyncio.run(scheduler._run_scheduler())

    assert str(captured_triggers["collect_delta"].timezone) == "Europe/Warsaw"
    assert str(captured_triggers["publish_stage"].timezone) == "Europe/Warsaw"
