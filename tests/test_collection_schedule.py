import asyncio
from datetime import datetime, timezone

from newsprism.config import Config, SourceConfig
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

    assert captured_job_ids == ["collect_full", "collect_delta", "publish_stage", "push_daily"]
