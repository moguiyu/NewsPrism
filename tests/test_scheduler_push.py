import asyncio
import json
import os
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

from newsprism.runtime.scheduler import Scheduler


def _make_scheduler(tmp_path: Path) -> Scheduler:
    scheduler = Scheduler.__new__(Scheduler)
    scheduler._pipeline_lock = asyncio.Lock()
    scheduler.schedule_timezone = ZoneInfo("Europe/Warsaw")
    scheduler.output_dir = tmp_path / "output"
    scheduler.output_dir.mkdir(parents=True, exist_ok=True)
    scheduler.staging_dir = scheduler.output_dir / "staging"
    scheduler.staging_dir.mkdir(parents=True, exist_ok=True)
    scheduler.publish_complete_flag = scheduler.staging_dir / ".publish_complete"
    scheduler.push_retry_enabled = True
    scheduler.push_retry_max_attempts = 3
    scheduler.push_retry_interval_minutes = 5
    scheduler._apscheduler = None
    return scheduler


def test_push_promotes_staged_report_and_updates_latest(tmp_path):
    scheduler = _make_scheduler(tmp_path)
    report_date = date(2026, 4, 22)
    staged_dir = scheduler._staging_report_dir(report_date)
    staged_dir.mkdir(parents=True)
    (staged_dir / "index.html").write_text("<html>staged</html>", encoding="utf-8")
    (staged_dir / "data.json").write_text(
        json.dumps(
            {
                "total_cluster_count": 2,
                "clusters": [
                    {"topic": "World News", "summary": "**Headline**\n\nBody"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    scheduler.publish_complete_flag.write_text(
        json.dumps({"report_date": report_date.isoformat(), "total_story_count": 2}),
        encoding="utf-8",
    )

    class FakePublisher:
        def __init__(self):
            self.calls: list[tuple[Path, date]] = []

        async def publish_rendered(self, data_json_path, publish_date):
            self.calls.append((Path(data_json_path), publish_date))

    scheduler.publisher = FakePublisher()

    asyncio.run(scheduler.push(report_date=report_date))

    final_dir = scheduler.output_dir / report_date.isoformat()
    assert final_dir.exists()
    assert not staged_dir.exists()
    assert not scheduler.publish_complete_flag.exists()
    assert os.readlink(scheduler.output_dir / "latest") == report_date.isoformat()
    assert scheduler.publisher.calls == [(final_dir / "data.json", report_date)]


def test_push_schedules_retry_when_stage_not_ready(tmp_path):
    scheduler = _make_scheduler(tmp_path)
    report_date = date(2026, 4, 22)
    captured: list[str] = []

    class FakeScheduler:
        def add_job(self, func, trigger, id, replace_existing=False):
            captured.append(id)

    scheduler._apscheduler = FakeScheduler()

    asyncio.run(scheduler.push(report_date=report_date))

    assert captured == ["push_retry_2026-04-22_1"]
