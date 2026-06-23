import asyncio
import json
import os
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

from newsprism.config import load_config
from newsprism.runtime.publisher import TelegramPublisher
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


def test_publish_rendered_skips_untranslated_positive_items(tmp_path, monkeypatch):
    data_path = tmp_path / "data.json"
    data_path.write_text(
        json.dumps(
            {
                "clusters": [
                    {
                        "topic": "World News",
                        "headline": "中文主新闻",
                        "summary": "这是一条中文摘要。",
                    }
                ],
                "positive_stories": [
                    {
                        "topic": "Positive Energy",
                        "headline": "Adorable puppy rescued by volunteers",
                        "summary": "Adorable puppy rescued by volunteers and reunited with a family.",
                        "positive_source": "BBC",
                    },
                    {
                        "topic": "Positive Energy",
                        "headline": "志愿者救助可爱小狗",
                        "summary": "志愿者救下一只可爱小狗，并帮助它与新家庭团聚。",
                        "positive_source": "BBC",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    publisher = TelegramPublisher(load_config())
    captured: list[tuple[list[dict[str, str]], date]] = []

    async def fake_publish_items(items, report_date):
        captured.append((items, report_date))

    monkeypatch.setattr(publisher, "_publish_items", fake_publish_items)

    asyncio.run(publisher.publish_rendered(data_path, date(2026, 5, 9)))

    assert [item["summary"] for item in captured[0][0]] == [
        "**中文主新闻**\n\n这是一条中文摘要。",
        "**志愿者救助可爱小狗**\n\n志愿者救下一只可爱小狗，并帮助它与新家庭团聚。",
    ]


def test_group_by_category_blocks_with_positive_last():
    from newsprism.runtime.publisher import _POSITIVE_CATEGORY, _broad, _category_label, _group_by_category

    assert _POSITIVE_CATEGORY == "今日好消息"

    items = [
        {"broad_category": "国际时政", "summary": "a"},
        {"broad_category": "商业财经", "summary": "b"},
        {"broad_category": "国际时政", "summary": "c"},  # category reappears
        {"broad_category": _POSITIVE_CATEGORY, "summary": "p"},
        {"broad_category": "科技创新", "summary": "d"},
    ]
    grouped = _group_by_category(items)
    # Legacy rendered category names normalize into the public category order;
    # positive stories always stay last.
    assert [_broad(it) for it in grouped] == [
        "World",
        "World",
        "Business",
        "Technology",
        _POSITIVE_CATEGORY,
    ]
    # Stable within a normalized category: impact order (a before c) is preserved.
    assert [it["summary"] for it in grouped if _broad(it) == "World"] == ["a", "c"]
    assert _category_label("World") == "国际"
    assert _category_label("Science & Health") == "科学健康"


def test_publish_rendered_groups_main_clusters_by_category(tmp_path, monkeypatch):
    data_path = tmp_path / "data.json"
    data_path.write_text(
        json.dumps(
            {
                "clusters": [
                    {"topic": "t1", "broad_category": "国际时政", "headline": "国际A", "summary": "摘要一", "cluster_id": 1},
                    {"topic": "t2", "broad_category": "商业财经", "headline": "财经B", "summary": "摘要二", "cluster_id": 2},
                    {"topic": "t3", "broad_category": "国际时政", "headline": "国际C", "summary": "摘要三", "cluster_id": 3},
                ],
                "positive_stories": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    publisher = TelegramPublisher(load_config())
    captured: list[list[dict[str, str]]] = []

    async def fake_publish_items(items, report_date):
        captured.append(items)

    monkeypatch.setattr(publisher, "_publish_items", fake_publish_items)

    asyncio.run(publisher.publish_rendered(data_path, date(2026, 5, 9)))

    # Legacy rendered categories normalize to World, World, Business in public order.
    assert [item["cluster_id"] for item in captured[0]] == [1, 3, 2]
