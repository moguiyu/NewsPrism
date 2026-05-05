import json
import sqlite3
from datetime import datetime, timezone

from newsprism.repo.db import init_db, insert_article, insert_cluster, insert_search_request_event
from newsprism.runtime.audit import audit, format_audit_report
from newsprism.types import Article, Cluster, SearchRequestEvent


def _article(title: str, source: str = "Reuters") -> Article:
    return Article(
        url=f"https://example.com/{title}",
        title=title,
        source_name=source,
        published_at=datetime(2026, 3, 27, tzinfo=timezone.utc),
        content=f"{title} body",
        origin_region="us",
    )


def test_audit_reports_db_and_rendered_quality_metrics(tmp_path):
    db_path = tmp_path / "newsprism.db"
    output_dir = tmp_path / "output"
    report_dir = output_dir / "2026-03-27"
    report_dir.mkdir(parents=True)
    init_db(db_path)
    article_id = insert_article(_article("story"), db_path=db_path)
    insert_cluster(
        Cluster(
            topic_category="World News",
            article_ids=[article_id],
            summary="**Story**\n\nBody.",
            perspectives={"Reuters": "Same angle."},
            report_date="2026-03-27",
        ),
        db_path=db_path,
    )
    insert_search_request_event(
        SearchRequestEvent(
            provider="tavily",
            request_type="acceptance_filter",
            target_region="jp",
            query="chip export",
            result_count=3,
            accepted_count=1,
            rejection_reason="generic_page",
            rejection_count=2,
            created_at=datetime(2026, 3, 27, tzinfo=timezone.utc),
        ),
        db_path=db_path,
    )
    (report_dir / "data.json").write_text(
        json.dumps(
            {
                "clusters": [
                    {
                        "index": 1,
                        "topic": "World News",
                        "headline": "Story",
                        "is_multi": True,
                        "distinct_perspective_count": 1,
                        "duplicate_action": "kept",
                        "articles": [
                            {
                                "source": "Reuters",
                                "title": "General Motors Co | Reuters",
                                "url": "https://www.reuters.com/company/general-motors-co/",
                            }
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (report_dir / "index.html").write_text("<html>**raw markdown**</html>", encoding="utf-8")

    payload = audit(days=1, anchor_date="2026-03-27", db_path=db_path, output_dir=output_dir)
    report = format_audit_report(payload)

    assert payload["db"]["cluster_count"] == 1
    assert payload["db"]["raw_by_source"] == [("Reuters", 1)]
    assert payload["db"]["raw_by_region"] == [("us", 1)]
    assert payload["db"]["selected_by_source"] == [("Reuters", 1)]
    assert payload["db"]["active_search_rejection_reasons"] == [("generic_page", 2)]
    assert payload["issues"]["rendered_one_angle_multi_source"] == 1
    assert payload["issues"]["rendered_generic_or_stale_article"] == 1
    assert payload["issues"]["rendered_markdown_leak"] == 2
    assert "NewsPrism quality audit" in report


def test_audit_is_read_only(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    before = db_path.read_bytes()

    audit(days=1, anchor_date="2026-03-27", db_path=db_path, output_dir=tmp_path / "missing-output")

    after = db_path.read_bytes()
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT count(*) FROM articles").fetchone()[0] == 0
    assert before == after
