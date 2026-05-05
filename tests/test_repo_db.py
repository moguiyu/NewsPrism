"""Tests for SQLite persistence of searched articles and telemetry."""
from datetime import datetime, timezone
import sqlite3

from newsprism.repo import (
    get_unclustered_articles,
    init_db,
    insert_article,
    insert_search_request_event,
)
from newsprism.types import Article, SearchRequestEvent


def test_init_db_persists_searched_article_metadata_and_telemetry(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path)

    article_id = insert_article(
        Article(
            url="https://x.com/mfa/status/1",
            title="Official statement",
            source_name="Japan MOFA",
            published_at=datetime.now(tz=timezone.utc),
            content="Official statement on export controls." + "x" * 40,
            is_searched=True,
            search_region="jp",
            source_kind="official_social",
            platform="x",
            account_id="mofa-jp",
            is_official_source=True,
            origin_region="jp",
            searched_provider="x_user_timeline",
        ),
        db_path=db_path,
    )
    assert article_id is not None

    rows = get_unclustered_articles(max_age_hours=48, db_path=db_path)
    assert len(rows) == 1
    assert rows[0].is_searched is True
    assert rows[0].search_region == "jp"
    assert rows[0].source_kind == "official_social"
    assert rows[0].platform == "x"
    assert rows[0].account_id == "mofa-jp"
    assert rows[0].is_official_source is True
    assert rows[0].origin_region == "jp"
    assert rows[0].searched_provider == "x_user_timeline"

    insert_search_request_event(
        SearchRequestEvent(
            provider="x",
            request_type="user_timeline",
            target_region="jp",
            query="chip export",
            account_id="mofa-jp",
            http_status=200,
            result_count=3,
            accepted_count=1,
            rejection_reason="generic_page",
            rejection_count=2,
            duration_ms=120,
            estimated_cost_usd=0.02,
        ),
        db_path=db_path,
    )

    with sqlite3.connect(db_path) as conn:
        telemetry = conn.execute(
            "SELECT provider, request_type, target_region, accepted_count, rejection_reason, rejection_count, estimated_cost_usd "
            "FROM search_request_events"
        ).fetchone()
    assert telemetry == ("x", "user_timeline", "jp", 1, "generic_page", 2, 0.02)
