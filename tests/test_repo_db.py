"""Tests for SQLite persistence of searched articles and telemetry."""
from datetime import datetime, timezone
import sqlite3

from newsprism.repo import (
    get_unclustered_articles,
    init_db,
    insert_article,
    insert_search_candidate_review,
    insert_search_request_event,
)
from newsprism.types import Article, SearchCandidateReview, SearchRequestEvent


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
            target_label="Japan MOFA",
            target_role="ministry",
            cluster_key="cluster-1",
            target_event_role="regulator",
            target_reason="The ministry owns the decision",
            coverage_before="missing",
            restricted_domains=["mofa.go.jp"],
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
            "SELECT provider, request_type, target_region, target_label, target_role, cluster_key, "
            "target_event_role, target_reason, coverage_before, restricted_domains, accepted_count, "
            "rejection_reason, rejection_count, estimated_cost_usd "
            "FROM search_request_events"
        ).fetchone()
    assert telemetry == (
        "x", "user_timeline", "jp", "Japan MOFA", "ministry", "cluster-1",
        "regulator", "The ministry owns the decision", "missing", '["mofa.go.jp"]',
        1, "generic_page", 2, 0.02,
    )

    review_id = insert_search_candidate_review(
        SearchCandidateReview(
            url="https://new-local.example/acme",
            domain="new-local.example",
            title="Acme response",
            source_name="new-local.example",
            target_label="Acme",
            target_region="cd",
            target_role="company",
            stage="country",
            verdict="country_editorial",
            decision="pending_review",
            identity_evidence={"source_type": "country_editorial", "relationship": "uncertain"},
        ),
        db_path=db_path,
    )
    assert review_id is not None
    with sqlite3.connect(db_path) as conn:
        candidate = conn.execute(
            "SELECT domain, target_region, target_role, verdict, decision, identity_evidence FROM search_candidate_reviews"
        ).fetchone()
    assert candidate[:5] == ("new-local.example", "cd", "company", "country_editorial", "pending_review")
    assert candidate[5] == '{"source_type":"country_editorial","relationship":"uncertain"}'


def test_init_db_adds_target_identity_columns_to_existing_search_tables(tmp_path):
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE search_request_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL,
                request_type TEXT NOT NULL, target_region TEXT, query TEXT,
                account_id TEXT, http_status INTEGER, result_count INTEGER,
                accepted_count INTEGER, rejection_reason TEXT, rejection_count INTEGER,
                duration_ms INTEGER, estimated_cost_usd REAL, created_at TEXT
            );
            CREATE TABLE search_candidate_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL,
                domain TEXT NOT NULL, title TEXT NOT NULL, source_name TEXT NOT NULL,
                target_label TEXT NOT NULL, target_region TEXT NOT NULL,
                stage TEXT NOT NULL, verdict TEXT NOT NULL, decision TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '', created_at TEXT,
                UNIQUE(url, target_label, stage)
            );
            """
        )

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:
        request_columns = {row[1] for row in conn.execute("PRAGMA table_info(search_request_events)")}
        candidate_columns = {row[1] for row in conn.execute("PRAGMA table_info(search_candidate_reviews)")}
    assert {
        "target_label", "target_role", "cluster_key", "target_event_role",
        "target_reason", "coverage_before", "restricted_domains",
    } <= request_columns
    assert {"target_role", "identity_evidence"} <= candidate_columns
