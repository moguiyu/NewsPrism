"""Tests for SQLite persistence of searched articles and telemetry."""
from datetime import datetime, timezone
import sqlite3

from newsprism.repo import (
    get_articles_by_ids,
    get_unclustered_articles,
    init_db,
    insert_llm_call_event,
    insert_article,
    insert_cluster,
    insert_search_candidate_review,
    insert_search_request_event,
    selected_source_regions,
)
from newsprism.types import (
    Article,
    Cluster,
    LLMCallEvent,
    SearchCandidateReview,
    SearchRequestEvent,
)


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
            search_evidence_role="direct_event",
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
    assert rows[0].search_evidence_role == "direct_event"
    assert rows[0].is_placeholder is False

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
            published_at=datetime(2026, 8, 2, 5, 30, tzinfo=timezone.utc),
        ),
        db_path=db_path,
    )
    assert review_id is not None
    with sqlite3.connect(db_path) as conn:
        candidate = conn.execute(
            "SELECT domain, target_region, target_role, verdict, decision, identity_evidence, published_at FROM search_candidate_reviews"
        ).fetchone()
    assert candidate[:5] == ("new-local.example", "cd", "company", "country_editorial", "pending_review")
    assert candidate[5] == '{"source_type":"country_editorial","relationship":"uncertain"}'
    assert candidate[6] == "2026-08-02T05:30:00+00:00"


def test_placeholder_metadata_round_trips_and_is_not_unclustered(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path)

    placeholder = Article(
        url="placeholder:fr:cluster-1",
        title="待补充：法国声音",
        source_name="[法国声音待补]",
        published_at=datetime.now(tz=timezone.utc),
        content="",
        is_searched=True,
        search_region="fr",
        origin_region="fr",
        searched_provider="tavily_search",
        is_placeholder=True,
        search_acceptance_status="failed",
        search_acceptance_reason="candidate_pending_review",
        search_stage_trace=[
            {"stage": "official", "reason": "official_not_found"},
            {"stage": "country", "reason": "candidate_pending_review"},
        ],
    )
    article_id = insert_article(placeholder, db_path=db_path)
    assert article_id is not None

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT is_placeholder, search_acceptance_status, search_acceptance_reason, search_stage_trace "
            "FROM articles WHERE id = ?",
            (article_id,),
        ).fetchone()
    assert row == (
        1,
        "failed",
        "candidate_pending_review",
        '[{"stage":"official","reason":"official_not_found"},{"stage":"country","reason":"candidate_pending_review"}]',
    )

    loaded = get_articles_by_ids([article_id], db_path=db_path)
    assert len(loaded) == 1
    assert loaded[0].is_placeholder is True
    assert loaded[0].search_acceptance_status == "failed"
    assert loaded[0].search_acceptance_reason == "candidate_pending_review"
    assert loaded[0].search_stage_trace[-1] == {
        "stage": "country",
        "reason": "candidate_pending_review",
    }
    assert get_unclustered_articles(max_age_hours=48, db_path=db_path) == []


def test_legacy_placeholder_url_is_migrated_and_filtered(tmp_path):
    db_path = tmp_path / "legacy.db"
    published_at = datetime.now(tz=timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE articles (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   url TEXT UNIQUE NOT NULL,
                   title TEXT NOT NULL,
                   source_name TEXT NOT NULL,
                   published_at TEXT NOT NULL,
                   content TEXT NOT NULL,
                   topics TEXT NOT NULL DEFAULT '[]',
                   embedding TEXT,
                   clustered INTEGER NOT NULL DEFAULT 0
               )"""
        )
        conn.execute(
            "INSERT INTO articles (url, title, source_name, published_at, content) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                "placeholder:ua:legacy-cluster",
                "待补充：乌克兰声音",
                "[乌克兰声音待补]",
                published_at,
                "",
            ),
        )

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(articles)")}
        stored = conn.execute(
            "SELECT is_placeholder, search_acceptance_status, search_acceptance_reason "
            "FROM articles WHERE url LIKE 'placeholder:%'"
        ).fetchone()
    assert {
        "is_placeholder",
        "search_acceptance_status",
        "search_acceptance_reason",
        "search_evidence_role",
    } <= columns
    assert stored == (1, None, None)

    loaded = get_unclustered_articles(max_age_hours=48, db_path=db_path)
    assert loaded == []
    by_id = get_articles_by_ids([1], db_path=db_path)
    assert by_id[0].is_placeholder is True


def test_selected_source_regions_excludes_placeholder_rows(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    now = datetime.now(tz=timezone.utc)
    real_id = insert_article(
        Article(
            url="https://reuters.example/story",
            title="Real story",
            source_name="Reuters",
            published_at=now,
            content="body",
            origin_region="us",
        ),
        db_path=db_path,
    )
    placeholder_id = insert_article(
        Article(
            url="placeholder:fr:story",
            title="待补充：法国声音",
            source_name="[法国声音待补]",
            published_at=now,
            content="",
            origin_region="fr",
            is_placeholder=True,
        ),
        db_path=db_path,
    )
    cluster_id = insert_cluster(
        Cluster(
            topic_category="World",
            article_ids=[real_id, placeholder_id],
            summary="story",
            perspectives={},
            report_date="2026-08-01",
        ),
        db_path=db_path,
    )

    assert selected_source_regions("2026-08-01", "2026-08-01", db_path=db_path) == [
        {"cluster_id": cluster_id, "origin_region": "us", "source_name": "Reuters"}
    ]


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


def test_init_db_migrates_llm_cache_usage_columns_race_safely(tmp_path):
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE llm_call_events (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   stage TEXT NOT NULL,
                   model TEXT NOT NULL,
                   report_date TEXT,
                   cluster_key TEXT,
                   item_count INTEGER,
                   attempt INTEGER NOT NULL DEFAULT 1,
                   status TEXT NOT NULL DEFAULT 'ok',
                   finish_reason TEXT,
                   prompt_tokens INTEGER,
                   completion_tokens INTEGER,
                   total_tokens INTEGER,
                   input_chars INTEGER,
                   output_chars INTEGER,
                   duration_ms INTEGER,
                   created_at TEXT NOT NULL DEFAULT (datetime('now'))
               )"""
        )

    init_db(db_path)
    init_db(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(llm_call_events)")}
    assert {"prompt_cache_hit_tokens", "prompt_cache_miss_tokens"} <= columns

    event_id = insert_llm_call_event(
        LLMCallEvent(
            stage="clustering",
            model="deepseek/deepseek-v4-flash",
            prompt_cache_hit_tokens=7,
            prompt_cache_miss_tokens=13,
        ),
        db_path=db_path,
    )
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT prompt_cache_hit_tokens, prompt_cache_miss_tokens "
            "FROM llm_call_events WHERE id = ?",
            (event_id,),
        ).fetchone()
    assert row == (7, 13)
