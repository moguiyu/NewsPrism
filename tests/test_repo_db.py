"""Tests for SQLite persistence of searched articles and telemetry."""
import json
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


def test_init_db_migrates_llm_billed_cost_column_race_safely(tmp_path):
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
    assert "billed_cost_usd" in columns

    event_id = insert_llm_call_event(
        LLMCallEvent(
            stage="clustering",
            model="openai/deepseek/deepseek-v4-flash",
            billed_cost_usd=0.000123,
        ),
        db_path=db_path,
    )
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT billed_cost_usd FROM llm_call_events WHERE id = ?",
            (event_id,),
        ).fetchone()
    assert row == (0.000123,)


# ─── Retention must not be defeated by the clustered flag ───────────────────


def _insert_article(db_path, url: str, published_at: datetime, clustered: bool = False) -> int:
    return insert_article(
        Article(
            url=url,
            title=url,
            source_name="Reuters",
            published_at=published_at,
            content="body " * 40,
        ),
        db_path=db_path,
    )


def _mark_clustered(db_path, ids):
    from newsprism.repo import mark_articles_clustered

    mark_articles_clustered(list(ids), db_path=db_path)


def test_retention_deletes_old_clustered_articles(tmp_path):
    """Phase 0 marks every article clustered, so a clustered-only exemption
    would make retention a no-op and grow the database without bound.

    Measured on production 2026-10-28: 21,973 articles were clustered AND older
    than 30 days, while the old rule only matched 975 rows.
    """
    from newsprism.repo import delete_old_articles

    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    old = _insert_article(db_path, "https://x.com/old", datetime(2026, 1, 1, tzinfo=timezone.utc))
    fresh = _insert_article(db_path, "https://x.com/new", datetime.now(tz=timezone.utc))
    _mark_clustered(db_path, [old, fresh])

    deleted = delete_old_articles(days=90, db_path=db_path)

    assert deleted == 1
    remaining = {row[0] for row in sqlite3.connect(db_path).execute("select url from articles")}
    assert remaining == {"https://x.com/new"}


def test_retention_keeps_articles_referenced_by_a_published_cluster(tmp_path):
    """Replay and audit depend on the articles behind a published card."""
    from newsprism.repo import delete_old_articles

    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    old = _insert_article(db_path, "https://x.com/kept", datetime(2026, 1, 1, tzinfo=timezone.utc))
    other_old = _insert_article(
        db_path, "https://x.com/dropped", datetime(2026, 1, 2, tzinfo=timezone.utc)
    )
    cluster_id = insert_cluster(
        Cluster(
            topic_category="Event",
            article_ids=[old],
            summary="summary",
            perspectives={},
            report_date="2026-01-01",
            freshness_state="new",
            continues_cluster_id=None,
            storyline_key=None,
            storyline_name=None,
            storyline_role="none",
            storyline_confidence=0.0,
            storyline_state="emerging",
            quality_status="publishable",
            quality_score=1.0,
        ),
        db_path=db_path,
    )
    from newsprism.repo import mark_clusters_published

    mark_clusters_published([cluster_id], html=True, db_path=db_path)

    deleted = delete_old_articles(days=90, db_path=db_path)

    assert deleted == 1
    remaining = {row[0] for row in sqlite3.connect(db_path).execute("select url from articles")}
    assert remaining == {"https://x.com/kept"}, "the published card's article must survive"
    assert "https://x.com/dropped" not in remaining


# ─── Audit persistence: every clustering decision must be recorded ──────────


def test_insert_cluster_audit_records_dropped_clusters(tmp_path):
    """A cluster the window discarded must still be recoverable later.

    Production 2026-10-28 formed 1354 validated clusters, persisted 29, and threw
    the other 1305 away with no row, so "why is X not on the page" could not be
    answered after the fact.
    """
    from newsprism.repo import (
        insert_cluster_audit,
        get_cluster_audit_for_date,
    )

    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    article = _insert_article(
        db_path, "https://x.com/a", datetime.now(tz=timezone.utc)
    )

    insert_cluster_audit(
        [
            {
                "report_date": "2026-10-28",
                "cluster_key": "abc123",
                "article_ids": [article],
                "source_count": 1,
                "region_count": 0,
                "signal": 0.417,
                "outcome": "window_excluded",
                "reason": "candidate_window",
                "rank": 121,
            }
        ],
        db_path=db_path,
    )

    rows = get_cluster_audit_for_date("2026-10-28", db_path=db_path)
    assert len(rows) == 1
    assert rows[0]["cluster_key"] == "abc123"
    assert rows[0]["outcome"] == "window_excluded"
    assert rows[0]["reason"] == "candidate_window"
    assert rows[0]["rank"] == 121


def test_cluster_audit_upsert_is_idempotent(tmp_path):
    """The same key may be recorded twice (e.g. re-run); the last word wins."""
    from newsprism.repo import insert_cluster_audit, get_cluster_audit_for_date

    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    row = {
        "report_date": "2026-10-28",
        "cluster_key": "abc123",
        "article_ids": [],
        "source_count": 2,
        "region_count": 1,
        "signal": 0.53,
        "outcome": "rendered",
        "reason": "",
        "rank": 3,
    }
    insert_cluster_audit([row], db_path=db_path)
    insert_cluster_audit([{**row, "outcome": "display_dup", "reason": "embedding_similarity"}], db_path=db_path)

    rows = get_cluster_audit_for_date("2026-10-28", db_path=db_path)
    assert len(rows) == 1
    assert rows[0]["outcome"] == "display_dup"


def test_cluster_audit_outcomes_are_updated_after_selection(tmp_path):
    """The audit row must end with the real reason, not the window verdict."""
    from newsprism.repo import (
        insert_cluster_audit,
        get_cluster_audit_for_date,
        update_cluster_audit_outcomes,
    )

    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    base = {
        "report_date": "2026-10-28",
        "article_ids": [],
        "source_count": 2,
        "region_count": 1,
        "signal": 0.53,
        "rank": 5,
    }
    insert_cluster_audit(
        [
            {**base, "cluster_key": "k1", "outcome": "candidate", "reason": ""},
            {**base, "cluster_key": "k2", "outcome": "candidate", "reason": ""},
            {**base, "cluster_key": "k3", "outcome": "window_excluded", "reason": "candidate_window"},
        ],
        db_path=db_path,
    )

    update_cluster_audit_outcomes(
        "2026-10-28",
        {"k1": ("rendered", ""), "k2": ("display_dup", "embedding_similarity")},
        db_path=db_path,
    )

    rows = {row["cluster_key"]: row for row in get_cluster_audit_for_date("2026-10-28", db_path=db_path)}
    assert rows["k1"]["outcome"] == "rendered"
    assert rows["k2"]["outcome"] == "display_dup"
    assert rows["k2"]["reason"] == "embedding_similarity"
    assert rows["k3"]["outcome"] == "window_excluded", "untouched rows keep their verdict"


def test_init_db_adds_selection_score_columns_to_cluster_audit(tmp_path):
    """A pre-existing audit table must gain the selection diagnostics.

    The audit row is written before scoring, so it can only carry `signal`.
    Without these columns an operator cannot tell whether a high-scoring story
    lost to the lane quota or to the per-category cap.
    """
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE cluster_audit (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   report_date    TEXT    NOT NULL,
                   cluster_key    TEXT    NOT NULL,
                   article_ids    TEXT    NOT NULL DEFAULT '[]',
                   source_count   INTEGER NOT NULL DEFAULT 0,
                   region_count   INTEGER NOT NULL DEFAULT 0,
                   signal         REAL    NOT NULL DEFAULT 0.0,
                   outcome        TEXT    NOT NULL,
                   reason         TEXT    NOT NULL DEFAULT '',
                   rank           INTEGER,
                   created_at     TEXT    NOT NULL DEFAULT (datetime('now')),
                   UNIQUE (report_date, cluster_key)
               )"""
        )

    init_db(db_path)
    init_db(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(cluster_audit)")}
    assert {"composite", "display_category", "lane"} <= columns


def test_backfill_cluster_audit_scores_explains_a_dropped_candidate(tmp_path):
    """A dropped high-scoring story must be explainable from the audit row alone.

    Production 2026-10-29: c51dc6d4671776dd scored 0.682 as the top-ranked
    candidate yet never reached the page, and `cluster_audit` held only its
    local `signal`, so the reason was unrecoverable.
    """
    from newsprism.repo import (
        backfill_cluster_audit_scores,
        get_cluster_audit_for_date,
        insert_cluster_audit,
    )

    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    base = {
        "report_date": "2026-10-29",
        "article_ids": [],
        "source_count": 4,
        "region_count": 2,
        "outcome": "not_selected",
        "reason": "lane_quota",
        "rank": 1,
    }
    insert_cluster_audit(
        [
            {**base, "cluster_key": "dropped", "signal": 0.610},
            {**base, "cluster_key": "window_only", "outcome": "window_excluded",
             "reason": "candidate_window", "signal": 0.30},
        ],
        db_path=db_path,
    )

    changed = backfill_cluster_audit_scores(
        [
            {
                "report_date": "2026-10-29",
                "cluster_key": "dropped",
                "composite": 0.682,
                "display_category": "Science & Health",
                "lane": "main",
            },
            {
                "report_date": "2026-10-29",
                "cluster_key": "never_written",
                "composite": 0.9,
                "display_category": "World",
                "lane": "main",
            },
        ],
        db_path=db_path,
    )

    assert changed == 1, "only the row that exists is updated"
    rows = {
        row["cluster_key"]: row
        for row in get_cluster_audit_for_date("2026-10-29", db_path=db_path)
    }
    assert rows["dropped"]["composite"] == 0.682
    assert rows["dropped"]["display_category"] == "Science & Health"
    assert rows["dropped"]["lane"] == "main"
    assert rows["dropped"]["reason"] == "lane_quota"
    assert rows["window_only"]["composite"] is None, "unscored rows stay null"


def test_retention_removes_old_unpublished_clusters_without_dangling_refs(tmp_path):
    """Deleting articles must not leave cluster rows pointing at nothing.

    Production 2026-10-28: 4,436 unpublished cluster rows older than 90 days
    referenced 16,834 articles that age-based retention would delete. A row whose
    article_ids resolve to nothing breaks replay and the ownership audit, so the
    never-published clusters go with their articles.
    """
    from newsprism.repo import delete_old_articles

    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    old_article = _insert_article(
        db_path, "https://x.com/old", datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    fresh_article = _insert_article(
        db_path, "https://x.com/new", datetime.now(tz=timezone.utc)
    )

    def cluster(article_id: int, report_date: str) -> int:
        return insert_cluster(
            Cluster(
                topic_category="Event",
                article_ids=[article_id],
                summary="summary",
                perspectives={},
                report_date=report_date,
                freshness_state="new",
                continues_cluster_id=None,
                storyline_key=None,
                storyline_name=None,
                storyline_role="none",
                storyline_confidence=0.0,
                storyline_state="emerging",
                quality_status="publishable",
                quality_score=1.0,
            ),
            db_path=db_path,
        )

    old_unpublished = cluster(old_article, "2026-01-01")
    fresh_cluster = cluster(fresh_article, datetime.now(tz=timezone.utc).date().isoformat())
    old_published_article = _insert_article(
        db_path, "https://x.com/published", datetime(2026, 1, 5, tzinfo=timezone.utc)
    )
    old_published = cluster(old_published_article, "2026-01-05")
    from newsprism.repo import mark_clusters_published

    mark_clusters_published([old_published], html=True, db_path=db_path)

    delete_old_articles(days=90, db_path=db_path)

    con = sqlite3.connect(db_path)
    remaining_cluster_ids = {row[0] for row in con.execute("select id from clusters")}
    assert old_unpublished not in remaining_cluster_ids, "unpublished old cluster must go"
    assert fresh_cluster in remaining_cluster_ids
    assert old_published in remaining_cluster_ids, "published cluster is kept for replay"

    # no cluster row may reference a missing article
    for cid, article_ids in con.execute("select id, article_ids from clusters"):
        for aid in json.loads(article_ids):
            assert (
                con.execute("select 1 from articles where id = ?", (aid,)).fetchone()
                is not None
            ), f"cluster {cid} points at deleted article {aid}"
