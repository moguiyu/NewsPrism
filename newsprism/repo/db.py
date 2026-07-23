"""SQLite persistence — articles and clusters.

Depends on: types (Article, Cluster)
Layer:      repo  (may import types + config; never imports service or runtime)
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

from newsprism.types import Article, Cluster, ClusterQualityReport, ClusterSummary, SearchRequestEvent

DB_PATH = Path("data/newsprism.db")


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")

        def _add_column(table: str, column: str, ddl: str) -> None:
            """Race-safe ALTER TABLE ADD COLUMN.

            Both the newsprism and newsprism-portal containers start
            simultaneously from the same image and call init_db(); without
            this guard, a concurrent add raises "duplicate column name" and
            crashes whichever container loses the race.
            """
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
            except sqlite3.OperationalError as exc:
                if "duplicate column" not in str(exc).lower():
                    raise

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS articles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT    UNIQUE NOT NULL,
                title       TEXT    NOT NULL,
                source_name TEXT    NOT NULL,
                published_at TEXT   NOT NULL,
                content     TEXT    NOT NULL,
                topics      TEXT    NOT NULL DEFAULT '[]',
                embedding   TEXT,               -- JSON float array
                clustered   INTEGER NOT NULL DEFAULT 0,
                is_searched INTEGER NOT NULL DEFAULT 0,
                search_region TEXT DEFAULT NULL,
                source_kind TEXT NOT NULL DEFAULT 'news',
                platform    TEXT,
                account_id  TEXT,
                is_official_source INTEGER NOT NULL DEFAULT 0,
                origin_region TEXT,
                searched_provider TEXT,
                created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS clusters (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_category  TEXT    NOT NULL,
                article_ids     TEXT    NOT NULL,   -- JSON int array
                summary         TEXT    NOT NULL,
                perspectives    TEXT    NOT NULL,   -- JSON {source: text}
                report_date     TEXT    NOT NULL,   -- YYYY-MM-DD
                published_telegram  INTEGER NOT NULL DEFAULT 0,
                published_html      INTEGER NOT NULL DEFAULT 0,
                freshness_state     TEXT    NOT NULL DEFAULT 'new',  -- new|developing|stale
                continues_cluster_id INTEGER,       -- links to previous day's cluster
                storyline_key   TEXT,
                storyline_name  TEXT,
                storyline_role  TEXT    NOT NULL DEFAULT 'none',
                storyline_confidence REAL NOT NULL DEFAULT 0.0,
                storyline_state TEXT    NOT NULL DEFAULT 'emerging',
                quality_status  TEXT    NOT NULL DEFAULT 'unknown',
                quality_score   REAL    NOT NULL DEFAULT 0.0,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS search_request_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                request_type TEXT NOT NULL,
                target_region TEXT,
                query TEXT,
                account_id TEXT,
                http_status INTEGER,
                result_count INTEGER,
                accepted_count INTEGER,
                rejection_reason TEXT,
                rejection_count INTEGER,
                duration_ms INTEGER,
                estimated_cost_usd REAL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS cluster_quality_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                quality_score REAL NOT NULL,
                fact_coverage REAL NOT NULL,
                source_diversity REAL NOT NULL,
                reliability_score REAL NOT NULL,
                bias_risk REAL NOT NULL,
                flags TEXT NOT NULL DEFAULT '[]',
                confirmed_claims TEXT NOT NULL DEFAULT '[]',
                contested_claims TEXT NOT NULL DEFAULT '[]',
                evidence_summary TEXT NOT NULL DEFAULT '',
                decision_status TEXT NOT NULL DEFAULT 'publishable',
                decision_reason TEXT NOT NULL DEFAULT '',
                summary_constraints TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(cluster_id)
            );

            CREATE TABLE IF NOT EXISTS cluster_claims (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id INTEGER NOT NULL,
                claim_uid TEXT NOT NULL,
                text TEXT NOT NULL,
                claim_type TEXT NOT NULL,
                importance REAL NOT NULL,
                source_names TEXT NOT NULL DEFAULT '[]',
                UNIQUE(cluster_id, claim_uid)
            );

            CREATE TABLE IF NOT EXISTS claim_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id INTEGER NOT NULL,
                claim_uid TEXT NOT NULL,
                source_name TEXT NOT NULL,
                stance TEXT NOT NULL,
                excerpt TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL,
                UNIQUE(cluster_id, claim_uid, source_name)
            );

            CREATE TABLE IF NOT EXISTS cluster_evaluations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id INTEGER,                 -- linked after cluster insert; NULL for unselected candidates
                report_date TEXT NOT NULL,
                cluster_key TEXT NOT NULL,
                dims TEXT NOT NULL DEFAULT '{}',    -- JSON {scope, severity, novelty, actor_influence, decision_relevance, feelgood}
                rationale TEXT NOT NULL DEFAULT '',
                signal REAL NOT NULL DEFAULT 0.0,
                composite REAL NOT NULL DEFAULT 0.0,
                rank INTEGER,
                selected INTEGER NOT NULL DEFAULT 0,
                display_category TEXT,
                status TEXT NOT NULL DEFAULT 'publishable',
                flags TEXT NOT NULL DEFAULT '[]',
                evaluated_by_llm INTEGER NOT NULL DEFAULT 1,
                model TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(report_date, cluster_key)
            );

            CREATE TABLE IF NOT EXISTS editorial_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id INTEGER NOT NULL,
                verdict INTEGER NOT NULL,           -- +1 accept / -1 reject
                channel TEXT NOT NULL DEFAULT 'cli',
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS calibration_weights (
                dimension TEXT PRIMARY KEY,
                weight REAL NOT NULL,
                seed REAL NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS calibration_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dimension TEXT NOT NULL,
                old_weight REAL NOT NULL,
                new_weight REAL NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS editorial_policy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version INTEGER NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS feedback_corrections (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                evaluation_id   INTEGER NOT NULL,
                kind            TEXT NOT NULL,        -- dimension | category | promote | demote
                dimension       TEXT,
                suggested_value REAL,
                payload         TEXT NOT NULL DEFAULT '',
                channel         TEXT NOT NULL DEFAULT 'portal',
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS storylines (
                storyline_key TEXT PRIMARY KEY,
                storyline_name TEXT,
                storyline_state TEXT NOT NULL DEFAULT 'emerging',
                last_report_date TEXT NOT NULL,
                quality_score REAL NOT NULL DEFAULT 0.0,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS storyline_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                storyline_key TEXT NOT NULL,
                cluster_id INTEGER,
                event_date TEXT NOT NULL,
                title TEXT NOT NULL,
                storyline_state TEXT NOT NULL DEFAULT 'emerging',
                summary TEXT NOT NULL DEFAULT '',
                quality_score REAL NOT NULL DEFAULT 0.0,
                event_type TEXT NOT NULL DEFAULT 'update',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(storyline_key, cluster_id, event_date, event_type)
            );

            CREATE INDEX IF NOT EXISTS idx_articles_source   ON articles(source_name);
            CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);
            CREATE INDEX IF NOT EXISTS idx_articles_clustered ON articles(clustered);
            CREATE INDEX IF NOT EXISTS idx_clusters_date      ON clusters(report_date);
            CREATE INDEX IF NOT EXISTS idx_search_request_events_created_at
                ON search_request_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_search_request_events_provider
                ON search_request_events(provider, request_type);
            CREATE INDEX IF NOT EXISTS idx_cluster_quality_reports_cluster_id
                ON cluster_quality_reports(cluster_id);
            CREATE INDEX IF NOT EXISTS idx_cluster_claims_cluster_id
                ON cluster_claims(cluster_id);
            CREATE INDEX IF NOT EXISTS idx_claim_evidence_cluster_id
                ON claim_evidence(cluster_id);
            CREATE INDEX IF NOT EXISTS idx_storyline_events_key_date
                ON storyline_events(storyline_key, event_date);
            CREATE INDEX IF NOT EXISTS idx_cluster_evaluations_date
                ON cluster_evaluations(report_date);
            CREATE INDEX IF NOT EXISTS idx_editorial_feedback_cluster
                ON editorial_feedback(cluster_id);
            CREATE INDEX IF NOT EXISTS idx_feedback_corrections_eval
                ON feedback_corrections(evaluation_id);
        """)

        # Migration: Add new columns if they don't exist (for existing databases)
        cursor = conn.execute("PRAGMA table_info(clusters)")
        columns = {row[1] for row in cursor.fetchall()}
        if "freshness_state" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN freshness_state TEXT NOT NULL DEFAULT 'new'")
        if "continues_cluster_id" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN continues_cluster_id INTEGER")
        if "storyline_key" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN storyline_key TEXT")
        if "storyline_name" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN storyline_name TEXT")
        if "storyline_role" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN storyline_role TEXT NOT NULL DEFAULT 'none'")
        if "storyline_confidence" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN storyline_confidence REAL NOT NULL DEFAULT 0.0")
        if "storyline_state" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN storyline_state TEXT NOT NULL DEFAULT 'emerging'")
        if "quality_status" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN quality_status TEXT NOT NULL DEFAULT 'unknown'")
        if "quality_score" not in columns:
            conn.execute("ALTER TABLE clusters ADD COLUMN quality_score REAL NOT NULL DEFAULT 0.0")

        # Migration: Add searched-article metadata to articles table
        cursor = conn.execute("PRAGMA table_info(articles)")
        article_columns = {row[1] for row in cursor.fetchall()}
        if "is_searched" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN is_searched INTEGER NOT NULL DEFAULT 0")
        if "search_region" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN search_region TEXT DEFAULT NULL")
        if "source_kind" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'news'")
        if "platform" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN platform TEXT")
        if "account_id" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN account_id TEXT")
        if "is_official_source" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN is_official_source INTEGER NOT NULL DEFAULT 0")
        if "origin_region" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN origin_region TEXT")
        if "searched_provider" not in article_columns:
            conn.execute("ALTER TABLE articles ADD COLUMN searched_provider TEXT")
        # Issue #5: persist the article-level ownership-gate decision so the
        # portal/audit can show WHICH articles were state-media-suppressed,
        # not just the aggregate cluster-level verdict. ALTER TABLE ADD COLUMN
        # with a constant default is O(1) metadata on SQLite (safe on 3.5GB+ DBs).
        # Uses the race-safe helper because newsprism + newsprism-portal start
        # concurrently and both run init_db().
        if "ownership_suppressed" not in article_columns:
            _add_column(
                "articles",
                "ownership_suppressed",
                "ownership_suppressed INTEGER NOT NULL DEFAULT 0",
            )

        cursor = conn.execute("PRAGMA table_info(search_request_events)")
        search_event_columns = {row[1] for row in cursor.fetchall()}
        if "rejection_reason" not in search_event_columns:
            conn.execute("ALTER TABLE search_request_events ADD COLUMN rejection_reason TEXT")
        if "rejection_count" not in search_event_columns:
            conn.execute("ALTER TABLE search_request_events ADD COLUMN rejection_count INTEGER")

        cursor = conn.execute("PRAGMA table_info(cluster_evaluations)")
        eval_columns = {row[1] for row in cursor.fetchall()}
        if "subject_regions" not in eval_columns:
            conn.execute(
                "ALTER TABLE cluster_evaluations ADD COLUMN subject_regions TEXT NOT NULL DEFAULT '[]'"
            )
        if "gate" not in eval_columns:
            conn.execute(
                "ALTER TABLE cluster_evaluations ADD COLUMN gate TEXT NOT NULL DEFAULT '{}'"
            )


@contextmanager
def get_conn(db_path: Path = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── ARTICLES ──────────────────────────────────────────────────────────────────

def insert_article(article: Article, db_path: Path = DB_PATH) -> int | None:
    """Insert article, return id or None if URL already exists."""
    # published_at is NOT NULL in the schema. Searched articles may have no
    # recoverable publish date (Tavily returns None and the URL has no date
    # segment); fall back to now so the row can be persisted. The freshness
    # gate already accepted the result (trust-the-bound), so "now" is a safe
    # lower bound on recency.
    published_at = article.published_at
    if published_at is None:
        published_at = datetime.now(timezone.utc)
    with get_conn(db_path) as conn:
        try:
            cur = conn.execute(
                """INSERT INTO articles (
                       url, title, source_name, published_at, content, topics, embedding,
                       is_searched, search_region, source_kind, platform, account_id,
                       is_official_source, origin_region, searched_provider, ownership_suppressed
                   )
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    article.url,
                    article.title,
                    article.source_name,
                    published_at.isoformat(),
                    article.content,
                    json.dumps(article.topics, ensure_ascii=False),
                    json.dumps(article.embedding) if article.embedding else None,
                    1 if article.is_searched else 0,
                    article.search_region,
                    article.source_kind,
                    article.platform,
                    article.account_id,
                    1 if article.is_official_source else 0,
                    article.origin_region,
                    article.searched_provider,
                    1 if getattr(article, "ownership_suppressed", False) else 0,
                ),
            )
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None  # duplicate URL


def get_article_id_by_url(url: str, db_path: Path = DB_PATH) -> int | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM articles WHERE url = ?",
            (url,),
        ).fetchone()
    return int(row["id"]) if row else None


def get_unclustered_articles(
    max_age_hours: int = 48,
    db_path: Path = DB_PATH,
) -> list[Article]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM articles
               WHERE clustered = 0
                 AND published_at >= datetime('now', ?)
               ORDER BY published_at DESC""",
            (f"-{max_age_hours} hours",),
        ).fetchall()
    return [_row_to_article(r) for r in rows]


def get_articles_by_ids(ids: list[int], db_path: Path = DB_PATH) -> list[Article]:
    placeholders = ",".join("?" * len(ids))
    with get_conn(db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM articles WHERE id IN ({placeholders})", ids
        ).fetchall()
    return [_row_to_article(r) for r in rows]


def mark_articles_clustered(ids: list[int], db_path: Path = DB_PATH) -> None:
    placeholders = ",".join("?" * len(ids))
    with get_conn(db_path) as conn:
        conn.execute(
            f"UPDATE articles SET clustered = 1 WHERE id IN ({placeholders})", ids
        )


def update_article_embedding(article_id: int, embedding: list[float], db_path: Path = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE articles SET embedding = ? WHERE id = ?",
            (json.dumps(embedding), article_id),
        )


def insert_search_request_event(event: SearchRequestEvent, db_path: Path = DB_PATH) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO search_request_events (
                   provider, request_type, target_region, query, account_id,
                   http_status, result_count, accepted_count, rejection_reason, rejection_count,
                   duration_ms, estimated_cost_usd, created_at
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))""",
            (
                event.provider,
                event.request_type,
                event.target_region,
                event.query,
                event.account_id,
                event.http_status,
                event.result_count,
                event.accepted_count,
                event.rejection_reason,
                event.rejection_count,
                event.duration_ms,
                event.estimated_cost_usd,
                event.created_at.isoformat() if event.created_at else None,
            ),
        )
        return cur.lastrowid


# ─── CLUSTERS ──────────────────────────────────────────────────────────────────

def insert_cluster(cluster: Cluster, db_path: Path = DB_PATH) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO clusters (
                   topic_category, article_ids, summary, perspectives, report_date,
                   freshness_state, continues_cluster_id, storyline_key, storyline_name,
                   storyline_role, storyline_confidence, storyline_state, quality_status,
                   quality_score
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cluster.topic_category,
                json.dumps(cluster.article_ids),
                cluster.summary,
                json.dumps(cluster.perspectives, ensure_ascii=False),
                cluster.report_date,
                cluster.freshness_state,
                cluster.continues_cluster_id,
                cluster.storyline_key,
                cluster.storyline_name,
                cluster.storyline_role,
                cluster.storyline_confidence,
                cluster.storyline_state,
                cluster.quality_status,
                cluster.quality_score,
            ),
        )
        return cur.lastrowid


def insert_cluster_quality_report(
    cluster_id: int,
    report: ClusterQualityReport,
    db_path: Path = DB_PATH,
) -> int:
    """Persist the quality report, claims, and evidence for a stored cluster."""
    with get_conn(db_path) as conn:
        conn.execute("DELETE FROM claim_evidence WHERE cluster_id = ?", (cluster_id,))
        conn.execute("DELETE FROM cluster_claims WHERE cluster_id = ?", (cluster_id,))
        conn.execute("DELETE FROM cluster_quality_reports WHERE cluster_id = ?", (cluster_id,))
        cur = conn.execute(
            """INSERT INTO cluster_quality_reports (
                   cluster_id, status, quality_score, fact_coverage, source_diversity,
                   reliability_score, bias_risk, flags, confirmed_claims, contested_claims,
                   evidence_summary, decision_status, decision_reason, summary_constraints,
                   created_at
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))""",
            (
                cluster_id,
                report.status,
                report.overall_score,
                report.fact_coverage,
                report.source_diversity,
                report.reliability_score,
                report.bias_risk,
                json.dumps(report.flags, ensure_ascii=False),
                json.dumps(report.confirmed_claims, ensure_ascii=False),
                json.dumps(report.contested_claims, ensure_ascii=False),
                report.evidence_summary,
                report.decision.status,
                report.decision.reason,
                json.dumps(report.decision.summary_constraints, ensure_ascii=False),
                report.created_at.isoformat() if report.created_at else None,
            ),
        )
        for claim in report.claims:
            conn.execute(
                """INSERT OR REPLACE INTO cluster_claims (
                       cluster_id, claim_uid, text, claim_type, importance, source_names
                   )
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    cluster_id,
                    claim.claim_id or "",
                    claim.text,
                    claim.claim_type,
                    claim.importance,
                    json.dumps(claim.source_names, ensure_ascii=False),
                ),
            )
        for evidence in report.evidence:
            conn.execute(
                """INSERT OR REPLACE INTO claim_evidence (
                       cluster_id, claim_uid, source_name, stance, excerpt, confidence
                   )
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    cluster_id,
                    evidence.claim_id,
                    evidence.source_name,
                    evidence.stance,
                    evidence.excerpt,
                    evidence.confidence,
                ),
            )
        return cur.lastrowid


def upsert_storyline_state(
    cluster_id: int,
    summary: ClusterSummary,
    report_date: str,
    db_path: Path = DB_PATH,
) -> None:
    storyline_key = summary.storyline_key or summary.macro_topic_key
    if not storyline_key:
        return
    storyline_name = summary.storyline_name or summary.macro_topic_name or summary.short_topic_name
    state = summary.storyline_state or "emerging"
    with get_conn(db_path) as conn:
        conn.execute(
            """INSERT INTO storylines (
                   storyline_key, storyline_name, storyline_state, last_report_date, quality_score, updated_at
               )
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(storyline_key) DO UPDATE SET
                   storyline_name = COALESCE(excluded.storyline_name, storylines.storyline_name),
                   storyline_state = excluded.storyline_state,
                   last_report_date = excluded.last_report_date,
                   quality_score = excluded.quality_score,
                   updated_at = datetime('now')""",
            (
                storyline_key,
                storyline_name,
                state,
                report_date,
                summary.quality_score,
            ),
        )
        events = summary.storyline_timeline or []
        if not events:
            return
        for event in events:
            event_cluster_id = cluster_id if event.event_type == "current" or event.cluster_id is None else event.cluster_id
            conn.execute(
                """INSERT OR REPLACE INTO storyline_events (
                       storyline_key, cluster_id, event_date, title, storyline_state,
                       summary, quality_score, event_type
                   )
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event.storyline_key or storyline_key,
                    event_cluster_id,
                    event.event_date,
                    event.title,
                    event.state,
                    event.summary,
                    event.quality_score,
                    event.event_type,
                ),
            )


def get_clusters_for_date(report_date: str, db_path: Path = DB_PATH) -> list[Cluster]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM clusters WHERE report_date = ? ORDER BY id",
            (report_date,),
        ).fetchall()
    return [_row_to_cluster(r) for r in rows]


def get_report_article_ids(report_date: str, db_path: Path = DB_PATH) -> list[int]:
    clusters = get_clusters_for_date(report_date, db_path=db_path)
    article_ids = {
        article_id
        for cluster in clusters
        for article_id in cluster.article_ids
        if isinstance(article_id, int)
    }
    return sorted(article_ids)


def get_recent_clusters(days: int = 3, anchor_date: str | None = None, db_path: Path = DB_PATH) -> list[Cluster]:
    """Get clusters from the past N days for cross-day deduplication."""
    if days <= 0:
        return []
    with get_conn(db_path) as conn:
        if anchor_date:
            rows = conn.execute(
                """SELECT * FROM clusters
                   WHERE report_date >= date(?, ?)
                     AND report_date < date(?)
                   ORDER BY report_date DESC, id""",
                (anchor_date, f"-{days} days", anchor_date),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM clusters
                   WHERE report_date >= date('now', ?)
                   ORDER BY report_date DESC, id""",
                (f"-{days} days",),
            ).fetchall()
    return [_row_to_cluster(r) for r in rows]



def delete_clusters_for_date(report_date: str, db_path: Path = DB_PATH) -> int:
    with get_conn(db_path) as conn:
        cluster_ids = [
            int(row["id"])
            for row in conn.execute("SELECT id FROM clusters WHERE report_date = ?", (report_date,)).fetchall()
        ]
        if cluster_ids:
            placeholders = ",".join("?" * len(cluster_ids))
            conn.execute(f"DELETE FROM claim_evidence WHERE cluster_id IN ({placeholders})", cluster_ids)
            conn.execute(f"DELETE FROM cluster_claims WHERE cluster_id IN ({placeholders})", cluster_ids)
            conn.execute(f"DELETE FROM cluster_quality_reports WHERE cluster_id IN ({placeholders})", cluster_ids)
            conn.execute(f"DELETE FROM storyline_events WHERE cluster_id IN ({placeholders})", cluster_ids)
        cur = conn.execute("DELETE FROM clusters WHERE report_date = ?", (report_date,))
        return cur.rowcount


def reset_articles_clustered(ids: list[int], db_path: Path = DB_PATH) -> int:
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    with get_conn(db_path) as conn:
        cur = conn.execute(
            f"UPDATE articles SET clustered = 0 WHERE id IN ({placeholders})",
            ids,
        )
        return cur.rowcount


# ─── IMPACT EVALUATION / EVOLUTION ─────────────────────────────────────────────

def insert_cluster_evaluation(
    report_date: str,
    cluster_key: str,
    dims: dict[str, float],
    rationale: str,
    signal: float,
    composite: float,
    rank: int | None,
    display_category: str | None,
    status: str,
    flags: list[str],
    evaluated_by_llm: bool,
    model: str | None,
    subject_regions: list[str] | None = None,
    gate: dict | None = None,
    db_path: Path = DB_PATH,
) -> int:
    """Persist one cluster's impact evaluation (upsert on report_date+cluster_key)."""
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO cluster_evaluations (
                   report_date, cluster_key, dims, rationale, signal, composite,
                   rank, display_category, status, flags, evaluated_by_llm, model,
                   subject_regions, gate
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(report_date, cluster_key) DO UPDATE SET
                   dims = excluded.dims,
                   rationale = excluded.rationale,
                   signal = excluded.signal,
                   composite = excluded.composite,
                   rank = excluded.rank,
                   display_category = excluded.display_category,
                   status = excluded.status,
                   flags = excluded.flags,
                   evaluated_by_llm = excluded.evaluated_by_llm,
                   model = excluded.model,
                   subject_regions = excluded.subject_regions,
                   gate = excluded.gate,
                   cluster_id = NULL,
                   selected = 0""",
            (
                report_date,
                cluster_key,
                json.dumps(dims, ensure_ascii=False),
                rationale,
                signal,
                composite,
                rank,
                display_category,
                status,
                json.dumps(flags, ensure_ascii=False),
                1 if evaluated_by_llm else 0,
                model,
                json.dumps(list(subject_regions or []), ensure_ascii=False),
                json.dumps(gate or {}, ensure_ascii=False),
            ),
        )
        return cur.lastrowid


def link_cluster_evaluation(
    report_date: str,
    cluster_key: str,
    cluster_id: int,
    selected: bool = True,
    db_path: Path = DB_PATH,
) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            """UPDATE cluster_evaluations
               SET cluster_id = ?, selected = ?
               WHERE report_date = ? AND cluster_key = ?""",
            (cluster_id, 1 if selected else 0, report_date, cluster_key),
        )


def get_calibration_weights(db_path: Path = DB_PATH) -> dict[str, float]:
    with get_conn(db_path) as conn:
        rows = conn.execute("SELECT dimension, weight FROM calibration_weights").fetchall()
    return {row["dimension"]: float(row["weight"]) for row in rows}


def seed_calibration_weights(weights: dict[str, float], db_path: Path = DB_PATH) -> None:
    """Insert seed weights for dimensions that have no calibrated value yet."""
    with get_conn(db_path) as conn:
        for dimension, weight in weights.items():
            conn.execute(
                """INSERT INTO calibration_weights (dimension, weight, seed)
                   VALUES (?, ?, ?)
                   ON CONFLICT(dimension) DO NOTHING""",
                (dimension, float(weight), float(weight)),
            )


def get_latest_editorial_policy(db_path: Path = DB_PATH) -> str | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT content FROM editorial_policy ORDER BY version DESC, id DESC LIMIT 1"
        ).fetchone()
    return row["content"] if row else None


# ─── EVOLUTION: FEEDBACK / CALIBRATION (P2) ────────────────────────────────────

def insert_editorial_feedback(
    cluster_id: int,
    verdict: int,
    channel: str = "cli",
    note: str = "",
    db_path: Path = DB_PATH,
) -> int:
    """Record one editor accept (+1) / reject (-1) signal for a published cluster."""
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO editorial_feedback (cluster_id, verdict, channel, note) VALUES (?, ?, ?, ?)",
            (cluster_id, 1 if verdict >= 0 else -1, channel, note),
        )
        return cur.lastrowid


def list_editorial_feedback(limit: int = 50, db_path: Path = DB_PATH) -> list[dict[str, object]]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT f.id, f.cluster_id, f.verdict, f.channel, f.note, f.created_at,
                      c.summary AS cluster_summary, c.report_date
               FROM editorial_feedback f
               LEFT JOIN clusters c ON c.id = f.cluster_id
               ORDER BY f.id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_feedback_training_rows(days: int = 30, db_path: Path = DB_PATH) -> list[dict[str, object]]:
    """Join feedback to its cluster's impact evaluation — the calibration training set.

    Returns one row per feedback signal with the evaluated dimensions, composite,
    signal, rationale, and the verdict (+1/-1).
    """
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT f.verdict, f.note, e.dims, e.rationale, e.composite, e.signal,
                      e.display_category, c.report_date, c.summary AS cluster_summary
               FROM editorial_feedback f
               JOIN clusters c ON c.id = f.cluster_id
               JOIN cluster_evaluations e
                    ON e.cluster_id = c.id AND e.report_date = c.report_date
               WHERE f.created_at >= datetime('now', ?)""",
            (f"-{days} days",),
        ).fetchall()
    result: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        try:
            item["dims"] = json.loads(item["dims"]) if item.get("dims") else {}
        except (TypeError, ValueError):
            item["dims"] = {}
        result.append(item)
    return result


def get_calibration_state(db_path: Path = DB_PATH) -> list[dict[str, object]]:
    """Current weight, seed, and last-updated per dimension (for `calibrate show`)."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT dimension, weight, seed, updated_at FROM calibration_weights ORDER BY dimension"
        ).fetchall()
    return [dict(row) for row in rows]


def update_calibration_weight(
    dimension: str,
    new_weight: float,
    reason: str = "",
    db_path: Path = DB_PATH,
) -> None:
    """Set a dimension weight and append a calibration_log entry (audit + reset)."""
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT weight FROM calibration_weights WHERE dimension = ?", (dimension,)
        ).fetchone()
        old_weight = float(row["weight"]) if row else 0.0
        conn.execute(
            """INSERT INTO calibration_weights (dimension, weight, seed, updated_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(dimension) DO UPDATE SET weight = excluded.weight, updated_at = datetime('now')""",
            (dimension, float(new_weight), float(new_weight)),
        )
        conn.execute(
            "INSERT INTO calibration_log (dimension, old_weight, new_weight, reason) VALUES (?, ?, ?, ?)",
            (dimension, old_weight, float(new_weight), reason),
        )


def reset_calibration_weights(db_path: Path = DB_PATH) -> int:
    """Restore every weight to its seed; log each reset. Returns count reset."""
    with get_conn(db_path) as conn:
        rows = conn.execute("SELECT dimension, weight, seed FROM calibration_weights").fetchall()
        for row in rows:
            if float(row["weight"]) != float(row["seed"]):
                conn.execute(
                    "INSERT INTO calibration_log (dimension, old_weight, new_weight, reason) VALUES (?, ?, ?, ?)",
                    (row["dimension"], float(row["weight"]), float(row["seed"]), "reset"),
                )
            conn.execute(
                "UPDATE calibration_weights SET weight = seed, updated_at = datetime('now') WHERE dimension = ?",
                (row["dimension"],),
            )
        return len(rows)


def insert_editorial_policy(content: str, db_path: Path = DB_PATH) -> int:
    """Append a new editorial-policy version (the persistent editorial memory)."""
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT COALESCE(MAX(version), 0) AS v FROM editorial_policy").fetchone()
        version = int(row["v"]) + 1
        cur = conn.execute(
            "INSERT INTO editorial_policy (version, content) VALUES (?, ?)",
            (version, content),
        )
        return cur.lastrowid


# ─── PORTAL: CORRECTIONS + READ AGGREGATIONS ───────────────────────────────────

def insert_feedback_correction(
    evaluation_id: int,
    kind: str,
    dimension: str | None = None,
    suggested_value: float | None = None,
    payload: str = "",
    channel: str = "portal",
    db_path: Path = DB_PATH,
) -> int:
    """Record one structured correction against a cluster_evaluations row."""
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO feedback_corrections
                   (evaluation_id, kind, dimension, suggested_value, payload, channel)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (evaluation_id, kind, dimension, suggested_value, payload, channel),
        )
        return cur.lastrowid


def list_corrections(days: int = 30, db_path: Path = DB_PATH) -> list[dict[str, object]]:
    """Recent corrections joined to their evaluation's dims (for memo + analytics)."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT fc.id, fc.evaluation_id, fc.kind, fc.dimension, fc.suggested_value,
                      fc.payload, fc.channel, fc.created_at,
                      e.dims, e.display_category, e.composite, e.report_date
               FROM feedback_corrections fc
               JOIN cluster_evaluations e ON e.id = fc.evaluation_id
               WHERE fc.created_at >= datetime('now', ?)
               ORDER BY fc.id DESC""",
            (f"-{days} days",),
        ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        try:
            item["dims"] = json.loads(item["dims"]) if item.get("dims") else {}
        except (TypeError, ValueError):
            item["dims"] = {}
        result.append(item)
    return result


def query_evaluations(
    date_from: str,
    date_to: str,
    db_path: Path = DB_PATH,
) -> list[dict[str, object]]:
    """All evaluations (selected + candidates) in [date_from, date_to], with the
    cluster summary (selected only), parsed dims/flags/subject_regions, and the
    latest verdict for selected clusters. Caller filters further in Python."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT e.id, e.cluster_id, e.report_date, e.cluster_key, e.dims,
                      e.rationale, e.signal, e.composite, e.rank, e.selected,
                      e.display_category, e.status, e.flags, e.subject_regions, e.gate,
                      e.evaluated_by_llm, c.summary AS cluster_summary, c.article_ids,
                      (SELECT f.verdict FROM editorial_feedback f
                       WHERE f.cluster_id = e.cluster_id
                       ORDER BY f.id DESC LIMIT 1) AS verdict
               FROM cluster_evaluations e
               LEFT JOIN clusters c ON c.id = e.cluster_id
               WHERE e.report_date BETWEEN ? AND ?
               ORDER BY e.composite DESC""",
            (date_from, date_to),
        ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        for key in ("dims", "flags", "subject_regions", "gate"):
            default = {} if key in ("dims", "gate") else []
            try:
                item[key] = json.loads(item[key]) if item.get(key) else default
            except (TypeError, ValueError):
                item[key] = default
        result.append(item)
    return result


def selected_source_regions(
    date_from: str,
    date_to: str,
    db_path: Path = DB_PATH,
) -> list[dict[str, object]]:
    """For selected clusters in the window, expand article_ids → (cluster_id,
    origin_region, source_name). Used for source-country matrices/source review."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT c.id AS cluster_id, a.origin_region, a.source_name
               FROM clusters c, json_each(c.article_ids) j
               JOIN articles a ON a.id = j.value
               WHERE c.report_date BETWEEN ? AND ?""",
            (date_from, date_to),
        ).fetchall()
    return [dict(row) for row in rows]


def get_correction_training_rows(days: int = 30, db_path: Path = DB_PATH) -> list[dict[str, object]]:
    """promote/demote corrections as accept/reject training rows (dims from the
    evaluation itself — no clusters join, so candidates are included)."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT fc.kind, fc.payload AS note, e.dims, e.rationale,
                      e.composite, e.signal, e.display_category, e.report_date
               FROM feedback_corrections fc
               JOIN cluster_evaluations e ON e.id = fc.evaluation_id
               WHERE fc.kind IN ('promote', 'demote')
                 AND fc.created_at >= datetime('now', ?)""",
            (f"-{days} days",),
        ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["verdict"] = 1 if item["kind"] == "promote" else -1
        try:
            item["dims"] = json.loads(item["dims"]) if item.get("dims") else {}
        except (TypeError, ValueError):
            item["dims"] = {}
        result.append(item)
    return result


def delete_old_unclustered_articles(days: int = 30, db_path: Path = DB_PATH) -> int:
    """Retention: drop unclustered articles older than `days`. Clustered rows are kept
    (replay depends on them). Returns rows deleted."""
    with get_conn(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM articles WHERE clustered = 0 AND published_at < datetime('now', ?)",
            (f"-{days} days",),
        )
        return cur.rowcount


# ─── HELPERS ───────────────────────────────────────────────────────────────────

def _row_to_article(row: sqlite3.Row) -> Article:
    return Article(
        id=row["id"],
        url=row["url"],
        title=row["title"],
        source_name=row["source_name"],
        published_at=datetime.fromisoformat(row["published_at"]),
        content=row["content"],
        topics=json.loads(row["topics"]),
        embedding=json.loads(row["embedding"]) if row["embedding"] else None,
        clustered=bool(row["clustered"]),
        is_searched=bool(row["is_searched"]) if "is_searched" in row.keys() else False,
        search_region=row["search_region"] if "search_region" in row.keys() else None,
        source_kind=row["source_kind"] if "source_kind" in row.keys() else "news",
        platform=row["platform"] if "platform" in row.keys() else None,
        account_id=row["account_id"] if "account_id" in row.keys() else None,
        is_official_source=bool(row["is_official_source"]) if "is_official_source" in row.keys() else False,
        origin_region=row["origin_region"] if "origin_region" in row.keys() else None,
        searched_provider=row["searched_provider"] if "searched_provider" in row.keys() else None,
        ownership_suppressed=bool(row["ownership_suppressed"]) if "ownership_suppressed" in row.keys() else False,
    )


def _row_to_cluster(row: sqlite3.Row) -> Cluster:
    return Cluster(
        id=row["id"],
        topic_category=row["topic_category"],
        article_ids=json.loads(row["article_ids"]),
        summary=row["summary"],
        perspectives=json.loads(row["perspectives"]),
        report_date=row["report_date"],
        freshness_state=row["freshness_state"] if "freshness_state" in row.keys() else "new",
        continues_cluster_id=row["continues_cluster_id"] if "continues_cluster_id" in row.keys() else None,
        storyline_key=row["storyline_key"] if "storyline_key" in row.keys() else None,
        storyline_name=row["storyline_name"] if "storyline_name" in row.keys() else None,
        storyline_role=row["storyline_role"] if "storyline_role" in row.keys() else "none",
        storyline_confidence=float(row["storyline_confidence"]) if "storyline_confidence" in row.keys() else 0.0,
        storyline_state=row["storyline_state"] if "storyline_state" in row.keys() else "emerging",
        quality_status=row["quality_status"] if "quality_status" in row.keys() else "unknown",
        quality_score=float(row["quality_score"]) if "quality_score" in row.keys() else 0.0,
    )
