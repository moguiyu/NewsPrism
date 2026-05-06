"""SQLite persistence — articles and clusters.

Depends on: types (Article, Cluster)
Layer:      repo  (may import types + config; never imports service or runtime)
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator

from newsprism.types import Article, Cluster, ClusterQualityReport, ClusterSummary, SearchRequestEvent

DB_PATH = Path("data/newsprism.db")


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
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

        cursor = conn.execute("PRAGMA table_info(search_request_events)")
        search_event_columns = {row[1] for row in cursor.fetchall()}
        if "rejection_reason" not in search_event_columns:
            conn.execute("ALTER TABLE search_request_events ADD COLUMN rejection_reason TEXT")
        if "rejection_count" not in search_event_columns:
            conn.execute("ALTER TABLE search_request_events ADD COLUMN rejection_count INTEGER")


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
    with get_conn(db_path) as conn:
        try:
            cur = conn.execute(
                """INSERT INTO articles (
                       url, title, source_name, published_at, content, topics, embedding,
                       is_searched, search_region, source_kind, platform, account_id,
                       is_official_source, origin_region, searched_provider
                   )
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    article.url,
                    article.title,
                    article.source_name,
                    article.published_at.isoformat(),
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


def mark_cluster_published(cluster_id: int, channel: str, db_path: Path = DB_PATH) -> None:
    _CHANNEL_COL = {"telegram": "published_telegram", "html": "published_html"}
    if channel not in _CHANNEL_COL:
        raise ValueError(f"Invalid channel: {channel!r}; must be one of {list(_CHANNEL_COL)}")
    col = _CHANNEL_COL[channel]
    with get_conn(db_path) as conn:
        conn.execute(f"UPDATE clusters SET {col} = 1 WHERE id = ?", (cluster_id,))


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
