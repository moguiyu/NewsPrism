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

from newsprism.types import Article, Cluster, SearchRequestEvent

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

            CREATE INDEX IF NOT EXISTS idx_articles_source   ON articles(source_name);
            CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);
            CREATE INDEX IF NOT EXISTS idx_articles_clustered ON articles(clustered);
            CREATE INDEX IF NOT EXISTS idx_clusters_date      ON clusters(report_date);
            CREATE INDEX IF NOT EXISTS idx_search_request_events_created_at
                ON search_request_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_search_request_events_provider
                ON search_request_events(provider, request_type);
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
                   storyline_role, storyline_confidence
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            ),
        )
        return cur.lastrowid


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
    )
