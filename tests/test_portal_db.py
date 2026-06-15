"""Tests for portal-related schema: subject_regions + feedback_corrections."""
from newsprism.repo.db import init_db, get_conn


def _columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_subject_regions_column_added(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    with get_conn(db) as conn:
        assert "subject_regions" in _columns(conn, "cluster_evaluations")


def test_feedback_corrections_table_created(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    with get_conn(db) as conn:
        cols = _columns(conn, "feedback_corrections")
    assert {"id", "evaluation_id", "kind", "dimension", "suggested_value", "payload", "channel", "created_at"} <= cols


def test_wal_enabled(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    with get_conn(db) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
