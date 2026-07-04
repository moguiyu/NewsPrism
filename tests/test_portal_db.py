"""Tests for portal-related schema: subject_regions + feedback_corrections."""
import json
from datetime import datetime, timezone
from newsprism.repo.db import (
    init_db, get_conn,
    insert_cluster_evaluation, link_cluster_evaluation, insert_feedback_correction,
    list_corrections, query_evaluations, insert_article, insert_cluster,
)
from newsprism.types import Article, Cluster


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


def _seed_eval(db, report_date="2026-06-14", key="k1", composite=0.5, selected=False, cluster_id=None):
    eid = insert_cluster_evaluation(
        report_date=report_date, cluster_key=key, dims={"scope": 7, "severity": 6},
        rationale="r", signal=0.4, composite=composite, rank=1,
        display_category="国际时政", status="publishable", flags=[],
        evaluated_by_llm=True, model="m", subject_regions=["il", "ir"], db_path=db,
    )
    if selected and cluster_id is not None:
        link_cluster_evaluation(report_date, key, cluster_id, selected=True, db_path=db)
    return eid


def test_query_evaluations_returns_subject_regions_and_selected_flag(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    _seed_eval(db, key="k1", composite=0.5)
    rows = query_evaluations("2026-06-14", "2026-06-14", db_path=db)
    assert len(rows) == 1
    assert rows[0]["subject_regions"] == ["il", "ir"]
    assert rows[0]["dims"]["scope"] == 7
    assert rows[0]["selected"] == 0


def test_query_evaluations_returns_gate(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    insert_cluster_evaluation(
        report_date="2026-06-14", cluster_key="kg", dims={"scope": 7},
        rationale="r", signal=0.4, composite=0.6, rank=1,
        display_category="国际时政", status="suppress",
        flags=["ownership_suppressed_all"], evaluated_by_llm=True, model="m",
        subject_regions=["de"], db_path=db,
        gate={"target": "de", "is_home_affairs": True,
              "blocked": ["华尔街见闻", "中国新闻网"], "review": []},
    )
    rows = query_evaluations("2026-06-14", "2026-06-14", db_path=db)
    assert len(rows) == 1
    assert rows[0]["gate"]["target"] == "de"
    assert rows[0]["gate"]["blocked"] == ["华尔街见闻", "中国新闻网"]
    assert rows[0]["gate"]["review"] == []


def test_insert_and_list_corrections(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    eid = _seed_eval(db)
    insert_feedback_correction(eid, "dimension", dimension="severity", suggested_value=8.0, db_path=db)
    insert_feedback_correction(eid, "promote", db_path=db)
    rows = list_corrections(days=30, db_path=db)
    kinds = sorted(r["kind"] for r in rows)
    assert kinds == ["dimension", "promote"]
    assert rows[0]["evaluation_id"] == eid
