"""Smoke tests for the portal app via FastAPI TestClient (no network, temp DB)."""
from datetime import datetime, timezone
import pytest
from fastapi.testclient import TestClient

from newsprism.repo.db import (
    init_db, insert_cluster_evaluation, link_cluster_evaluation,
    insert_article, insert_cluster,
)
from newsprism.types import Cluster
from newsprism.runtime.portal.app import create_app


@pytest.fixture
def client(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    # one selected + one candidate evaluation
    insert_cluster_evaluation(report_date="2026-06-14", cluster_key="k1",
        dims={"scope": 8, "severity": 7}, rationale="r", signal=0.5, composite=0.7,
        rank=1, display_category="国际时政", status="publishable", flags=[],
        evaluated_by_llm=True, model="m", subject_regions=["il"], db_path=db)
    insert_cluster_evaluation(report_date="2026-06-14", cluster_key="k2",
        dims={"scope": 3, "severity": 2}, rationale="r2", signal=0.2, composite=0.2,
        rank=2, display_category="体育运动", status="suppress", flags=[],
        evaluated_by_llm=True, model="m", subject_regions=["us"], db_path=db)
    return TestClient(create_app(db_path=db))


def test_index_ok(client):
    r = client.get("/")
    assert r.status_code == 200


def test_day_inspector_lists_rows(client):
    r = client.get("/day?date=2026-06-14")
    assert r.status_code == 200
    assert "国际时政" in r.text and "体育运动" in r.text


def test_matrices_ok(client):
    r = client.get("/matrices?date_from=2026-06-14&date_to=2026-06-14")
    assert r.status_code == 200


from newsprism.repo.db import get_conn


def test_post_verdict_writes_editorial_feedback(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    # selected cluster needs a clusters row + linked evaluation
    cid = insert_cluster(Cluster(topic_category="国际时政", article_ids=[1], summary="s",
        perspectives={}, report_date="2026-06-14"), db_path=db)
    insert_cluster_evaluation(report_date="2026-06-14", cluster_key="k1", dims={"scope": 8},
        rationale="r", signal=0.5, composite=0.7, rank=1, display_category="国际时政",
        status="publishable", flags=[], evaluated_by_llm=True, model="m",
        subject_regions=["il"], db_path=db)
    link_cluster_evaluation("2026-06-14", "k1", cid, selected=True, db_path=db)
    client = TestClient(create_app(db_path=db))
    r = client.post("/api/verdict", json={"cluster_id": cid, "verdict": -1})
    assert r.status_code == 200
    with get_conn(db) as conn:
        row = conn.execute("SELECT verdict, channel FROM editorial_feedback WHERE cluster_id=?", (cid,)).fetchone()
    assert row["verdict"] == -1 and row["channel"] == "portal"


def test_post_correction_writes_row(tmp_path):
    db = tmp_path / "n.db"
    init_db(db)
    eid = insert_cluster_evaluation(report_date="2026-06-14", cluster_key="k2", dims={"scope": 3},
        rationale="r", signal=0.2, composite=0.2, rank=2, display_category="体育运动",
        status="suppress", flags=[], evaluated_by_llm=True, model="m",
        subject_regions=["us"], db_path=db)
    client = TestClient(create_app(db_path=db))
    r = client.post("/api/correction", json={"evaluation_id": eid, "kind": "promote"})
    assert r.status_code == 200
    with get_conn(db) as conn:
        row = conn.execute("SELECT kind FROM feedback_corrections WHERE evaluation_id=?", (eid,)).fetchone()
    assert row["kind"] == "promote"


def test_trends_page_ok(client):
    assert client.get("/trends?date_from=2026-06-01&date_to=2026-06-14").status_code == 200


def test_calibration_page_ok(client):
    assert client.get("/calibration").status_code == 200


def test_sources_page_ok(client):
    assert client.get("/sources?date_from=2026-06-01&date_to=2026-06-14").status_code == 200


def test_bad_numeric_filter_does_not_500(client):
    # non-numeric composite_min must be ignored, not crash the route
    r = client.get("/day?date=2026-06-14&composite_min=abc")
    assert r.status_code == 200


def test_has_feedback_and_composite_max_filters_in_form(client):
    # both backend-wired filters are now exposed in the shared filter form
    html = client.get("/day?date=2026-06-14").text
    assert 'name="composite_max"' in html and 'name="has_feedback"' in html
