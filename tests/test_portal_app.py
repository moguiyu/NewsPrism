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
