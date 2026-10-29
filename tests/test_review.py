"""Tests for the post-publication review channel (review.py)."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from newsprism.repo import (
    get_cluster_audit_for_date,
    init_db,
    insert_cluster_audit,
    insert_cluster_evaluation,
    list_review_verdicts,
)
from newsprism.runtime.review import (
    format_review_list,
    record_verdict,
)


def _db(tmp_path: Path) -> Path:
    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    return db_path


def _eval(db_path: Path, key: str, composite: float, category: str = "World") -> None:
    insert_cluster_evaluation(
        report_date="2026-10-29",
        cluster_key=key,
        dims={"scope": 6.0},
        rationale=f"rationale for {key}",
        signal=0.61,
        composite=composite,
        rank=1,
        display_category=category,
        status="publishable",
        flags=[],
        evaluated_by_llm=True,
        model="m",
        db_path=db_path,
    )


def test_record_verdict_accepts_a_dropped_candidate(tmp_path):
    """The dropped story is exactly what an editor needs to judge."""
    db_path = _db(tmp_path)
    _eval(db_path, "dropped-key", 0.682, "Science & Health")
    insert_cluster_audit(
        [
            {
                "report_date": "2026-10-29",
                "cluster_key": "dropped-key",
                "article_ids": [],
                "source_count": 2,
                "region_count": 2,
                "signal": 0.61,
                "composite": 0.682,
                "display_category": "Science & Health",
                "lane": "main",
                "outcome": "not_selected",
                "reason": "main_limit",
                "rank": 1,
            }
        ],
        db_path=db_path,
    )

    row_id = record_verdict(
        "2026-10-29", "dropped-key", verdict="publish", reason="重大突破", db_path=db_path
    )

    assert row_id > 0
    rows = list_review_verdicts(days=1, db_path=db_path)
    assert rows[0]["verdict"] == 1
    assert rows[0]["stage"] == "dropped"


def test_record_verdict_rejects_an_unknown_key(tmp_path):
    """A typo'd key must not invent a verdict row."""
    db_path = _db(tmp_path)
    _eval(db_path, "real-key", 0.5)

    code = record_verdict(
        "2026-10-29", "typo-key", verdict="publish", db_path=db_path
    )

    assert code == 0
    assert list_review_verdicts(days=1, db_path=db_path) == []


def test_format_review_list_shows_dropped_candidates_first(tmp_path):
    """The listing must put invisible stories in front of the editor."""
    db_path = _db(tmp_path)
    _eval(db_path, "shown-key", 0.90)
    _eval(db_path, "dropped-key", 0.68, "Science & Health")
    insert_cluster_audit(
        [
            {
                "report_date": "2026-10-29",
                "cluster_key": "shown-key",
                "article_ids": [],
                "source_count": 2,
                "region_count": 1,
                "signal": 0.5,
                "composite": 0.90,
                "display_category": "World",
                "lane": "main",
                "outcome": "rendered",
                "reason": "",
                "rank": 5,
            },
            {
                "report_date": "2026-10-29",
                "cluster_key": "dropped-key",
                "article_ids": [],
                "source_count": 2,
                "region_count": 2,
                "signal": 0.61,
                "composite": 0.682,
                "display_category": "Science & Health",
                "lane": "main",
                "outcome": "not_selected",
                "reason": "main_limit",
                "rank": 1,
            },
        ],
        db_path=db_path,
    )

    text = format_review_list("2026-10-29", db_path=db_path)

    assert "dropped-key" in text
    assert "shown-key" in text
    assert text.index("dropped-key") < text.index("shown-key"), "dropped ranked first"
    assert "0.682" in text


def test_format_review_list_marks_unreviewed_and_decided(tmp_path):
    """The editor must see at a glance what still needs a call."""
    db_path = _db(tmp_path)
    _eval(db_path, "k1", 0.5)
    _eval(db_path, "k2", 0.6)
    insert_cluster_audit(
        [
            {
                "report_date": "2026-10-29",
                "cluster_key": key,
                "article_ids": [],
                "source_count": 2,
                "region_count": 1,
                "signal": 0.5,
                "composite": comp,
                "display_category": "World",
                "lane": "main",
                "outcome": "rendered",
                "reason": "",
                "rank": 1,
            }
            for key, comp in (("k1", 0.5), ("k2", 0.6))
        ],
        db_path=db_path,
    )
    record_verdict("2026-10-29", "k1", verdict="drop", db_path=db_path)

    text = format_review_list("2026-10-29", db_path=db_path)

    assert "k1" in text and "已判定" in text
    assert "k2" in text and "待判定" in text


def test_verdicts_on_dropped_candidates_actually_move_a_weight(tmp_path, monkeypatch):
    """The loop must close: review → training rows → a changed weight.

    Before this, `run_calibration` short-circuited with
    `insufficient_feedback` every week because nothing ever wrote feedback, so
    `signal: 0.24` and all six dimension weights were frozen since 2026-06-14.
    A dropped-candidate verdict is the signal that was missing.
    """
    from newsprism.config import Config
    from newsprism.repo import get_calibration_weights, seed_calibration_weights
    from newsprism.service import calibrate

    db_path = _db(tmp_path)
    seed_calibration_weights(
        {
            "scope": 0.16,
            "severity": 0.16,
            "novelty": 0.12,
            "actor_influence": 0.14,
            "decision_relevance": 0.18,
            "feelgood": 0.0,
            "signal": 0.24,
        },
        db_path=db_path,
    )

    # Six accepted dropped stories with high novelty, six rejected with low.
    audit_rows = []
    for index in range(12):
        accepted = index < 6
        key = f"k{index}"
        _eval(db_path, key, 0.7 if accepted else 0.4)
        audit_rows.append(
            {
                "report_date": "2026-10-29",
                "cluster_key": key,
                "article_ids": [],
                "source_count": 2,
                "region_count": 1,
                "signal": 0.5,
                "composite": 0.7 if accepted else 0.4,
                "display_category": "World",
                "lane": "main",
                "outcome": "not_selected",
                "reason": "main_limit",
                "rank": index,
            }
        )
    insert_cluster_audit(audit_rows, db_path=db_path)
    # Novelty is the dimension that separates them.
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        for index in range(12):
            dims = '{"scope": 5.0, "severity": 5.0, "novelty": %s, ' \
                   '"actor_influence": 5.0, "decision_relevance": 5.0}' % (
                       "9.0" if index < 6 else "1.0"
                   )
            conn.execute(
                "UPDATE cluster_evaluations SET dims = ? WHERE cluster_key = ?",
                (dims, f"k{index}"),
            )

    for index in range(12):
        record_verdict(
            "2026-10-29",
            f"k{index}",
            verdict="publish" if index < 6 else "drop",
            db_path=db_path,
        )

    monkeypatch.setattr(calibrate, "_get_review_rows", lambda days: calibrate.get_review_training_rows(days=days, db_path=db_path))
    monkeypatch.setattr(calibrate, "_get_feedback_rows", lambda days: [])
    monkeypatch.setattr(calibrate, "_get_correction_rows", lambda days: [])
    monkeypatch.setattr(calibrate, "_get_weights", lambda: get_calibration_weights(db_path=db_path))
    monkeypatch.setattr(
        calibrate,
        "_get_seeds",
        lambda: {
            "scope": 0.16, "severity": 0.16, "novelty": 0.12,
            "actor_influence": 0.14, "decision_relevance": 0.18, "feelgood": 0.0,
        },
    )
    monkeypatch.setattr(calibrate, "_update_weight", lambda dim, w, reason="": calibrate.update_calibration_weight(dim, w, reason=reason, db_path=db_path))
    monkeypatch.setattr(calibrate, "_insert_policy", lambda text: None)
    monkeypatch.setattr(calibrate, "_call_policy_llm", lambda *a, **k: [])

    result = calibrate.run_calibration(
        Config(
            raw={}, sources=[], topics={}, schedule={}, collection={}, filter={},
            clustering={}, dedup={}, summarizer={}, output={}, active_search={},
            evolution={"calibration": {"min_feedback_to_run": 4}},
        )
    )

    assert result["status"] == "ok", result
    assert result["feedback_count"] == 12
    assert "novelty" in result["weights_changed"], result
    novelty = get_calibration_weights(db_path=db_path)["novelty"]
    assert novelty > 0.12, "accepted dropped stories must raise the novelty weight"


def test_portal_endpoint_records_a_dropped_candidate_verdict(tmp_path, monkeypatch):
    """The portal is the channel an editor actually uses, so it must reach
    dropped candidates too, not just the ones with a cluster row."""
    from fastapi.testclient import TestClient

    from newsprism.runtime.portal.app import create_app

    monkeypatch.setenv("PORTAL_REQUIRE_CF_ACCESS", "false")

    db_path = _db(tmp_path)
    _eval(db_path, "dropped-key", 0.682)
    insert_cluster_audit(
        [
            {
                "report_date": "2026-10-29",
                "cluster_key": "dropped-key",
                "article_ids": [],
                "source_count": 2,
                "region_count": 2,
                "signal": 0.61,
                "composite": 0.682,
                "display_category": "Science & Health",
                "lane": "main",
                "outcome": "not_selected",
                "reason": "main_limit",
                "rank": 1,
            }
        ],
        db_path=db_path,
    )

    client = TestClient(create_app(db_path=db_path))
    response = client.post(
        "/api/review-verdict",
        json={
            "report_date": "2026-10-29",
            "cluster_key": "dropped-key",
            "verdict": 1,
            "reason": "should have run",
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    rows = list_review_verdicts(days=1, db_path=db_path)
    assert rows[0]["cluster_key"] == "dropped-key"
    assert rows[0]["verdict"] == 1
    assert rows[0]["channel"] == "portal"


def test_format_review_list_hides_window_excluded_and_uses_eval_scores(tmp_path):
    """Only candidates that were actually weighed deserve review.

    Running this against production 2026-10-29 listed all 1351 audited clusters
    at composite 0.000 -- 1261 of them were excluded by the candidate window and
    the rest predate the `cluster_audit.composite` column. The listing must show
    the ~90 that were weighed, with the score from their evaluation.
    """
    db_path = _db(tmp_path)
    _eval(db_path, "weighed", 0.682, "Science & Health")
    _eval(db_path, "excluded", 0.10)
    insert_cluster_audit(
        [
            {
                "report_date": "2026-10-29",
                "cluster_key": "weighed",
                "article_ids": [],
                "source_count": 2,
                "region_count": 2,
                "signal": 0.61,
                "composite": None,          # legacy row: column added later
                "display_category": None,
                "lane": "",
                "outcome": "candidate",
                "reason": "",
                "rank": 1,
            },
            {
                "report_date": "2026-10-29",
                "cluster_key": "excluded",
                "article_ids": [],
                "source_count": 1,
                "region_count": 1,
                "signal": 0.1,
                "composite": None,
                "display_category": None,
                "lane": "",
                "outcome": "window_excluded",
                "reason": "candidate_window",
                "rank": None,
            },
        ],
        db_path=db_path,
    )

    text = format_review_list("2026-10-29", db_path=db_path)

    assert "weighed" in text
    assert "excluded" not in text, "window-excluded rows are not reviewable candidates"
    assert "0.682" in text, "fell back to 0.000 instead of the evaluation's score"
    assert "Science & Health" in text
