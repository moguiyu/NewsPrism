"""Unit tests for calibrate.py — the weekly calibration + editorial-memory engine.

TDD: these tests were written BEFORE the implementation.
All litellm.completion calls are monkeypatched — no network.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import newsprism.service.calibrate as calibrate_mod
from newsprism.repo.db import init_db, seed_calibration_weights


# ─── HELPERS ───────────────────────────────────────────────────────────────────

_SEED_WEIGHTS = {
    "scope": 0.16,
    "severity": 0.16,
    "novelty": 0.12,
    "actor_influence": 0.14,
    "decision_relevance": 0.18,
    "feelgood": 0.0,
    "signal": 0.24,
}

_LLM_DIMS = ("scope", "severity", "novelty", "actor_influence", "decision_relevance", "feelgood")


def _make_cfg(tmp_db: Path, min_feedback: int = 5) -> object:
    """Minimal config-like object used by run_calibration."""
    return SimpleNamespace(
        evolution={
            "calibration": {
                "enabled": True,
                "training_window_days": 30,
                "max_step": 0.02,
                "weight_bounds": [0.3, 3.0],
                "min_feedback_to_run": min_feedback,
                "policy_max_bullets": 10,
            }
        },
        litellm_model="openai/test-model",
        litellm_api_key="test-key",
        litellm_base_url="https://api.test.example.com/v1",
    )


def _make_row(verdict: int, severity: float, other: float = 5.0) -> dict:
    """Build a fake training row with controllable severity."""
    dims = {
        "scope": other,
        "severity": severity,
        "novelty": other,
        "actor_influence": other,
        "decision_relevance": other,
        "feelgood": 0.0,
    }
    return {
        "verdict": verdict,
        "note": "",
        "dims": dims,
        "rationale": "test rationale",
        "composite": 0.5,
        "signal": 0.5,
        "display_category": "国际时政",
        "report_date": "2026-06-01",
        "cluster_summary": "Test cluster summary for calibration",
    }


def _canned_llm_response(bullets: list[str]) -> object:
    """Return a mock litellm.completion response carrying JSON bullets."""
    content = json.dumps({"bullets": bullets})
    return SimpleNamespace(
        choices=[
            SimpleNamespace(message=SimpleNamespace(content=content))
        ]
    )


def _seeded_tmp_db() -> Path:
    """Create a temp DB, init schema, seed all weights, return its path."""
    tmp = Path(tempfile.mktemp(suffix=".db"))
    init_db(tmp)
    seed_calibration_weights(_SEED_WEIGHTS, db_path=tmp)
    return tmp


# ─── TEST: SKIPPED WHEN INSUFFICIENT FEEDBACK ──────────────────────────────────

def test_run_calibration_skipped_when_insufficient_feedback():
    """run_calibration returns status='skipped' when feedback count < min_feedback_to_run."""
    tmp = _seeded_tmp_db()
    cfg = _make_cfg(tmp, min_feedback=10)

    # Only 3 rows — below the threshold of 10.
    few_rows = [_make_row(+1, 8.0), _make_row(-1, 2.0), _make_row(+1, 7.0)]

    with patch.object(calibrate_mod, "_get_feedback_rows", return_value=few_rows), \
         patch("litellm.completion") as mock_llm:
        result = calibrate_mod.run_calibration(cfg)

    assert result["status"] == "skipped"
    assert result["reason"] == "insufficient_feedback"
    assert result["count"] == 3
    mock_llm.assert_not_called()


# ─── TEST: SEVERITY WEIGHT MOVES UP ─────────────────────────────────────────────

def test_severity_weight_increases_when_accepted_rows_have_high_severity():
    """Accepted rows with high severity and rejected with low severity nudge severity weight up."""
    tmp = _seeded_tmp_db()
    cfg = _make_cfg(tmp, min_feedback=5)

    # 6 accepted with severity=9, 6 rejected with severity=2 → clear positive diff
    rows = (
        [_make_row(+1, severity=9.0) for _ in range(6)]
        + [_make_row(-1, severity=2.0) for _ in range(6)]
    )

    bullets = ["高影响力事件优先", "降低例行市场波动权重"]
    canned = _canned_llm_response(bullets)

    with patch.object(calibrate_mod, "_get_feedback_rows", return_value=rows), \
         patch.object(calibrate_mod, "_get_weights", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_get_seeds", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_update_weight") as mock_update, \
         patch("litellm.completion", return_value=canned):
        result = calibrate_mod.run_calibration(cfg)

    assert result["status"] == "ok"
    assert result["feedback_count"] == 12

    # Find the severity call among update calls
    severity_calls = [c for c in mock_update.call_args_list if c.args[0] == "severity"]
    assert len(severity_calls) == 1, "Expected exactly one weight update for severity"

    new_weight = severity_calls[0].args[1]
    seed = _SEED_WEIGHTS["severity"]
    assert new_weight > seed, f"Expected severity weight > seed {seed}, got {new_weight}"
    # Must stay within bounds
    assert new_weight <= seed * 3.0


def test_severity_weight_stays_within_bounds_at_upper_limit():
    """A dimension already at its upper bound does not exceed seed*3.0 after a positive nudge."""
    tmp = _seeded_tmp_db()
    from newsprism.repo.db import update_calibration_weight

    # Push severity to its upper bound
    seed = _SEED_WEIGHTS["severity"]
    upper = seed * 3.0
    update_calibration_weight("severity", upper, reason="pre-test ceiling", db_path=tmp)

    cfg = _make_cfg(tmp, min_feedback=5)

    # All accepted with high severity → nudge would push above ceiling
    rows = [_make_row(+1, severity=9.0) for _ in range(6)] + [_make_row(-1, severity=1.0) for _ in range(6)]
    canned = _canned_llm_response(["test bullet"])

    with patch.object(calibrate_mod, "_get_feedback_rows", return_value=rows), \
         patch.object(calibrate_mod, "_get_weights", return_value={
             "severity": upper, **{k: v for k, v in _SEED_WEIGHTS.items() if k != "severity"}
         }), \
         patch.object(calibrate_mod, "_update_weight") as mock_update, \
         patch.object(calibrate_mod, "_get_seeds", return_value=_SEED_WEIGHTS), \
         patch("litellm.completion", return_value=canned):
        calibrate_mod.run_calibration(cfg)

    severity_calls = [c for c in mock_update.call_args_list if c.args[0] == "severity"]
    # If there is an update call, the new weight must not exceed the bound
    for call in severity_calls:
        new_weight = call.args[1]
        assert new_weight <= seed * 3.0, f"Weight {new_weight} exceeds ceiling {seed * 3.0}"


# ─── TEST: POLICY BULLETS STORED ────────────────────────────────────────────────

def test_policy_bullets_stored_on_success():
    """On successful calibration, policy bullets are returned in the result."""
    tmp = _seeded_tmp_db()
    cfg = _make_cfg(tmp, min_feedback=5)

    rows = (
        [_make_row(+1, severity=8.0) for _ in range(6)]
        + [_make_row(-1, severity=3.0) for _ in range(6)]
    )
    bullets = ["降低例行市场波动权重", "提升半导体供应链事件优先级"]
    canned = _canned_llm_response(bullets)

    with patch.object(calibrate_mod, "_get_feedback_rows", return_value=rows), \
         patch.object(calibrate_mod, "_get_weights", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_get_seeds", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_update_weight"), \
         patch.object(calibrate_mod, "_insert_policy") as mock_policy, \
         patch("litellm.completion", return_value=canned):
        result = calibrate_mod.run_calibration(cfg)

    assert result["status"] == "ok"
    assert result["policy_bullets"] == 2
    mock_policy.assert_called_once()
    stored_text = mock_policy.call_args.args[0]
    assert "降低例行市场波动权重" in stored_text
    assert "提升半导体供应链事件优先级" in stored_text


def test_policy_failure_does_not_block_quantitative_result():
    """LLM failure for policy memo does not fail the whole calibration run."""
    tmp = _seeded_tmp_db()
    cfg = _make_cfg(tmp, min_feedback=5)

    rows = (
        [_make_row(+1, severity=8.0) for _ in range(6)]
        + [_make_row(-1, severity=3.0) for _ in range(6)]
    )

    with patch.object(calibrate_mod, "_get_feedback_rows", return_value=rows), \
         patch.object(calibrate_mod, "_get_weights", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_get_seeds", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_update_weight"), \
         patch.object(calibrate_mod, "_insert_policy") as mock_policy, \
         patch("litellm.completion", side_effect=RuntimeError("network error")):
        result = calibrate_mod.run_calibration(cfg)

    assert result["status"] == "ok", "LLM failure should not change status to error"
    assert result["policy_bullets"] == 0
    mock_policy.assert_not_called()


# ─── TEST: NO SIGNAL DIMS SKIPPED ───────────────────────────────────────────────

def test_dim_with_no_signal_is_not_updated():
    """A dimension where accepted and rejected means are equal gets no weight update."""
    tmp = _seeded_tmp_db()
    cfg = _make_cfg(tmp, min_feedback=5)

    # All rows have the same scope=5.0 → diff = 0 → no nudge for scope
    rows = (
        [_make_row(+1, severity=8.0) for _ in range(6)]
        + [_make_row(-1, severity=3.0) for _ in range(6)]
    )
    # But also set scope identical for accepted/rejected (already the default _make_row uses other=5.0)
    canned = _canned_llm_response(["bullet"])

    with patch.object(calibrate_mod, "_get_feedback_rows", return_value=rows), \
         patch.object(calibrate_mod, "_get_weights", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_get_seeds", return_value=dict(_SEED_WEIGHTS)), \
         patch.object(calibrate_mod, "_update_weight") as mock_update, \
         patch("litellm.completion", return_value=canned):
        calibrate_mod.run_calibration(cfg)

    scope_calls = [c for c in mock_update.call_args_list if c.args[0] == "scope"]
    # scope diff = mean(5.0 for accepted) - mean(5.0 for rejected) = 0 → no call
    assert len(scope_calls) == 0, "scope should not be updated when accept/reject means are equal"


# ─── TEST: RESET ─────────────────────────────────────────────────────────────────

def test_reset_calibration_returns_confirmation_string():
    """reset_calibration returns a string mentioning the count of restored dimensions."""
    tmp = _seeded_tmp_db()

    with patch.object(calibrate_mod, "_reset_weights", return_value=7) as mock_reset:
        result = calibrate_mod.reset_calibration()

    assert isinstance(result, str)
    assert "7" in result
    mock_reset.assert_called_once()


# ─── TEST: SHOW_CALIBRATION FORMAT ──────────────────────────────────────────────

def test_show_calibration_includes_dimension_names():
    """show_calibration output contains all six LLM dimension names."""
    fake_state = [
        {"dimension": dim, "weight": 0.16, "seed": 0.16, "updated_at": "2026-06-01T00:00:00"}
        for dim in _LLM_DIMS
    ]
    with patch.object(calibrate_mod, "_get_calibration_state", return_value=fake_state), \
         patch.object(calibrate_mod, "_get_latest_policy", return_value="• test bullet"):
        text = calibrate_mod.show_calibration()

    for dim in _LLM_DIMS:
        assert dim in text, f"Expected '{dim}' in show_calibration output"
    assert "test bullet" in text


def test_show_calibration_handles_no_policy():
    """show_calibration works when there is no editorial policy yet."""
    fake_state = [
        {"dimension": "severity", "weight": 0.16, "seed": 0.16, "updated_at": "2026-06-01T00:00:00"}
    ]
    with patch.object(calibrate_mod, "_get_calibration_state", return_value=fake_state), \
         patch.object(calibrate_mod, "_get_latest_policy", return_value=None):
        text = calibrate_mod.show_calibration()

    assert isinstance(text, str)
    assert "severity" in text
