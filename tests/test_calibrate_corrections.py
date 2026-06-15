"""Promote/demote corrections join the verdict training set; correction notes
reach the policy memo. litellm is monkeypatched."""
from types import SimpleNamespace
from unittest.mock import patch
import newsprism.service.calibrate as cal


def _cfg(min_feedback=2):
    return SimpleNamespace(
        evolution={"calibration": {"training_window_days": 30, "max_step": 0.02,
                                    "weight_bounds": [0.3, 3.0], "min_feedback_to_run": min_feedback,
                                    "policy_max_bullets": 10}},
        litellm_model="m", litellm_api_key="k", litellm_base_url="u",
    )


def _vrow(verdict, severity):
    return {"verdict": verdict, "note": "", "dims": {"severity": severity, "scope": 5,
            "novelty": 5, "actor_influence": 5, "decision_relevance": 5, "feelgood": 0},
            "rationale": "", "cluster_summary": "x"}


def test_promote_rows_combine_with_verdicts(monkeypatch):
    # Two accepts (high severity) from verdicts, one demote (low severity) from corrections.
    monkeypatch.setattr(cal, "_get_feedback_rows", lambda days: [_vrow(1, 9), _vrow(1, 8)])
    monkeypatch.setattr(cal, "_get_correction_rows", lambda days: [
        {**_vrow(-1, 1), "kind": "demote"}])
    monkeypatch.setattr(cal, "_get_weights", lambda: {"severity": 0.16, "scope": 0.16,
        "novelty": 0.12, "actor_influence": 0.14, "decision_relevance": 0.18, "feelgood": 0.0, "signal": 0.24})
    monkeypatch.setattr(cal, "_get_seeds", lambda: {"severity": 0.16, "scope": 0.16,
        "novelty": 0.12, "actor_influence": 0.14, "decision_relevance": 0.18, "feelgood": 0.0, "signal": 0.24})
    updated = {}
    monkeypatch.setattr(cal, "_update_weight", lambda dim, w, reason="": updated.__setitem__(dim, w))
    monkeypatch.setattr(cal, "_call_policy_llm", lambda *a, **k: [])
    monkeypatch.setattr(cal, "_get_all_corrections", lambda days: [])
    result = cal.run_calibration(_cfg())
    assert result["status"] == "ok"
    # severity separates accept(8.5 avg) from reject(1) → its weight moves up
    assert "severity" in result["weights_changed"]
    assert updated["severity"] > 0.16
