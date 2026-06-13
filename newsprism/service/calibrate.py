"""Weekly calibration + editorial-memory engine.

Reads editor accept/reject feedback joined to impact scores, nudges the six
LLM-scored dimension weights toward what the editor accepts, and distills the
week's feedback into a persistent editorial-policy memo injected into future
impact prompts.

Public API:
    run_calibration(cfg) -> dict
    show_calibration() -> str
    reset_calibration() -> str

Layer: service (imports types, config, repo)
"""
from __future__ import annotations

import json
import logging
import re

import litellm

from newsprism.repo import (
    get_calibration_state,
    get_calibration_weights,
    get_feedback_training_rows,
    get_latest_editorial_policy,
    insert_editorial_policy,
    reset_calibration_weights,
    seed_calibration_weights,
    update_calibration_weight,
)
from newsprism.service.llm_compat import completion_compat_kwargs

logger = logging.getLogger(__name__)

# The six LLM-scored dimensions — signal is computed, not calibrated.
_LLM_DIMS = (
    "scope",
    "severity",
    "novelty",
    "actor_influence",
    "decision_relevance",
    "feelgood",
)


# ─── THIN WRAPPERS (monkeypatchable in tests) ───────────────────────────────────

def _get_feedback_rows(days: int) -> list[dict]:
    return get_feedback_training_rows(days=days)


def _get_weights() -> dict[str, float]:
    return get_calibration_weights()


def _get_seeds() -> dict[str, float]:
    """Return the seed weight for every calibrated dimension."""
    state = get_calibration_state()
    return {row["dimension"]: float(row["seed"]) for row in state}


def _update_weight(dimension: str, new_weight: float, reason: str = "") -> None:
    update_calibration_weight(dimension, new_weight, reason=reason)


def _insert_policy(text: str) -> None:
    insert_editorial_policy(text)


def _reset_weights() -> int:
    return reset_calibration_weights()


def _get_calibration_state() -> list[dict]:
    return get_calibration_state()


def _get_latest_policy() -> str | None:
    return get_latest_editorial_policy()


# ─── QUANTITATIVE NUDGE ─────────────────────────────────────────────────────────

def _compute_nudge(
    rows: list[dict],
    dim: str,
    max_step: float,
) -> float | None:
    """Return the weight nudge for `dim`, or None if there is no signal."""
    accepted = [r["dims"].get(dim, 0.0) for r in rows if r.get("verdict", 0) == 1]
    rejected = [r["dims"].get(dim, 0.0) for r in rows if r.get("verdict", 0) == -1]

    if not accepted or not rejected:
        return None

    mean_accept = sum(accepted) / len(accepted)
    mean_reject = sum(rejected) / len(rejected)
    diff = mean_accept - mean_reject

    if diff == 0.0:
        return None

    # Normalize to [-1, 1] (dims are 0-10 scale)
    normalized = max(-1.0, min(1.0, diff / 10.0))
    sign = 1 if normalized > 0 else -1
    return sign * min(abs(normalized), 1.0) * max_step


# ─── QUALITATIVE MEMORY ──────────────────────────────────────────────────────────

def _build_feedback_summary(rows: list[dict]) -> str:
    """Compact Chinese summary of this week's feedback for the LLM."""
    lines = []
    for row in rows:
        verdict_label = "接受" if row.get("verdict", 0) == 1 else "拒绝"
        summary = (row.get("cluster_summary") or "")[:80]
        rationale = (row.get("rationale") or "").strip()
        note = (row.get("note") or "").strip()
        line = f"[{verdict_label}] {summary}"
        if rationale:
            line += f" — 理由: {rationale}"
        if note:
            line += f" (备注: {note})"
        lines.append(line)
    return "\n".join(lines)


def _call_policy_llm(
    cfg,
    feedback_summary: str,
    policy_max_bullets: int,
) -> list[str]:
    """Ask the LLM to distill editor preferences into policy bullets. Returns [] on failure."""
    model = cfg.litellm_model
    api_key = cfg.litellm_api_key
    base_url = cfg.litellm_base_url
    compat = completion_compat_kwargs(model, base_url)

    system = (
        "你是一家国际新闻编辑部的编辑政策助手。"
        "根据编辑本周的接受/拒绝反馈，提炼出编辑的隐性偏好，"
        "用简短的中文编辑政策条目表达出来。只输出 JSON。"
    )
    user = (
        f"以下是本周的编辑反馈（共 {len(feedback_summary.splitlines())} 条）：\n\n"
        f"{feedback_summary}\n\n"
        f"请提炼成最多 {policy_max_bullets} 条简短的编辑政策（每条不超过 20 个中文字符），"
        "反映编辑的真实偏好，例如：\n"
        "- 降低例行市场波动权重\n"
        "- 提升半导体供应链事件优先级\n\n"
        '只输出 JSON：{"bullets": ["...", ...]}'
    )

    response = litellm.completion(
        model=model,
        api_key=api_key,
        api_base=base_url,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.3,
        max_tokens=600,
        response_format={"type": "json_object"},
        **compat,
    )
    content = (response.choices[0].message.content or "").strip()
    return _parse_bullets(content)


def _parse_bullets(content: str) -> list[str]:
    """Parse JSON bullets from LLM response; strip markdown fences robustly."""
    # Strip fences
    text = re.sub(r"^```[a-z]*\s*", "", content.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```$", "", text.strip(), flags=re.MULTILINE)
    text = text.strip()

    # Locate JSON object
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        raise ValueError(f"No JSON object found in: {content[:200]}")
    obj = json.loads(text[start:end + 1])
    bullets = obj.get("bullets", [])
    if not isinstance(bullets, list):
        raise ValueError(f"'bullets' is not a list: {type(bullets)}")
    return [str(b).strip() for b in bullets if str(b).strip()]


# ─── PUBLIC API ──────────────────────────────────────────────────────────────────

def run_calibration(cfg) -> dict:
    """Weekly calibration job.

    Returns:
        {"status": "skipped", "reason": "insufficient_feedback", "count": n}
      or
        {"status": "ok", "feedback_count": n, "weights_changed": [...], "policy_bullets": k}
    """
    cal_cfg = (cfg.evolution or {}).get("calibration", {})
    window_days: int = int(cal_cfg.get("training_window_days", 30))
    max_step: float = float(cal_cfg.get("max_step", 0.02))
    bounds: list[float] = list(cal_cfg.get("weight_bounds", [0.3, 3.0]))
    min_feedback: int = int(cal_cfg.get("min_feedback_to_run", 10))
    policy_max_bullets: int = int(cal_cfg.get("policy_max_bullets", 10))

    rows = _get_feedback_rows(window_days)
    if len(rows) < min_feedback:
        return {
            "status": "skipped",
            "reason": "insufficient_feedback",
            "count": len(rows),
        }

    # Load current weights and seeds
    current_weights = _get_weights()
    seeds = _get_seeds()

    weights_changed: list[str] = []

    for dim in _LLM_DIMS:
        nudge = _compute_nudge(rows, dim, max_step)
        if nudge is None:
            continue

        seed = seeds.get(dim, current_weights.get(dim, 0.0))
        if seed == 0.0:
            # Cannot bound a zero-seed dimension meaningfully; skip.
            continue

        current = current_weights.get(dim, seed)
        lo = seed * bounds[0]
        hi = seed * bounds[1]
        new_weight = max(lo, min(hi, current + nudge))

        if abs(new_weight - current) < 1e-9:
            continue

        diff = (
            (sum(r["dims"].get(dim, 0.0) for r in rows if r.get("verdict") == 1) /
             max(1, sum(1 for r in rows if r.get("verdict") == 1)))
            - (sum(r["dims"].get(dim, 0.0) for r in rows if r.get("verdict") == -1) /
               max(1, sum(1 for r in rows if r.get("verdict") == -1)))
        )
        reason = f"weekly: accept-reject diff={diff:+.2f}"
        _update_weight(dim, new_weight, reason=reason)
        weights_changed.append(dim)

    # Qualitative memory (best-effort)
    policy_bullets = 0
    try:
        feedback_summary = _build_feedback_summary(rows)
        bullets = _call_policy_llm(cfg, feedback_summary, policy_max_bullets)
        if bullets:
            policy_text = "\n".join(f"• {b}" for b in bullets)
            _insert_policy(policy_text)
            policy_bullets = len(bullets)
    except Exception as exc:
        logger.warning("Editorial policy memo failed (quantitative calibration still applied): %s", exc)

    return {
        "status": "ok",
        "feedback_count": len(rows),
        "weights_changed": weights_changed,
        "policy_bullets": policy_bullets,
    }


def show_calibration() -> str:
    """Format calibration state + latest editorial policy into a readable string."""
    state = _get_calibration_state()
    lines = ["Calibration weights (dimension | weight | seed | updated_at):"]
    for row in state:
        dim = row["dimension"]
        weight = float(row["weight"])
        seed = float(row["seed"])
        updated = row.get("updated_at", "")
        ratio = f"{weight / seed:.2f}x" if seed != 0.0 else "n/a"
        lines.append(f"  {dim:<22} {weight:.4f}  (seed {seed:.4f}, {ratio})  [{updated}]")

    policy = _get_latest_policy()
    if policy:
        lines.append("\nLatest editorial policy:")
        for bullet in policy.splitlines():
            lines.append(f"  {bullet}")
    else:
        lines.append("\nNo editorial policy stored yet.")

    return "\n".join(lines)


def reset_calibration() -> str:
    """Restore all calibration weights to their seed values."""
    count = _reset_weights()
    return f"Reset {count} dimension weight(s) to seed values."
