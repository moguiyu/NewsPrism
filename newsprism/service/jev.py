"""TypeSafe Jev decision client (OpenRouter Decisions API).

Jev returns typed decisions with probabilities instead of prose. It is used
only for bounded editorial judgments (duplicate adjudication, hot-family
coherence, importance scoring, candidate relevance); summaries, labels and
translations stay on the generative model.

The client is deliberately fail-open: any transport, schema or budget problem
returns ``None`` and callers keep their existing deterministic/LLM behavior.
Layer: service (may import repo; never imports runtime).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping

import httpx

from newsprism.repo import DB_PATH, insert_jev_call_event
from newsprism.types import JevCallEvent

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://openrouter.ai/api/alpha/decisions"
DEFAULT_MODEL = "typesafe/jev-1.14"

_IMPORTANCE_CRITERIA = [
    "Routine item: celebrity, product, sport result, or daily market noise with no lasting consequence.",
    "Minor local item: limited to a small group, easily reversible, little public consequence.",
    "Notable regional item: affects one city/region or a specialised community.",
    "Significant national item: affects a country's policy, economy, security, or a large population.",
    "Major international item: affects several countries, a alliance, or a global system.",
    "Historic global item: irreversible shift for much of humanity (war, pandemic, constitutional crisis, major breakthrough).",
]

_COHERENCE_CRITERIA = [
    "Unrelated: the items are different events that happen to share a broad topic.",
    "Loose theme: same subject area but different events, actors, or places.",
    "Related but distinct: one broader story with clearly separate incidents or angles.",
    "Same event: all items cover one real-world event or one directly connected development.",
    "Same event and same stage: all items describe the same moment/decision with no new development.",
]


@dataclass
class JevResult:
    """One successful decisions response."""

    answers: dict[str, Any]
    model: str
    cost_usd: float
    duration_ms: int
    input_tokens: int
    output_tokens: int
    shadow: bool = True

    def probability(self, name: str) -> float | None:
        answer = self.answers.get(name) or {}
        if answer.get("type") == "noul":
            try:
                return float(answer.get("noul"))
            except (TypeError, ValueError):
                return None
        return None

    def score_unit(self, name: str) -> float | None:
        answer = self.answers.get(name) or {}
        try:
            score = float(answer.get("score"))
        except (TypeError, ValueError):
            return None
        legend = answer.get("legend") or {}
        top = max(1, len(legend) - 1)
        return max(0.0, min(1.0, score / top))

    def confidence(self, name: str) -> float | None:
        answer = self.answers.get(name) or {}
        try:
            return float(answer.get("confidence"))
        except (TypeError, ValueError):
            return None


@dataclass
class _StageConfig:
    enabled: bool = True
    options: dict[str, Any] = field(default_factory=dict)


class JevClient:
    """Small synchronous client for the OpenRouter-hosted Jev Decisions API."""

    def __init__(
        self,
        config: Mapping[str, Any] | None = None,
        *,
        api_key: str | None = None,
        db_path=None,
        transport: httpx.BaseTransport | None = None,
        sleep=time.sleep,
    ) -> None:
        cfg = dict(config or {})
        self.enabled = bool(cfg.get("enabled", False))
        self.shadow = bool(cfg.get("shadow", True))
        self.model = str(cfg.get("model") or DEFAULT_MODEL)
        self.base_url = str(cfg.get("base_url") or DEFAULT_BASE_URL)
        self.timeout_seconds = float(cfg.get("timeout_seconds", 6.0))
        self.max_retries = max(0, int(cfg.get("max_retries", 1)))
        self.max_requests_per_run = max(1, int(cfg.get("max_requests_per_run", 200)))
        self.max_input_tokens_per_run = max(1, int(cfg.get("max_input_tokens_per_run", 800_000)))
        key_env = str(cfg.get("api_key_env") or "OPENROUTER_API_KEY")
        self.api_key = (
            api_key
            or os.environ.get(key_env)
            or os.environ.get("OPENROUTER_API_KEY")
            or os.environ.get("LITELLM_API_KEY")
            or ""
        )
        self.db_path = db_path or DB_PATH
        self._transport = transport
        self._sleep = sleep
        self._stages: dict[str, _StageConfig] = {}
        raw_stages = cfg.get("stages") or {}
        if isinstance(raw_stages, Mapping):
            for name, value in raw_stages.items():
                if isinstance(value, Mapping):
                    self._stages[str(name)] = _StageConfig(
                        enabled=bool(value.get("enabled", True)),
                        options={k: v for k, v in value.items() if k != "enabled"},
                    )
                else:
                    self._stages[str(name)] = _StageConfig(enabled=bool(value))
        self._requests = 0
        self._input_tokens = 0
        self._run_date: str | None = None
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0
        self._logged_budget = False

    # ─── lifecycle / configuration ───────────────────────────────────────────

    @property
    def available(self) -> bool:
        return bool(self.enabled and self.api_key and self.model)

    def stage_enabled(self, stage: str) -> bool:
        if not self.available:
            return False
        return self._stages.get(stage, _StageConfig()).enabled

    def stage_options(self, stage: str) -> dict[str, Any]:
        return dict(self._stages.get(stage, _StageConfig()).options)

    def begin_run(self, report_date: str | date | None) -> None:
        key = str(report_date or "")
        if key != self._run_date:
            self._run_date = key
            self._requests = 0
            self._input_tokens = 0
            self._logged_budget = False

    def close(self) -> None:
        # httpx clients are created per call, so nothing persistent to close.
        return None

    # ─── low-level request ───────────────────────────────────────────────────

    def _budget_available(self) -> bool:
        if self._requests >= self.max_requests_per_run:
            if not self._logged_budget:
                logger.warning(
                    "Jev request budget exhausted (%d requests); continuing with existing rules",
                    self.max_requests_per_run,
                )
                self._logged_budget = True
            return False
        if self._input_tokens >= self.max_input_tokens_per_run:
            if not self._logged_budget:
                logger.warning(
                    "Jev input-token budget exhausted (%d tokens); continuing with existing rules",
                    self.max_input_tokens_per_run,
                )
                self._logged_budget = True
            return False
        return True

    def decide(
        self,
        *,
        stage: str,
        state: Any,
        questions: dict[str, Any],
        report_date: str | date | None = None,
        cluster_key: str | None = None,
        item_count: int | None = None,
    ) -> JevResult | None:
        if not self.stage_enabled(stage):
            return None
        self.begin_run(report_date)
        now = time.monotonic()
        if now < self._circuit_open_until:
            return None
        if not self._budget_available():
            return None

        payload = {"model": self.model, "state": state, "questions": questions}
        request_hash = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:32]
        self._requests += 1
        attempts = self.max_retries + 1
        last_error = ""
        for attempt in range(1, attempts + 1):
            started = time.monotonic()
            try:
                with httpx.Client(
                    timeout=self.timeout_seconds,
                    transport=self._transport,
                    follow_redirects=True,
                ) as client:
                    response = client.post(
                        self.base_url,
                        headers={
                            "Authorization": f"Bearer {self.api_key}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                    )
                    if response.status_code in {429, 500, 502, 503, 524, 529} and attempt < attempts:
                        last_error = f"http_{response.status_code}"
                        self._sleep(0.4 * attempt)
                        continue
                    response.raise_for_status()
                    data = response.json()
                answers = data.get("answers")
                if not isinstance(answers, Mapping):
                    raise ValueError("missing answers object")
                usage = data.get("usage") or {}
                try:
                    input_tokens = int(usage.get("input_tokens") or 0)
                    output_tokens = int(usage.get("output_tokens") or 0)
                    cost = float(usage.get("cost") or 0.0)
                except (TypeError, ValueError):
                    input_tokens = output_tokens = 0
                    cost = 0.0
                duration_ms = int((time.monotonic() - started) * 1000)
                result = JevResult(
                    answers=dict(answers),
                    model=str(data.get("model") or self.model),
                    cost_usd=cost,
                    duration_ms=duration_ms,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    shadow=self.shadow,
                )
                self._input_tokens += input_tokens
                self._consecutive_failures = 0
                self._record(
                    stage=stage,
                    request_hash=request_hash,
                    report_date=report_date,
                    cluster_key=cluster_key,
                    item_count=item_count,
                    status="shadow" if self.shadow else "ok",
                    result=result,
                )
                if self.shadow:
                    logger.info(
                        "Jev shadow [%s] cost=$%.6f latency=%dms answers=%s",
                        stage,
                        result.cost_usd,
                        result.duration_ms,
                        list(result.answers),
                    )
                return result
            except Exception as exc:  # noqa: BLE001 - fail open by design
                last_error = f"{type(exc).__name__}: {exc}"[:300]
                if attempt < attempts:
                    self._sleep(0.4 * attempt)
                    continue

        self._consecutive_failures += 1
        if self._consecutive_failures >= 3:
            self._circuit_open_until = time.monotonic() + 600
            logger.warning("Jev circuit opened after %d consecutive failures", self._consecutive_failures)
        logger.warning("Jev call failed [%s]: %s", stage, last_error)
        self._record(
            stage=stage,
            request_hash=request_hash,
            report_date=report_date,
            cluster_key=cluster_key,
            item_count=item_count,
            status="error",
            result=None,
            error=last_error,
        )
        return None

    def _record(
        self,
        *,
        stage: str,
        request_hash: str,
        report_date: str | date | None,
        cluster_key: str | None,
        item_count: int | None,
        status: str,
        result: JevResult | None,
        error: str | None = None,
    ) -> None:
        try:
            insert_jev_call_event(
                JevCallEvent(
                    stage=stage,
                    model=result.model if result else self.model,
                    request_hash=request_hash,
                    report_date=str(report_date) if report_date else None,
                    cluster_key=cluster_key,
                    item_count=item_count,
                    shadow=self.shadow,
                    status=status,
                    prompt_tokens=result.input_tokens if result else None,
                    completion_tokens=result.output_tokens if result else None,
                    duration_ms=result.duration_ms if result else None,
                    billed_cost_usd=result.cost_usd if result else None,
                    error=error,
                ),
                db_path=self.db_path,
            )
        except Exception as exc:  # noqa: BLE001 - telemetry must never break publishing
            logger.debug("Jev telemetry write failed: %s", exc)

    # ─── editorial judgments ─────────────────────────────────────────────────

    def adjudicate_duplicate(
        self,
        *,
        left: Mapping[str, Any],
        right: Mapping[str, Any],
        similarity: float,
        report_date: str | date | None = None,
        cluster_key: str | None = None,
    ) -> float | None:
        """Probability that two display summaries are the same event."""
        options = self.stage_options("display_dedup")
        state = {
            "candidate_similarity": round(float(similarity), 3),
            "left": _compact_item(left),
            "right": _compact_item(right),
        }
        questions = {
            "same_event": {
                "type": "noul",
                "instructions": (
                    "Do the two candidate news items describe the same real-world event, "
                    "such that publishing both as separate stories would duplicate coverage? "
                    "Different angles, reactions, or market moves caused by the same event count as the same event. "
                    "A shared topic, country, or actor alone does not."
                ),
                "criteria": {
                    "true": "Both items report the same event or one directly connected development.",
                    "false": "The items are different events, even if they share a topic, actor, or country.",
                },
            }
        }
        result = self.decide(
            stage="display_dedup",
            state=state,
            questions=questions,
            report_date=report_date,
            cluster_key=cluster_key,
            item_count=2,
        )
        return result.probability("same_event") if result else None

    def score_importance(
        self,
        *,
        title: str,
        summary: str,
        sources: list[str],
        regions: list[str],
        report_date: str | date | None = None,
        cluster_key: str | None = None,
    ) -> tuple[float, float] | None:
        """Return (normalised importance 0-1, confidence)."""
        state = {
            "title": title[:300],
            "summary": summary[:700],
            "sources": sources[:8],
            "regions": regions[:5],
        }
        questions = {
            "importance": {
                "type": "score",
                "instructions": (
                    "How important is this news event for a reader who wants to understand "
                    "the most consequential developments in the world today? Judge the event's "
                    "real consequences, not how many sources cover it. Routine market moves, "
                    "celebrity news, product launches and sports results are low."
                ),
                "criteria": _IMPORTANCE_CRITERIA,
            }
        }
        result = self.decide(
            stage="importance",
            state=state,
            questions=questions,
            report_date=report_date,
            cluster_key=cluster_key,
            item_count=1,
        )
        if not result:
            return None
        score = result.score_unit("importance")
        if score is None:
            return None
        return score, (result.confidence("importance") or 0.0)

    def score_hot_family(
        self,
        *,
        name: str,
        members: list[Mapping[str, Any]],
        report_date: str | date | None = None,
        cluster_key: str | None = None,
    ) -> tuple[float, float, float] | None:
        """Return (importance, coherence, label_support), each normalised 0-1."""
        state = {"label": name, "members": [_compact_item(m) for m in members[:8]]}
        questions = {
            "importance": {
                "type": "score",
                "instructions": (
                    "How important is this group of news stories for a reader who wants to "
                    "understand the most consequential developments today?"
                ),
                "criteria": _IMPORTANCE_CRITERIA,
            },
            "coherence": {
                "type": "score",
                "instructions": (
                    "Do these stories belong together as one hot-topic family? Judge whether "
                    "they cover one real-world event or one directly connected development, "
                    "not merely the same broad subject."
                ),
                "criteria": _COHERENCE_CRITERIA,
            },
            "label_support": {
                "type": "score",
                "instructions": (
                    "Does the label accurately describe every member story without overstating "
                    "or narrowing the event? A label that names one member's angle while the "
                    "others are different events scores low."
                ),
                "criteria": _COHERENCE_CRITERIA,
            },
        }
        result = self.decide(
            stage="hot_topics",
            state=state,
            questions=questions,
            report_date=report_date,
            cluster_key=cluster_key,
            item_count=len(members),
        )
        if not result:
            return None
        importance = result.score_unit("importance")
        coherence = result.score_unit("coherence")
        support = result.score_unit("label_support")
        if importance is None or coherence is None or support is None:
            return None
        return importance, coherence, support

    def score_candidate_event(
        self,
        *,
        article_title: str,
        article_content: str,
        cluster_titles: list[str],
        target_label: str,
        report_date: str | date | None = None,
        cluster_key: str | None = None,
    ) -> float | None:
        """Probability that a search candidate covers the cluster's current event."""
        state = {
            "target_voice": target_label,
            "cluster_titles": cluster_titles[:5],
            "candidate_title": article_title[:300],
            "candidate_content": article_content[:1200],
        }
        questions = {
            "same_event": {
                "type": "noul",
                "instructions": (
                    "Is the candidate article about the same current event as the cluster "
                    "headlines, rather than background, a different incident, or a generic "
                    "page about the same topic?"
                ),
                "criteria": {
                    "true": "The candidate reports this event or a directly connected development.",
                    "false": "The candidate is background, a different event, or a generic topic page.",
                },
            }
        }
        result = self.decide(
            stage="seeker",
            state=state,
            questions=questions,
            report_date=report_date,
            cluster_key=cluster_key,
            item_count=1,
        )
        return result.probability("same_event") if result else None


def _compact_item(item: Mapping[str, Any]) -> dict[str, Any]:
    """Bound state size: headline, short summary, sources, time."""
    return {
        "headline": str(item.get("headline") or item.get("title") or "")[:300],
        "summary": str(item.get("summary") or item.get("snippet") or "")[:700],
        "sources": [str(s)[:60] for s in (item.get("sources") or [])][:8],
        "published": str(item.get("published") or item.get("published_at") or "")[:40],
    }
