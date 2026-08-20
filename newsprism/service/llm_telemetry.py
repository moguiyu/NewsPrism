"""Best-effort per-stage LLM token telemetry.

Thin wrapper around ``litellm.completion``. Production enables it through
``llm_telemetry.enabled`` in config.yaml; tests and local runs leave it off.

The wrapper never changes call semantics. When telemetry is enabled it records
one ``llm_call_events`` row per API call and returns a
``TrackedCompletion`` object that delegates every attribute to the real
litellm response. Callers that later fail JSON parsing can call
``tracked.mark("malformed_json")`` to update the row status without changing
their response handling.

Layer: service (may import repo; never imports runtime).
"""
from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import litellm

from newsprism.repo import DB_PATH, insert_llm_call_event, update_llm_call_event_status
from newsprism.types import LLMCallEvent

logger = logging.getLogger(__name__)

_run_report_date: ContextVar[str | None] = ContextVar(
    "llm_run_report_date", default=None
)


@contextmanager
def llm_run_context(*, report_date: str | None):
    """Provide a report date for telemetry calls in the current execution context."""
    token = _run_report_date.set(report_date)
    try:
        yield
    finally:
        _run_report_date.reset(token)


def _effective_report_date(report_date: str | None) -> str | None:
    return report_date if report_date is not None else _run_report_date.get()


def _message_chars(messages: list[dict[str, str]]) -> int:
    total = 0
    for message in messages or []:
        content = message.get("content")
        if isinstance(content, str):
            total += len(content)
    return total


def _response_chars(response: Any) -> int:
    try:
        return len(response.choices[0].message.content or "")
    except Exception:
        return 0


def _lookup(value: Any, key: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _token_count(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _first_token_count(*values: Any) -> int | None:
    for value in values:
        parsed = _token_count(value)
        if parsed is not None:
            return parsed
    return None


def _usage_fields(
    response: Any,
) -> tuple[int | None, int | None, int | None, int | None, int | None]:
    usage = getattr(response, "usage", None)
    if usage is None:
        return None, None, None, None, None
    prompt_tokens_details = _lookup(usage, "prompt_tokens_details")
    prompt_cache_hit_tokens = _first_token_count(
        _lookup(usage, "prompt_cache_hit_tokens"),
        _lookup(usage, "cache_read_input_tokens"),
        _lookup(prompt_tokens_details, "cached_tokens"),
    )
    prompt_cache_miss_tokens = _first_token_count(
        _lookup(usage, "prompt_cache_miss_tokens"),
        _lookup(usage, "cache_creation_input_tokens"),
        _lookup(prompt_tokens_details, "cache_creation_tokens"),
    )
    return (
        _token_count(_lookup(usage, "prompt_tokens")),
        _token_count(_lookup(usage, "completion_tokens")),
        _token_count(_lookup(usage, "total_tokens")),
        prompt_cache_hit_tokens,
        prompt_cache_miss_tokens,
    )


def _finish_reason(response: Any) -> str | None:
    try:
        return response.choices[0].finish_reason
    except Exception:
        return None


class TrackedCompletion:
    """Proxy that behaves like a litellm response and can mark parse status."""

    def __init__(
        self,
        response: Any,
        event_id: int | None,
        db_path: Path,
    ) -> None:
        self._response = response
        self._event_id = event_id
        self._db_path = db_path
        self.choices = getattr(response, "choices", None)
        self.usage = getattr(response, "usage", None)
        self.model = getattr(response, "model", None)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._response, name)

    def mark(self, status: str) -> None:
        """Update the telemetry row after the caller determines parse status."""
        if self._event_id is None:
            return
        try:
            update_llm_call_event_status(self._event_id, status, db_path=self._db_path)
        except Exception as exc:
            logger.debug("LLM telemetry status update failed for event %s: %s", self._event_id, exc)


def tracked_completion(
    *,
    stage: str,
    enabled: bool,
    model: str,
    messages: list[dict[str, str]],
    report_date: str | None = None,
    cluster_key: str | None = None,
    item_count: int | None = None,
    attempt: int = 1,
    db_path: Path = DB_PATH,
    **kwargs: Any,
) -> Any:
    """Call litellm.completion and, when enabled, persist usage telemetry.

    On telemetry-enabled paths the returned object is a ``TrackedCompletion``
    proxy; callers can use it exactly like the raw response. On disabled paths
    the raw litellm response is returned unchanged.
    """
    if not enabled:
        return litellm.completion(
            model=model,
            messages=messages,
            **kwargs,
        )

    effective_report_date = _effective_report_date(report_date)
    started = time.perf_counter()
    try:
        response = litellm.completion(
            model=model,
            messages=messages,
            **kwargs,
        )
    except Exception as exc:
        try:
            prompt_tokens = None
            completion_tokens = None
            total_tokens = None
            insert_llm_call_event(
                LLMCallEvent(
                    stage=stage,
                    model=str(model),
                    report_date=effective_report_date,
                    cluster_key=cluster_key,
                    item_count=item_count,
                    attempt=attempt,
                    status="api_error",
                    finish_reason=None,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    input_chars=_message_chars(messages),
                    output_chars=0,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                ),
                db_path=db_path,
            )
        except Exception:
            logger.debug("LLM telemetry error-row write failed for %s", stage)
        raise exc

    (
        prompt_tokens,
        completion_tokens,
        total_tokens,
        prompt_cache_hit_tokens,
        prompt_cache_miss_tokens,
    ) = _usage_fields(response)
    event_id: int | None = None
    try:
        event_id = insert_llm_call_event(
            LLMCallEvent(
                stage=stage,
                model=str(model),
                report_date=effective_report_date,
                cluster_key=cluster_key,
                item_count=item_count,
                attempt=attempt,
                status="ok",
                finish_reason=_finish_reason(response),
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                prompt_cache_hit_tokens=prompt_cache_hit_tokens,
                prompt_cache_miss_tokens=prompt_cache_miss_tokens,
                input_chars=_message_chars(messages),
                output_chars=_response_chars(response),
                duration_ms=int((time.perf_counter() - started) * 1000),
            ),
            db_path=db_path,
        )
    except Exception:
        logger.debug("LLM telemetry write failed for %s", stage)

    return TrackedCompletion(response, event_id, db_path)


def record_llm_parse_failure(
    *,
    stage: str,
    enabled: bool,
    model: str,
    item_count: int | None = None,
    attempt: int = 1,
    report_date: str | None = None,
    cluster_key: str | None = None,
    db_path: Path = DB_PATH,
) -> None:
    """Record a separate zero-token row when a valid API response is unusable.

    Used only when a caller cannot retain the ``TrackedCompletion`` handle
    (e.g. a nested helper already returned the raw string). Callers with the
    handle should prefer ``tracked.mark("malformed_json")``.
    """
    if not enabled:
        return
    try:
        insert_llm_call_event(
            LLMCallEvent(
                stage=stage,
                model=str(model),
                report_date=_effective_report_date(report_date),
                cluster_key=cluster_key,
                item_count=item_count,
                attempt=attempt,
                status="malformed_json",
            ),
            db_path=db_path,
        )
    except Exception as exc:
        logger.debug("LLM parse-failure telemetry write failed for %s: %s", stage, exc)
