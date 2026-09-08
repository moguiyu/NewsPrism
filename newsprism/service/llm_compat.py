"""Provider-specific LiteLLM request compatibility helpers."""
from __future__ import annotations

from urllib.parse import urlparse


DEEPSEEK_API_HOST = "api.deepseek.com"
OPENROUTER_API_HOST = "openrouter.ai"


def _base_url_host(base_url: str) -> str:
    parsed = urlparse(base_url)
    return (parsed.hostname or "").lower().rstrip(".")


def completion_compat_kwargs(model: str, base_url: str) -> dict[str, object]:
    """Return extra LiteLLM kwargs needed for provider/model quirks."""
    normalized_model = model.lower().removeprefix("openai/").removeprefix("deepseek/")
    base_host = _base_url_host(base_url)

    if normalized_model in {"deepseek-v4-flash", "deepseek-v4-pro"} and base_host == DEEPSEEK_API_HOST:
        # DeepSeek V4 defaults to thinking mode, which can spend short JSON-call
        # token budgets on reasoning and leave final content empty. The previous
        # deepseek-chat alias used non-thinking mode, so preserve that behavior.
        return {"extra_body": {"thinking": {"type": "disabled"}}}

    if base_host == OPENROUTER_API_HOST:
        extra_body: dict[str, object] = {
            # OpenRouter usage accounting returns the actual billed cost per
            # request; llm_telemetry persists it and warns when the blended
            # rate exceeds BLENDED_RATE_ALERT_USD_PER_1M.
            "usage": {"include": True},
        }
        if normalized_model in {"deepseek-v4-flash", "deepseek-v4-pro"}:
            # OpenRouter-hosted DeepSeek V4 has the same reasoning-by-default
            # issue (verified: finish=length with empty content on short JSON
            # budgets). OpenRouter's unified `reasoning` switch is the
            # provider-agnostic way to disable it; free stage models tolerate
            # the extra key.
            extra_body["reasoning"] = {"enabled": False}
        return {"extra_body": extra_body}

    return {}
