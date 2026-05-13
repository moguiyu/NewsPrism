"""Provider-specific LiteLLM request compatibility helpers."""
from __future__ import annotations

from urllib.parse import urlparse


DEEPSEEK_API_HOST = "api.deepseek.com"


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

    return {}
