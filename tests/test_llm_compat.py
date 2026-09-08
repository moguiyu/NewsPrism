from newsprism.service.llm_compat import completion_compat_kwargs


def test_disables_thinking_for_direct_deepseek_v4_flash() -> None:
    assert completion_compat_kwargs("openai/deepseek-v4-flash", "https://api.deepseek.com/v1") == {
        "extra_body": {"thinking": {"type": "disabled"}}
    }


def test_disables_thinking_for_direct_deepseek_v4_pro_with_normalized_host() -> None:
    assert completion_compat_kwargs("deepseek/deepseek-v4-pro", "https://API.DEEPSEEK.COM./v1") == {
        "extra_body": {"thinking": {"type": "disabled"}}
    }


def test_leaves_legacy_deepseek_alias_unchanged() -> None:
    assert completion_compat_kwargs("openai/deepseek-chat", "https://api.deepseek.com/v1") == {}


def test_leaves_non_deepseek_provider_unchanged() -> None:
    assert completion_compat_kwargs("openai/gpt-4.1-mini", "https://api.openai.com/v1") == {}


def test_rejects_deepseek_host_substring_outside_hostname() -> None:
    for base_url in [
        "https://evil.example/api.deepseek.com/v1",
        "https://api.deepseek.com.evil.example/v1",
        "https://api.deepseek.com@evil.example/v1",
        "",
        "not a url",
    ]:
        assert completion_compat_kwargs("openai/deepseek-v4-flash", base_url) == {}


def test_disables_reasoning_for_openrouter_hosted_deepseek_v4_flash() -> None:
    assert completion_compat_kwargs("openai/deepseek/deepseek-v4-flash", "https://openrouter.ai/api/v1") == {
        "extra_body": {"reasoning": {"enabled": False}, "usage": {"include": True}}
    }


def test_disables_reasoning_for_openrouter_hosted_deepseek_v4_pro() -> None:
    assert completion_compat_kwargs("deepseek/deepseek-v4-pro", "https://openrouter.ai/api/v1") == {
        "extra_body": {"reasoning": {"enabled": False}, "usage": {"include": True}}
    }


def test_openrouter_requests_include_usage_accounting() -> None:
    """Every OpenRouter call opts into usage accounting so billed cost lands in telemetry."""
    assert completion_compat_kwargs(
        "openai/dots-studio/dots-3-note-preview:free", "https://openrouter.ai/api/v1"
    ) == {"extra_body": {"usage": {"include": True}}}


def test_deepseek_direct_excludes_usage_accounting() -> None:
    kwargs = completion_compat_kwargs("openai/deepseek-v4-flash", "https://api.deepseek.com/v1")
    assert kwargs == {"extra_body": {"thinking": {"type": "disabled"}}}
    assert "usage" not in kwargs["extra_body"]


def test_rejects_openrouter_host_substring_outside_hostname() -> None:
    for base_url in [
        "https://evil.example/openrouter.ai/v1",
        "https://openrouter.ai.evil.example/v1",
    ]:
        assert (
            completion_compat_kwargs("openai/deepseek/deepseek-v4-flash", base_url)
            == {}
        )
