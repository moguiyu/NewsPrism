from newsprism.service.llm_compat import completion_compat_kwargs


def test_disables_thinking_for_direct_deepseek_v4_flash() -> None:
    assert completion_compat_kwargs("openai/deepseek-v4-flash", "https://api.deepseek.com/v1") == {
        "extra_body": {"thinking": {"type": "disabled"}}
    }


def test_leaves_legacy_deepseek_alias_unchanged() -> None:
    assert completion_compat_kwargs("openai/deepseek-chat", "https://api.deepseek.com/v1") == {}


def test_leaves_non_deepseek_provider_unchanged() -> None:
    assert completion_compat_kwargs("openai/gpt-4.1-mini", "https://api.openai.com/v1") == {}
