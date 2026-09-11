import json
from datetime import datetime, timezone

import pytest

from newsprism.types import Article, ArticleCluster


def test_cost_uses_three_token_prices_and_beijing_peak_window():
    from scripts.clustering_cost_benchmark import estimate_cost

    event = dict(model="openai/deepseek-v4-flash", prompt_tokens=1_000_000,
                 prompt_cache_hit_tokens=200_000, completion_tokens=100_000,
                 created_at="2026-09-04 05:20:00")  # Friday 13:20 Beijing
    assert estimate_cost(event) == pytest.approx(1.66)
    event["created_at"] = "2026-09-04 06:00:00"  # Friday 14:00 Beijing
    assert estimate_cost(event) == pytest.approx(3.32)
    event["created_at"] = "2026-09-05 06:00:00"  # Saturday, off peak
    assert estimate_cost(event) == pytest.approx(1.66)


def test_missing_usage_and_unknown_models_are_not_reported_as_zero_cost():
    from scripts.clustering_cost_benchmark import estimate_cost

    event = dict(model="openai/deepseek-v4-flash", prompt_tokens=None,
                 completion_tokens=100, created_at="2026-09-05 05:20:00")
    assert estimate_cost(event) is None
    event.update(prompt_tokens=1000, prompt_cache_hit_tokens=None)
    assert estimate_cost(event) == pytest.approx(.00195)  # conservative uncached bound
    event.update(model="unknown-model")
    assert estimate_cost(event) is None


def test_live_usage_extraction_records_billed_cost_for_dated_models():
    """The 6-field usage contract (token classes + billed cost) must survive
    dated slugs such as -0731: a stale 5-field unpack silently degraded the
    live benchmark arms to embedding fallback on 2026-09-11."""
    from scripts.clustering_cost_benchmark import usage_event_fields

    class FakeUsage:
        prompt_tokens = 1200
        completion_tokens = 300
        total_tokens = 1500
        prompt_cache_hit_tokens = 0
        prompt_cache_miss_tokens = 0
        cost = 0.00042

    class FakeResponse:
        usage = FakeUsage()

    fields = usage_event_fields(FakeResponse())
    assert fields["prompt_tokens"] == 1200
    assert fields["completion_tokens"] == 300
    assert fields["total_tokens"] == 1500
    assert fields["billed_cost_usd"] == 0.00042


def test_pair_comparison_exposes_merges_splits_and_uncovered_articles():
    from scripts.clustering_cost_benchmark import compare_clusters

    articles = [Article(url=f"https://example.com/{i}", title=f"Event {i}",
                        source_name=f"Source {i}", published_at=datetime.now(timezone.utc),
                        content="Evidence") for i in range(4)]
    baseline = [ArticleCluster(topic_category="base", articles=articles[:3])]
    candidate = [ArticleCluster(topic_category="new label", articles=articles[:2]),
                 ArticleCluster(topic_category="other", articles=articles[2:])]
    result = compare_clusters(baseline, candidate)
    assert result["shared_pairs"] == 1
    assert len(result["baseline_only_pairs"]) == 2
    assert result["candidate_only_pairs"] == [[articles[2].url, articles[3].url]]
    assert result["baseline_article_count"] == 3
    assert result["candidate_article_count"] == 4


def test_benchmark_defaults_to_offline_and_does_not_call_provider(tmp_path, monkeypatch, capsys):
    import litellm
    from scripts.clustering_cost_benchmark import main

    def forbidden(**kwargs):
        pytest.fail("Offline benchmark attempted a paid API call")

    monkeypatch.setattr(litellm, "completion", forbidden)
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps({"events": [], "clusters": [], "articles": [
        dict(id=i, url=f"https://example.com/{i}", title=f"Event {i}",
             source_name=f"Source {i}", published_at="2026-09-05T01:00:00+00:00",
             created_at="2026-09-05 02:00:00", content="evidence " * 80,
             embedding=None, is_searched=False, is_placeholder=False)
        for i in range(2)
    ]}))
    assert main([str(snapshot), "--date", "2026-09-05", "--max-batches", "1"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "offline"
    assert result["api_calls"] == 0
    assert result["batches"][0]["article_count"] == 2
    assert result["batches"][0]["compact_prompt_chars"] < result["batches"][0]["legacy_prompt_chars"]


def test_live_benchmark_records_failed_attempts_with_unknown_cost(monkeypatch):
    import litellm
    from newsprism.config import load_config
    from scripts.clustering_cost_benchmark import _live_cluster

    articles = [Article(url=f"https://example.com/{i}", title=f"Event {i}",
                        source_name=f"Source {i}", published_at=datetime.now(timezone.utc),
                        content="Evidence", embedding=[float(i == j) for j in range(3)])
                for i in range(3)]

    def timeout(**kwargs):
        raise TimeoutError("Provider request timed out")

    monkeypatch.setattr(litellm, "completion", timeout)
    clusters, calls = _live_cluster(load_config(), articles, compact=True)
    assert len(clusters) == 3
    assert len(calls) == 1
    assert calls[0]["status"] == "api_error"
    assert calls[0]["estimated_cny"] is None
    assert calls[0]["prompt_tokens"] is None


def test_cold_cache_benchmark_adds_unique_prefix_to_each_request(monkeypatch):
    import litellm
    from newsprism.config import load_config
    from newsprism.service.llm_clusterer import _SYSTEM_PROMPT
    from scripts.clustering_cost_benchmark import _live_cluster

    messages = []

    def completion(**kwargs):
        messages.append(kwargs["messages"])
        return litellm.ModelResponse(
            choices=[{"message": {"role": "assistant", "content": '{"groups":[]}'},
                      "finish_reason": "stop"}],
            usage={"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110},
        )

    monkeypatch.setattr(litellm, "completion", completion)
    articles = [Article(url=f"https://example.com/{i}", title=f"Event {i}",
                        source_name=f"Source {i}", published_at=datetime.now(timezone.utc),
                        content="Evidence", embedding=[float(i == j) for j in range(3)])
                for i in range(3)]
    for _ in range(2):
        clusters, calls = _live_cluster(load_config(), articles, compact=True, cold_cache=True)
        assert len(clusters) == 3
        assert len(calls) == 1
    assert messages[0][0]["content"] != messages[1][0]["content"]
    assert all(m[0]["content"].endswith(_SYSTEM_PROMPT) for m in messages)
    assert messages[0][1] == messages[1][1]
