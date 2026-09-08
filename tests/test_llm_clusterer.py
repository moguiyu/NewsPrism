from datetime import datetime, timezone

import json
import pytest

from newsprism.config import Config
from newsprism.service.llm_clusterer import LLMClusterer
from newsprism.types import Article, ArticleCluster


def _config() -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={"llm_min_clusters_fallback": 1, "llm_max_articles_per_call": 40},
        dedup={},
        summarizer={},
        output={},
        active_search={},
    )


def _article(index: int) -> Article:
    return Article(
        url=f"https://example.com/{index}",
        title=f"Event {index}",
        source_name=f"Source {index}",
        published_at=datetime.now(tz=timezone.utc),
        content=f"Event {index} coverage.",
        embedding=[1.0, 0.0],
    )


def test_llm_clusterer_defaults_to_sixty_article_calls():
    cfg = _config()
    cfg.clustering = {"llm_min_clusters_fallback": 1}

    assert LLMClusterer(cfg).max_articles_per_call == 60


def test_llm_clusterer_retries_only_the_failed_large_chunk(monkeypatch):
    clusterer = LLMClusterer(_config())
    articles = [_article(index) for index in range(40)]
    calls: list[int] = []

    def llm_cluster(chunk, **kwargs):
        calls.append(len(chunk))
        if len(chunk) == 40:
            raise ValueError("malformed JSON")
        return [ArticleCluster(topic_category=f"chunk-{chunk[0].title}", articles=[chunk[0]])]

    monkeypatch.setattr(clusterer, "_llm_cluster", llm_cluster)
    fallback_calls: list[int] = []
    monkeypatch.setattr(
        clusterer._fallback,
        "cluster",
        lambda chunk: fallback_calls.append(len(chunk)) or [],
    )

    clusters = clusterer.cluster(articles)

    assert calls == [40, 20, 20]
    assert fallback_calls == []
    assert len(clusters) == 2


def test_llm_clusterer_falls_back_only_for_unrecoverable_subchunk(monkeypatch):
    clusterer = LLMClusterer(_config())
    articles = [_article(index) for index in range(40)]

    def llm_cluster(chunk, **kwargs):
        if len(chunk) == 40 or chunk[0].title == "Event 0":
            raise ValueError("malformed JSON")
        return [ArticleCluster(topic_category="llm", articles=[chunk[0]])]

    monkeypatch.setattr(clusterer, "_llm_cluster", llm_cluster)
    fallback_calls: list[list[Article]] = []
    monkeypatch.setattr(
        clusterer._fallback,
        "cluster",
        lambda chunk: fallback_calls.append(chunk) or [ArticleCluster(topic_category="embedding", articles=[chunk[0]])],
    )

    clusters = clusterer.cluster(articles)

    assert [len(chunk) for chunk in fallback_calls] == [20]
    assert [cluster.topic_category for cluster in clusters] == ["embedding", "llm"]


def test_parse_cluster_entries_recovers_complete_objects_from_truncated_json():
    from newsprism.service.llm_clusterer import _parse_cluster_entries

    raw = '{"clusters": [{"label": "one", "ids": [0, 1]}, {"label": "two", "ids": [2, 3]}, {"label": "br'
    entries = _parse_cluster_entries(raw)
    assert [entry["label"] for entry in entries] == ["one", "two"]


def test_llm_cluster_prompt_omits_unclustered_contract(monkeypatch):
    from types import SimpleNamespace

    import litellm

    clusterer = LLMClusterer(_config())
    articles = [_article(index) for index in range(3)]
    captured: dict = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"clusters": []}'))]
        )

    monkeypatch.setattr(litellm, "completion", fake_completion)
    clusterer._llm_cluster(articles)

    prompt = captured["messages"][1]["content"]
    assert '"unclustered"' not in prompt
    assert 'Articles that do not fit any cluster are omitted entirely' in prompt


def test_llm_cluster_uses_configured_snippet_chars():
    cfg = _config()
    cfg.clustering = {
        "llm_min_clusters_fallback": 1,
        "llm_max_articles_per_call": 60,
        "article_snippet_chars": 20,
    }
    clusterer = LLMClusterer(cfg)
    articles = [_article(1)]
    articles[0].content = "x" * 100
    payload = clusterer._article_payload(articles)
    assert len(payload[0]["snippet"]) == 20


def test_llm_cluster_uses_configured_max_output_tokens(monkeypatch):
    from types import SimpleNamespace

    import litellm

    cfg = _config()
    cfg.clustering = {
        "llm_min_clusters_fallback": 1,
        "llm_max_articles_per_call": 60,
        "llm_max_output_tokens": 4000,
    }
    clusterer = LLMClusterer(cfg)
    captured: dict = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"clusters": []}'))]
        )

    monkeypatch.setattr(litellm, "completion", fake_completion)
    clusterer._llm_cluster([_article(1)])
    assert captured["max_tokens"] == 4000


def test_llm_cluster_followup_uses_configured_max_output_tokens(monkeypatch):
    from types import SimpleNamespace

    import litellm

    cfg = _config()
    cfg.clustering = {
        "llm_min_clusters_fallback": 1,
        "llm_max_articles_per_call": 60,
        "llm_max_output_tokens": 5000,
    }
    clusterer = LLMClusterer(cfg)
    captured: dict = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"clusters": []}'))]
        )

    monkeypatch.setattr(litellm, "completion", fake_completion)
    clusterer._llm_cluster_followup(
        [_article(1)],
        [ArticleCluster(topic_category="existing", articles=[_article(2)])],
        report_date=None,
    )
    assert captured["max_tokens"] == 5000


def test_build_clusters_assigns_each_article_only_once():
    from newsprism.service.llm_clusterer import _build_clusters

    articles = [_article(index) for index in range(4)]
    entries = [
        {"label": "first", "ids": [0, 1]},
        {"label": "duplicate", "ids": [1, 2]},
        {"label": "second", "ids": [2, 3]},
    ]
    clusters = _build_clusters(entries, articles)
    assert [cluster.topic_category for cluster in clusters] == ["first", "duplicate", "second"]
    assigned = {id(article) for cluster in clusters for article in cluster.articles}
    assert assigned == {id(articles[0]), id(articles[1]), id(articles[2]), id(articles[3])}
    assert all(len({id(a) for a in cluster.articles}) == len(cluster.articles) for cluster in clusters)


def test_salvage_follows_up_only_with_uncovered_articles(monkeypatch):
    clusterer = LLMClusterer(_config())
    articles = [_article(index) for index in range(6)]
    raw = (
        '{"clusters": [{"label": "recovered event", "ids": [0, 1]}, '
        '{"label": "truncated", "ids": [2, 3'
    )
    followup_calls: list[list[Article]] = []

    def followup(uncovered, prior_clusters, report_date=None):
        followup_calls.append(uncovered)
        return [
            ArticleCluster(topic_category=prior_clusters[0].topic_category, articles=[uncovered[0]])
        ]

    monkeypatch.setattr(clusterer, "_llm_cluster_followup", followup)

    result = clusterer._salvage_failed_chunk(articles, raw, None)

    assert len(followup_calls) == 1
    assert len(followup_calls[0]) == 4
    assert {cluster.topic_category for cluster in result} == {"recovered event"}


def test_clustering_user_prompt_has_stable_prefix_and_dynamic_tail():
    from newsprism.service.llm_clusterer import build_clustering_user_prompt

    prompt = build_clustering_user_prompt([_article(1)], snippet_chars=160)
    assert "Articles that do not fit any cluster are omitted entirely" in prompt
    assert '"unclustered"' not in prompt
    # The dynamic article JSON should be at the very end, after the static rules.
    assert prompt.rstrip().endswith('"snippet": "Event 1 coverage."}]')


def test_clustering_user_prompt_is_deterministic_for_same_articles():
    from newsprism.service.llm_clusterer import build_clustering_user_prompt

    first = build_clustering_user_prompt([_article(1), _article(2)], snippet_chars=160)
    second = build_clustering_user_prompt([_article(1), _article(2)], snippet_chars=160)
    assert first == second


def _cluster_with_url(label: str, urls: list[str]) -> ArticleCluster:
    articles = []
    for i, url in enumerate(urls):
        article = _article(i)
        article.url = url
        articles.append(article)
    return ArticleCluster(topic_category=label, articles=articles)


def test_merge_incremental_clusters_merges_same_label_and_keeps_new():
    from newsprism.service.llm_clusterer import merge_incremental_clusters

    previous = [
        _cluster_with_url("Event A", ["http://a/1"]),
        _cluster_with_url("Event B", ["http://b/1"]),
    ]
    new = [
        _cluster_with_url("Event A", ["http://a/2"]),
        _cluster_with_url("Event C", ["http://c/1"]),
    ]
    merged = merge_incremental_clusters(previous, new)
    assert len(merged) == 3
    by_label = {c.topic_category: c for c in merged}
    assert {a.url for a in by_label["Event A"].articles} == {"http://a/1", "http://a/2"}
    assert {a.url for a in by_label["Event B"].articles} == {"http://b/1"}
    assert {a.url for a in by_label["Event C"].articles} == {"http://c/1"}


def test_incremental_cluster_only_new_articles_when_previous_clusters_provided(monkeypatch):
    cfg = _config()
    cfg.clustering = {
        "llm_min_clusters_fallback": 1,
        "llm_max_articles_per_call": 60,
        "incremental_enabled": True,
    }
    clusterer = LLMClusterer(cfg)
    previous = [_cluster_with_url("Event A", ["http://old/1"])]
    old_article = _article(0)
    old_article.url = "http://old/1"
    new_article_1 = _article(1)
    new_article_1.url = "http://new/1"
    new_article_2 = _article(2)
    new_article_2.url = "http://new/2"
    all_articles = [old_article, new_article_1, new_article_2]
    clustered_new: list[list[Article]] = []

    def fake_cluster_chunked(articles, report_date=None):
        clustered_new.append(articles)
        return [_cluster_with_url("Event NEW", [a.url for a in articles])]

    monkeypatch.setattr(clusterer, "_cluster_chunked", fake_cluster_chunked)
    result = clusterer.cluster(all_articles, previous_clusters=previous)
    assert len(clustered_new) == 1
    assert {a.url for a in clustered_new[0]} == {"http://new/1", "http://new/2"}
    assert {c.topic_category for c in result} == {"Event A", "Event NEW"}


def _compact_config() -> Config:
    cfg = _config()
    cfg.clustering.update(compact_protocol_enabled=True, compact_snippet_chars=80)
    return cfg


def _response(content: str):
    from litellm import ModelResponse

    return ModelResponse(
        model="deepseek-v4-flash",
        choices=[{"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        usage={"prompt_tokens": 500, "completion_tokens": 20, "total_tokens": 520},
    )


def test_compact_protocol_reconstructs_singletons_and_preserves_article_provenance(monkeypatch):
    import litellm

    articles = [_article(i) for i in range(4)]
    articles[0].title = "Japan launches lunar mission"
    articles[2].title = "日本发射月球探测器"
    articles[2].origin_region = "jp"
    articles[2].content = "Full evidence remains available. " * 40
    original_content = articles[2].content
    monkeypatch.setattr(litellm, "completion", lambda **kw: _response(
        '{"groups":[{"label":"Japan lunar mission","ids":[0,2]}]}'
    ))

    result = LLMClusterer(_compact_config())._llm_cluster(articles)

    assert [{a.url for a in c.articles} for c in result] == [
        {articles[0].url, articles[2].url}, {articles[1].url}, {articles[3].url},
    ]
    assert result[0].topic_category == "Japan lunar mission"
    assert result[0].articles[0] is articles[2]
    assert result[0].sources == [articles[2].source_name, articles[0].source_name]
    assert result[0].articles[0].origin_region == "jp"
    assert articles[2].content == original_content


def test_compact_protocol_no_matches_keeps_every_singleton(monkeypatch):
    import litellm

    articles = [_article(i) for i in range(4)]
    monkeypatch.setattr(litellm, "completion", lambda **kw: _response('{"groups":[]}'))
    result = LLMClusterer(_compact_config())._llm_cluster(articles)
    assert [c.articles for c in result] == [[a] for a in articles]


def test_compact_protocol_single_article_needs_no_paid_call(monkeypatch):
    import litellm

    def unexpected_call(**kwargs):
        pytest.fail("A single article has no grouping decision to send to the API")

    monkeypatch.setattr(litellm, "completion", unexpected_call)
    article = _article(1)
    result = LLMClusterer(_compact_config()).cluster([article])
    assert result[0].articles == [article]
    assert result[0].topic_category == article.title


@pytest.mark.parametrize("raw", [
    '{}', '{"groups":null}', '{"groups":[{"label":"a","ids":[]}]}',
    '{"groups":[{"label":"a","ids":[0,4]}]}',
    '{"groups":[{"label":"a","ids":[0,-1]}]}',
    '{"groups":[{"label":"a","ids":[0,true]}]}',
    '{"groups":[{"label":"a","ids":[0,"1"]}]}',
    '{"groups":[{"label":"","ids":[0,1]}]}',
    '{"groups":[{"ids":[0,1]}]}', '{"groups":[{"label":"a","ids":[0,1]',
])
def test_compact_protocol_rejects_invalid_assignments(raw, monkeypatch):
    import litellm
    from newsprism.service.llm_clusterer import _ClusterParseError

    monkeypatch.setattr(litellm, "completion", lambda **kw: _response(raw))
    with pytest.raises(_ClusterParseError):
        LLMClusterer(_compact_config())._llm_cluster([_article(i) for i in range(4)])


def test_compact_redundant_assignments_are_normalized_without_a_paid_retry(monkeypatch):
    import litellm

    calls = []
    raw = '{"groups":[{"label":"first","ids":[0,1,1]},' \
          '{"label":"overlap","ids":[1,2]},{"label":"singleton","ids":[3]}]}'

    def completion(**kwargs):
        calls.append(kwargs)
        if len(calls) > 1:
            pytest.fail("Redundant known IDs must not trigger a paid retry")
        return _response(raw)

    monkeypatch.setattr(litellm, "completion", completion)
    articles = [_article(i) for i in range(5)]
    result = LLMClusterer(_compact_config()).cluster(articles)
    assert {a.url for c in result for a in c.articles} == {a.url for a in articles}
    assert sorted(len(c.articles) for c in result) == [1, 1, 1, 2]
    paired = next(c for c in result if len(c.articles) == 2)
    assert {a.url for a in paired.articles} == {articles[0].url, articles[1].url}
    assert paired.topic_category == "first"
    assert len(calls) == 1


def test_compact_invalid_response_recovers_locally_without_losing_unrelated_articles(monkeypatch):
    import litellm

    articles = [_article(i) for i in range(3)]
    for i, article in enumerate(articles):
        article.embedding = [float(i == j) for j in range(3)]
    monkeypatch.setattr(litellm, "completion", lambda **kw: _response(
        '{"groups":[{"label":"bad","ids":[0,99]}]}'
    ))
    result = LLMClusterer(_compact_config()).cluster(articles)
    assert {a.url for c in result for a in c.articles} == {a.url for a in articles}
    assert all(len(c.articles) == 1 for c in result)


def test_compact_prompt_preserves_titles_and_uses_bounded_positional_rows():
    from newsprism.service.llm_clusterer import build_compact_clustering_user_prompt

    articles = [_article(i) for i in range(3)]
    articles[2].source_name = articles[0].source_name
    for article in articles:
        article.content = "evidence " * 80
    prompt = build_compact_clustering_user_prompt(articles, snippet_chars=80)
    payload = json.loads(prompt.split("Articles:\n", 1)[1])
    assert payload == [
        [0, 0, "Event 0", articles[0].content[:80]],
        [1, 1, "Event 1", articles[1].content[:80]],
        [2, 0, "Event 2", articles[2].content[:80]],
    ]


def test_compact_wrong_schema_cannot_salvage_a_partial_legacy_result(monkeypatch):
    import litellm

    articles = [_article(i) for i in range(3)]
    for i, article in enumerate(articles):
        article.embedding = [float(i == j) for j in range(3)]
    monkeypatch.setattr(litellm, "completion", lambda **kw: _response(
        '{"clusters":[{"label":"wrong schema","ids":[0]}]}'
    ))
    result = LLMClusterer(_compact_config()).cluster(articles)
    assert {a.url for c in result for a in c.articles} == {a.url for a in articles}


def test_compact_truncated_legacy_response_never_enters_partial_legacy_recovery(monkeypatch):
    import litellm

    articles = [_article(i) for i in range(5)]
    for i, article in enumerate(articles):
        article.embedding = [float(i == j) for j in range(5)]
    responses = iter([
        '{"clusters":[{"ids":[0]},{"ids":[1]},{"ids":[2]},{"ids":[',
        '{"clusters":[]}',
    ])
    monkeypatch.setattr(litellm, "completion", lambda **kw: _response(next(responses)))
    result = LLMClusterer(_compact_config()).cluster(articles)
    assert {a.url for c in result for a in c.articles} == {a.url for a in articles}
