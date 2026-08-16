from datetime import datetime, timezone

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


def test_llm_cluster_prompt_omits_unused_unclustered_field(monkeypatch):
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
    assert "omitted entirely" in prompt


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
