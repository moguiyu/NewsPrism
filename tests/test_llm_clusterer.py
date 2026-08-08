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

    def llm_cluster(chunk):
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

    def llm_cluster(chunk):
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
