"""Regression tests for event-level clustering."""
from datetime import datetime, timedelta, timezone

from newsprism.config import Config, SourceConfig
from newsprism.service.clusterer import Clusterer
from newsprism.types import Article, ArticleCluster


def _config() -> Config:
    return Config(
        raw={},
        sources=[
            SourceConfig("华尔街见闻", "WallStreetCN", "https://wallstreetcn.com", None, "rss", 1.0, "zh", region="cn"),
            SourceConfig("The Guardian", "The Guardian", "https://theguardian.com", None, "rss", 1.0, "en", region="gb"),
            SourceConfig("联合早报", "Zaobao", "https://zaobao.com.sg", None, "rss", 1.0, "zh", region="sg"),
            SourceConfig("Reuters", "Reuters", "https://reuters.com", None, "rss", 1.0, "en", region="us"),
            SourceConfig("연합뉴스", "Yonhap", "https://yna.co.kr", None, "rss", 1.0, "ko", region="kr"),
        ],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={
            "semantic_threshold": 0.72,
            "strong_similarity_threshold": 0.85,
            "coherence_title_ngram_threshold": 0.08,
            "time_window_hours": 48,
            "max_clusters_per_report": 20,
        },
        dedup={},
        summarizer={},
        output={},
        active_search={},
        topic_equivalence={
            "World News": ["Geopolitics"],
            "Geopolitics": ["World News"],
        },
    )


def _article(source: str, title: str, topics: list[str], embedding: list[float], hours_ago: int = 0, content: str = "content") -> Article:
    return Article(
        url=f"https://example.com/{source}/{abs(hash(title))}",
        title=title,
        source_name=source,
        published_at=datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago),
        content=content * 20,
        topics=topics,
        embedding=embedding,
    )


def test_event_graph_merges_near_duplicate_angles():
    clusterer = Clusterer(_config())
    articles = [
        _article("华尔街见闻", "IEA plans biggest strategic oil reserve release", ["World News"], [1.0, 0.0]),
        _article("The Guardian", "IEA orders largest oil stockpile release as war shocks markets", ["Geopolitics"], [0.97, 0.03]),
        _article("联合早报", "US reserve release follows IEA emergency oil plan", ["World News"], [0.95, 0.05]),
    ]

    clusters = clusterer.cluster(articles)

    assert len(clusters) == 1
    assert clusters[0].is_multi_source is True
    assert len(clusters[0].sources) == 3


def test_article_cluster_ignores_flagged_and_legacy_placeholder_urls():
    real_articles = [
        _article("Reuters", "Real event report", ["World News"], [1.0, 0.0]),
        _article("The Guardian", "Real event analysis", ["World News"], [0.99, 0.01]),
    ]
    placeholders = [
        Article(
            url="placeholder:fr:event",
            title="待补充：法国声音",
            source_name="[法国声音待补]",
            published_at=datetime.now(tz=timezone.utc),
            content="",
            embedding=[1.0, 0.0],
            is_placeholder=False,
        ),
        Article(
            url="placeholder:de:event",
            title="待补充：德国声音",
            source_name="[德国声音待补]",
            published_at=datetime.now(tz=timezone.utc),
            content="",
            embedding=[1.0, 0.0],
            is_placeholder=True,
        ),
    ]

    cluster = ArticleCluster(topic_category="World", articles=real_articles + placeholders)

    assert cluster.sources == ["Reuters", "The Guardian"]
    assert cluster.is_multi_source is True


def test_clusterer_drops_synthetic_placeholder_articles_before_clustering():
    clusterer = Clusterer(_config())
    real = _article("Reuters", "Real event report", ["World News"], [1.0, 0.0])
    legacy_placeholder = Article(
        url="placeholder:fr:event",
        title="待补充：法国声音",
        source_name="[法国声音待补]",
        published_at=datetime.now(tz=timezone.utc),
        content="",
        embedding=[1.0, 0.0],
        is_placeholder=False,
    )

    clusters = clusterer.cluster([real, legacy_placeholder])
    assert len(clusters) == 1
    assert clusters[0].articles == [real]
    assert clusters[0].sources == ["Reuters"]
    assert clusterer.cluster([legacy_placeholder]) == []


def test_event_graph_keeps_related_follow_on_story_separate():
    clusterer = Clusterer(_config())
    articles = [
        _article("华尔街见闻", "IEA plans biggest strategic oil reserve release", ["World News"], [1.0, 0.0]),
        _article("The Guardian", "IEA orders largest oil stockpile release as war shocks markets", ["Geopolitics"], [0.97, 0.03]),
        _article("Reuters", "Russia offers mediation in Iran conflict", ["Geopolitics"], [0.75, 0.66]),
    ]

    clusters = clusterer.cluster(articles)

    assert len(clusters) == 2
    sizes = sorted(len(cluster.sources) for cluster in clusters)
    assert sizes == [1, 2]


def test_cluster_prunes_duplicate_source_articles():
    clusterer = Clusterer(_config())
    articles = [
        _article("연합뉴스", "IEA emergency release lifts uncertainty", ["World News"], [0.96, 0.04], content="short"),
        _article("연합뉴스", "IEA emergency release lifts uncertainty for oil markets", ["World News"], [0.95, 0.05], content="longer analysis"),
        _article("Reuters", "IEA moves to release reserves after conflict", ["Geopolitics"], [0.97, 0.03]),
    ]

    clusters = clusterer.cluster(articles)

    assert len(clusters) == 1
    assert clusters[0].sources.count("연합뉴스") == 1
    assert len(clusters[0].sources) == 2


def test_clusterer_does_not_apply_report_cap():
    clusterer = Clusterer(_config())
    # Orthogonal (one-hot) embeddings → 21 genuinely distinct events. With topic
    # gating removed, separation is purely by cosine similarity.
    articles = [
        _article(
            "Reuters",
            f"Distinct story {i}",
            [f"Unique Topic {i}"],
            [1.0 if j == i else 0.0 for j in range(21)],
        )
        for i in range(21)
    ]

    clusters = clusterer.cluster(articles)

    assert len(clusters) == 21
