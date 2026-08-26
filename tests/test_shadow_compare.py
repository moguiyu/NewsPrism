"""Tests for the shadow report comparison harness."""
from __future__ import annotations

from newsprism.runtime.shadow_compare import (
    compare_reports,
    extract_report_metrics,
)


def _cluster(key: str, title: str, sources: list[str], articles: list[dict]) -> dict:
    return {
        "topic": key,
        "storyline_key": key,
        "headline": title,
        "summary": f"这是关于{key}的新闻摘要内容。",
        "sources": sources,
        "articles": articles,
        "perspectives": {},
        "grouped_perspectives": [],
        "source_groups": [],
    }


def _article(url: str, source: str, title: str) -> dict:
    return {
        "title": title,
        "url": url,
        "source": source,
        "is_placeholder": False,
        "is_real_article": True,
    }


def _report(clusters: list[dict], hot_topics: list[dict]) -> dict:
    hot_story_count = sum(len(ht.get("clusters", [])) for ht in hot_topics)
    return {
        "default_language": "zh",
        "cluster_count": len(clusters),
        "hot_topic_count": len(hot_topics),
        "hot_topic_story_count": hot_story_count,
        "total_cluster_count": len(clusters) + hot_story_count,
        "clusters": clusters,
        "hot_topics": hot_topics,
    }


def _hot_topic(key: str, clusters: list[dict]) -> dict:
    return {
        "storyline_key": key,
        "macro_topic_key": key,
        "macro_topic_name": key,
        "clusters": clusters,
    }


def _valid_report() -> dict:
    clusters = [
        _cluster(
            f"c{i}",
            f"中文标题{i}",
            [f"source{i}"],
            [_article(f"http://example.com/{i}", f"source{i}", f"Title {i}")],
        )
        for i in range(10)
    ]
    hot_topic = _hot_topic(
        "hot1",
        [
            _cluster("h1", "热点事件一", ["s1"], [_article("http://hot/1", "s1", "H1")]),
            _cluster("h2", "热点事件二", ["s2"], [_article("http://hot/2", "s2", "H2")]),
            _cluster("h3", "热点事件三", ["s3"], [_article("http://hot/3", "s3", "H3")]),
        ],
    )
    return _report(clusters=clusters, hot_topics=[hot_topic])


def test_extract_report_metrics_counts():
    data = _valid_report()
    metrics = extract_report_metrics(data)
    assert metrics["main_lane_count"] == 10
    assert metrics["hot_topic_count"] == 1
    assert metrics["hot_topic_story_count"] == 3
    assert len(metrics["cluster_keys"]) == 10
    assert metrics["hot_topic_keys"] == ["hot1"]


def test_compare_reports_passes_when_identical():
    data = _valid_report()
    result = compare_reports(data, data)
    assert result["passed"] is True
    assert result["failures"] == []


def test_compare_reports_fails_on_main_lane_count_drop():
    baseline = _valid_report()
    candidate = _report(clusters=baseline["clusters"][:5], hot_topics=baseline["hot_topics"])
    result = compare_reports(baseline, candidate)
    assert result["passed"] is False
    assert any("main-lane" in failure for failure in result["failures"])


def test_compare_reports_fails_on_hot_topic_story_count():
    baseline = _valid_report()
    candidate = _report(
        clusters=baseline["clusters"],
        hot_topics=[
            _hot_topic("hot1", baseline["hot_topics"][0]["clusters"][:1]),
        ],
    )
    result = compare_reports(baseline, candidate)
    assert result["passed"] is False
    assert any("hot-topic #1" in failure for failure in result["failures"])


def test_compare_reports_detects_language_purity_issue():
    baseline = _valid_report()
    candidate = _valid_report()
    candidate["clusters"][0]["headline"] = "English headline only"
    result = compare_reports(baseline, candidate)
    assert result["passed"] is False
    assert any("language" in failure.lower() for failure in result["failures"])


def test_compare_reports_accepts_bilingual_english_default_with_english_fields():
    baseline = _valid_report()
    baseline["default_language"] = "en"
    candidate = _valid_report()
    candidate["default_language"] = "en"
    for cluster in candidate["clusters"]:
        cluster["headline_en"] = f"English headline {cluster['storyline_key']}"
        cluster["summary_en"] = "English summary for this event."
    for hot in candidate["hot_topics"]:
        for cluster in hot["clusters"]:
            cluster["headline_en"] = f"English hot headline {cluster['storyline_key']}"
            cluster["summary_en"] = "English summary for hot topic item."
    result = compare_reports(baseline, candidate)
    assert result["passed"] is True
