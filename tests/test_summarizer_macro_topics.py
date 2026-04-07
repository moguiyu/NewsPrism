from datetime import datetime, timezone
from types import SimpleNamespace

import litellm

from newsprism.config import Config
from newsprism.service.summarizer import PerspectiveGroupItem, PerspectiveItem, Summarizer
from newsprism.types import Article, ArticleCluster


def _config() -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={"style_guide_file": "config/style-guide.md", "max_tokens": 1200},
        output={
            "hot_topics": {
                "icon_allowlist": ["globe", "war", "trade", "chip", "ai", "energy"],
                "storyline_relation_batch_size": 2,
            }
        },
        active_search={},
        topic_equivalence={},
    )


def _cluster(title: str) -> ArticleCluster:
    return ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url=f"https://example.com/{title}",
                title=title,
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content=f"{title} body",
            )
        ],
    )


def _response(content: str):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))]
    )


def test_classify_storyline_relations_batches_and_preserves_pairs(monkeypatch):
    summarizer = Summarizer(_config())
    pair_candidates = [
        {
            "left_index": 1,
            "right_index": 2,
            "left_cluster": _cluster("Tariff hike announced"),
            "right_cluster": _cluster("Markets drop after tariff hike"),
            "left_history": {},
            "right_history": {},
            "signal_overlap": 2,
            "similarity": 0.81,
        },
        {
            "left_index": 2,
            "right_index": 3,
            "left_cluster": _cluster("Markets drop after tariff hike"),
            "right_cluster": _cluster("Team wins championship"),
            "left_history": {},
            "right_history": {},
            "signal_overlap": 0,
            "similarity": 0.12,
        },
        {
            "left_index": 3,
            "right_index": 4,
            "left_cluster": _cluster("Airport closure after storm"),
            "right_cluster": _cluster("Flights rerouted after storm"),
            "left_history": {},
            "right_history": {},
            "signal_overlap": 2,
            "similarity": 0.74,
        },
    ]
    calls = []

    def fake_completion(**kwargs):
        calls.append(kwargs["messages"][-1]["content"])
        prompt = kwargs["messages"][-1]["content"]
        if "[1,2]" in prompt and "[2,3]" in prompt:
            return _response(
                '{"relations":['
                '{"left_index":1,"right_index":2,"relation":"same_core_storyline","confidence":0.88},'
                '{"left_index":2,"right_index":3,"relation":"not_related","confidence":0.94}'
                "]}",
            )
        return _response(
            '{"relations":['
            '{"left_index":3,"right_index":4,"relation":"same_direct_spillover_storyline","confidence":0.78}'
            "]}",
        )

    monkeypatch.setattr(litellm, "completion", fake_completion)

    relations = summarizer.classify_storyline_relations(pair_candidates)

    assert len(calls) == 2
    assert relations == [
        {"left_index": 1, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.88},
        {"left_index": 2, "right_index": 3, "relation": "not_related", "confidence": 0.94},
        {"left_index": 3, "right_index": 4, "relation": "same_direct_spillover_storyline", "confidence": 0.78},
    ]


def test_classify_storyline_relations_retries_after_invalid_json(monkeypatch):
    summarizer = Summarizer(_config())
    pair_candidates = [
        {
            "left_index": 1,
            "right_index": 2,
            "left_cluster": _cluster("Tariff hike announced"),
            "right_cluster": _cluster("Markets drop after tariff hike"),
            "left_history": {},
            "right_history": {},
            "signal_overlap": 2,
            "similarity": 0.81,
        }
    ]
    calls = {"count": 0}

    def fake_completion(**kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return _response('{"relations":[{"left_index":1,"right_index":2')
        return _response(
            '{"relations":['
            '{"left_index":1,"right_index":2,"relation":"same_core_storyline","confidence":0.83}'
            "]}",
        )

    monkeypatch.setattr(litellm, "completion", fake_completion)

    relations = summarizer.classify_storyline_relations(pair_candidates)

    assert calls["count"] == 2
    assert relations == [
        {"left_index": 1, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.83}
    ]


def test_classify_storyline_relations_salvages_partial_assignments(monkeypatch):
    summarizer = Summarizer(_config())
    pair_candidates = [
        {
            "left_index": 1,
            "right_index": 2,
            "left_cluster": _cluster("Tariff hike announced"),
            "right_cluster": _cluster("Markets drop after tariff hike"),
            "left_history": {},
            "right_history": {},
            "signal_overlap": 2,
            "similarity": 0.81,
        },
        {
            "left_index": 3,
            "right_index": 4,
            "left_cluster": _cluster("Airport closure after storm"),
            "right_cluster": _cluster("Flights rerouted after storm"),
            "left_history": {},
            "right_history": {},
            "signal_overlap": 2,
            "similarity": 0.74,
        },
    ]

    def fake_completion(**kwargs):
        return _response(
            '{"relations":['
            '{"left_index":1,"right_index":2,"relation":"same_core_storyline","confidence":0.88},'
            '{"left_index":3,"right_index":4,"relation":"same_direct_spillover_storyline"'
        )

    monkeypatch.setattr(litellm, "completion", fake_completion)

    relations = summarizer.classify_storyline_relations(pair_candidates)

    assert relations == [
        {"left_index": 1, "right_index": 2, "relation": "same_core_storyline", "confidence": 0.88}
    ]


def test_normalize_perspective_groups_merges_and_backfills_missing_sources():
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/reuters",
                title="Tariff response",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="US angle",
            ),
            Article(
                url="https://example.com/bbc",
                title="Tariff response UK",
                source_name="BBC",
                published_at=datetime.now(tz=timezone.utc),
                content="UK angle",
            ),
            Article(
                url="https://example.com/zaobao",
                title="Tariff response SG",
                source_name="联合早报",
                published_at=datetime.now(tz=timezone.utc),
                content="SG angle",
            ),
        ],
    )

    groups = summarizer._normalize_perspective_groups(
        cluster,
        raw_groups=[
            PerspectiveGroupItem(
                sources=["Reuters", "BBC"],
                perspective="Western outlets focus on market repricing.",
            )
        ],
        legacy_items=[
            PerspectiveItem(
                source="联合早报",
                perspective="Asian coverage focuses on trade-chain fallout.",
            )
        ],
    )

    assert [(group.sources, group.perspective) for group in groups] == [
        (["Reuters", "BBC"], "Western outlets focus on market repricing."),
        (["联合早报"], "Asian coverage focuses on trade-chain fallout."),
    ]


def test_normalize_perspective_groups_ignores_invalid_sources_and_falls_back():
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/reuters",
                title="Tariff response",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="US angle",
            ),
            Article(
                url="https://example.com/bbc",
                title="Tariff response UK",
                source_name="BBC",
                published_at=datetime.now(tz=timezone.utc),
                content="UK angle",
            ),
        ],
    )

    groups = summarizer._normalize_perspective_groups(
        cluster,
        raw_groups=[
            PerspectiveGroupItem(
                sources=["Reuters", "Invalid Source"],
                perspective="",
            )
        ],
        legacy_items=[],
    )

    assert groups[0].sources == ["Reuters"]
    assert "差异化视角" in groups[0].perspective
    assert groups[1].sources == ["BBC"]
    assert "差异化视角" in groups[1].perspective
