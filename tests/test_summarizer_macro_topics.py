from datetime import datetime, timezone
from types import SimpleNamespace

import litellm

from newsprism.config import Config
from newsprism.service.summarizer import PerspectiveGroupItem, PerspectiveItem, Summarizer
from newsprism.types import Article, ArticleCluster, ClusterSummary


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


def test_normalize_perspective_groups_drops_non_distinct_fallbacks():
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

    assert groups == []


def test_normalize_perspective_groups_merges_semantically_duplicate_angles():
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url=f"https://example.com/{source}",
                title="Japan earthquake",
                source_name=source,
                published_at=datetime.now(tz=timezone.utc),
                content="Earthquake casualties and rescue updates.",
            )
            for source in ("Reuters", "BBC", "NHK")
        ],
    )

    groups = summarizer._normalize_perspective_groups(
        cluster,
        raw_groups=[
            PerspectiveGroupItem(
                sources=["Reuters"],
                perspective="报道地震灾情及伤亡情况，关注救援进展和余震风险。",
            ),
            PerspectiveGroupItem(
                sources=["BBC"],
                perspective="报道地震伤亡及救援进展，关注余震风险。",
            ),
            PerspectiveGroupItem(
                sources=["NHK"],
                perspective="强调地方政府的避难安排与交通中断。",
            ),
        ],
        legacy_items=[],
    )

    assert len(groups) == 2
    assert groups[0].sources == ["Reuters", "BBC"]
    assert groups[1].sources == ["NHK"]


def test_numeric_grounding_blocks_unsupported_vote_count(monkeypatch):
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/senate",
                title="Senate approves measure 86–12",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="报道确认参议院以86–12票通过该法案。",
            )
        ],
    )
    summary = ClusterSummary(
        cluster=cluster,
        summary="**参议院通过制裁法案**\n\n参议院以75–11票通过该法案。",
        quality_status="publishable",
    )
    monkeypatch.setattr(summarizer, "_rewrite_grounded_summary", lambda *_args: None)

    summarizer._enforce_numeric_grounding(summary)

    assert "75–11" not in summary.summary
    assert summary.quality_status == "needs_review"
    assert summary.contested_claims == ["75–11票"]
    assert "unsupported_numeric_claim" in summary.quality_flags


def test_numeric_grounding_accepts_currency_scale_equivalent_claim():
    """Regression for cluster 5775 (2026-07-31): a Chinese claim like '114亿美元'
    must be recognized as supported when English sources say '$11.4 billion' /
    '$11.5 billion' — same fact, different currency notation, not a substring
    match."""
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/ap",
                title="EU lays out $11.4 billion for 7 AI gigafactories",
                source_name="AP",
                published_at=datetime.now(tz=timezone.utc),
                content="The EU unveiled $11.4 billion in funding for seven AI gigafactories across the bloc.",
            ),
            Article(
                url="https://example.com/hindu",
                title="EU announces AI investment package",
                source_name="The Hindu",
                published_at=datetime.now(tz=timezone.utc),
                content="Brussels pledged a package worth €10 billion ($11.5 billion) for AI gigafactories.",
            ),
        ],
    )
    summary = ClusterSummary(
        cluster=cluster,
        summary="**欧盟为7个AI超算中心投入114亿美元**\n\n欧盟宣布为7个人工智能超算中心投入114亿美元资金。",
        quality_status="publishable",
    )

    summarizer._enforce_numeric_grounding(summary)

    assert "114亿美元" in summary.summary
    assert summary.quality_status == "publishable"
    assert "unsupported_numeric_claim" not in summary.quality_flags


def test_numeric_grounding_headline_fallback_never_emits_raw_source_title(monkeypatch):
    """Regression for cluster 5779 (2026-07-31): when the LLM rewrite fails and
    the headline itself carries a genuinely unsupported number, the fallback
    must strip just the flagged value — never replace the whole headline with
    the raw, non-Chinese article title."""
    summarizer = Summarizer(_config())
    raw_title = "Цены на чипы Qualcomm вырастут на 15%"
    cluster = ArticleCluster(
        topic_category=raw_title,
        articles=[
            Article(
                url="https://example.com/ru",
                title=raw_title,
                source_name="RU Outlet",
                published_at=datetime.now(tz=timezone.utc),
                content="Компания Qualcomm заявила о повышении цен на чипы на 15% из-за роста издержек.",
            ),
        ],
    )
    summary = ClusterSummary(
        cluster=cluster,
        summary="**高通芯片提价23%**\n\n高通宣布芯片价格上调23%，理由是成本上升。",
        quality_status="publishable",
    )
    monkeypatch.setattr(summarizer, "_rewrite_grounded_summary", lambda *_args: None)

    summarizer._enforce_numeric_grounding(summary)

    headline = summary.summary.splitlines()[0]
    assert raw_title not in summary.summary
    assert headline != f"**{raw_title}**"
    assert "23%" not in summary.summary
    assert "高通" in headline
    assert summary.quality_status == "needs_review"


def test_numeric_grounding_accepts_source_supported_vote_count():
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/senate",
                title="Senate approves measure 86–12",
                source_name="Reuters",
                published_at=datetime.now(tz=timezone.utc),
                content="报道确认参议院以86–12票通过该法案。",
            )
        ],
    )
    summary = ClusterSummary(
        cluster=cluster,
        summary="**参议院通过制裁法案**\n\n参议院以86–12票通过该法案。",
        quality_status="publishable",
    )

    summarizer._enforce_numeric_grounding(summary)

    assert "86–12" in summary.summary
    assert summary.quality_status == "publishable"


def test_numeric_grounding_marks_conflicting_source_vote_counts(monkeypatch):
    summarizer = Summarizer(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            Article(
                url="https://example.com/a",
                title="Vote result",
                source_name="Source A",
                published_at=datetime.now(tz=timezone.utc),
                content="参议院以75–11票通过该法案。",
            ),
            Article(
                url="https://example.com/b",
                title="Updated vote result",
                source_name="Source B",
                published_at=datetime.now(tz=timezone.utc),
                content="参议院最终以86–12票通过该法案。",
            ),
        ],
    )
    summary = ClusterSummary(
        cluster=cluster,
        summary="**参议院通过法案**\n\n参议院以75–11票通过该法案。",
        quality_status="publishable",
    )
    monkeypatch.setattr(summarizer, "_rewrite_grounded_summary", lambda *_args: None)

    summarizer._enforce_numeric_grounding(summary)

    assert "75–11" not in summary.summary
    assert summary.quality_status == "needs_review"
