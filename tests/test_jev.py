"""Tests for the TypeSafe Jev decision client and its editorial hooks."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
import pytest

from newsprism.repo import init_db
from newsprism.service import editorial_planner as planner
from newsprism.service.editorial_planner import resolve_display_duplicates, select_hot_topic_families
from newsprism.service.impact import ImpactAssessor
from newsprism.service.jev import JevClient
from newsprism.types import Article, ArticleCluster, ClusterSummary, ImpactAssessment


def _config():
    from newsprism.config import Config

    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={
            "hot_topics": {
                "enabled": True,
                "max_topic_tabs": 3,
                "tab_name_max_chars": 10,
                "main_lane_target": 15,
            }
        },
        active_search={},
        editorial_values={"impact": {"diversity": {"max_per_category": 8}}},
    )


def _summary(title: str, composite: float = 0.5, key: str | None = None) -> ClusterSummary:
    article = Article(
        url=f"https://example.com/{title.replace(' ', '-')}",
        title=title,
        source_name="Reuters",
        published_at=datetime.now(tz=timezone.utc),
        content=f"{title} body",
    )
    cluster = ArticleCluster(topic_category=title, articles=[article])
    cluster.impact = ImpactAssessment(cluster_key=title, composite=composite)
    cluster.display_category = "World"
    if key:
        cluster.storyline_key = key
        cluster.storyline_name = title
        cluster.storyline_role = "core"
        cluster.storyline_membership_status = "core"
        cluster.macro_topic_key = key
        cluster.macro_topic_name = title
    summary = ClusterSummary(cluster=cluster, summary=f"**{title}**\n\n{title} body")
    summary.impact = cluster.impact
    summary.display_category = "World"
    summary.quality_status = "publishable"
    if key:
        summary.storyline_key = key
        summary.storyline_name = title
        summary.storyline_role = "core"
        summary.storyline_membership_status = "core"
        summary.macro_topic_key = key
        summary.macro_topic_name = title
    return summary


def _response(noul: float = 0.9, score: float = 4.0, confidence: float = 0.8) -> dict:
    return {
        "model": "typesafe/jev-1.14-20261014",
        "answers": {
            "same_event": {"type": "noul", "noul": noul},
            "importance": {
                "type": "score",
                "score": score,
                "confidence": confidence,
                "legend": {str(i): f"level {i}" for i in range(6)},
                "probabilities": {str(i): 0.0 for i in range(6)},
            },
            "coherence": {
                "type": "score",
                "score": score,
                "confidence": confidence,
                "legend": {str(i): f"level {i}" for i in range(5)},
                "probabilities": {str(i): 0.0 for i in range(5)},
            },
            "label_support": {
                "type": "score",
                "score": score,
                "confidence": confidence,
                "legend": {str(i): f"level {i}" for i in range(5)},
                "probabilities": {str(i): 0.0 for i in range(5)},
            },
        },
        "usage": {"input_tokens": 500, "output_tokens": 20, "cost": 0.000021},
    }


def _client(tmp_path, *, shadow=False, handler=None, config=None) -> JevClient:
    db = tmp_path / "jev.db"
    init_db(db)
    cfg = {
        "enabled": True,
        "shadow": shadow,
        "model": "typesafe/jev-1.14",
        "base_url": "https://openrouter.ai/api/alpha/decisions",
        "api_key_env": "TEST_JEV_KEY",
        "timeout_seconds": 1.0,
        "max_retries": 0,
        "max_requests_per_run": 10,
        "max_input_tokens_per_run": 100_000,
        "stages": {
            "display_dedup": {"enabled": True, "max_pairs": 5},
            "hot_topics": {"enabled": True, "max_families": 3},
            "importance": {"enabled": True, "max_clusters": 5},
            "seeker": {"enabled": True},
        },
    }
    if config:
        cfg.update(config)
    return JevClient(
        cfg,
        api_key="test-key",
        db_path=db,
        transport=httpx.MockTransport(handler or (lambda request: httpx.Response(200, json=_response()))),
        sleep=lambda _: None,
    )


def test_jev_importance_parses_score_and_records_telemetry(tmp_path):
    client = _client(tmp_path)
    result = client.score_importance(
        title="Major treaty signed",
        summary="Two countries signed a security treaty.",
        sources=["Reuters", "AP"],
        regions=["us", "cn"],
        report_date="2026-10-21",
    )

    assert result is not None
    assert result[0] == pytest.approx(0.8)
    assert result[1] == pytest.approx(0.8)

    import sqlite3

    with sqlite3.connect(tmp_path / "jev.db") as conn:
        row = conn.execute(
            "SELECT stage, status, shadow, prompt_tokens, billed_cost_usd FROM jev_call_events"
        ).fetchone()
    assert row[0] == "importance"
    assert row[1] == "ok"
    assert row[2] == 0
    assert row[3] == 500
    assert row[4] == pytest.approx(0.000021)


def test_jev_shadow_records_but_integration_does_not_apply(tmp_path):
    client = _client(tmp_path, shadow=True)
    result = client.adjudicate_duplicate(
        left={"headline": "A", "summary": "Event A"},
        right={"headline": "B", "summary": "Event B"},
        similarity=0.8,
    )
    assert result == pytest.approx(0.9)
    import sqlite3

    with sqlite3.connect(tmp_path / "jev.db") as conn:
        status = conn.execute("SELECT status FROM jev_call_events").fetchone()[0]
    assert status == "shadow"


def test_jev_fails_open_on_http_error(tmp_path):
    client = _client(tmp_path, handler=lambda request: httpx.Response(500, json={"error": "down"}))
    assert client.score_importance(
        title="X", summary="Y", sources=[], regions=[], report_date="2026-10-21"
    ) is None


def test_jev_budget_stops_calls(tmp_path):
    client = _client(tmp_path, config={"max_requests_per_run": 1})
    assert client.score_importance(
        title="X", summary="Y", sources=[], regions=[], report_date="2026-10-21"
    ) is not None
    assert client.score_importance(
        title="Z", summary="W", sources=[], regions=[], report_date="2026-10-21"
    ) is None


def test_jev_hot_family_can_change_tab_order(tmp_path):
    low = _summary("low family", 0.9, key="low")
    low2 = _summary("low family 2", 0.8, key="low")
    high = _summary("high family", 0.4, key="high")
    high2 = _summary("high family 2", 0.3, key="high")

    def handler(request):
        payload = json.loads(request.content)
        label = payload["state"]["label"]
        score = 1.0 if label.startswith("high") else 0.0
        body = _response(score=score)
        return httpx.Response(200, json=body)

    client = _client(tmp_path, handler=handler)
    cfg = _config()
    cfg.output["hot_topics"]["max_topic_tabs"] = 1

    # Shadow=False: the coherent/important family is promoted even though it
    # has fewer members than the unrelated one.
    hot_topics, _, _ = select_hot_topic_families([low, low2, high, high2], cfg, jev_client=client)
    assert hot_topics and hot_topics[0]["macro_topic_name"] == "highfamily"


def test_jev_display_dedup_vetoes_a_low_probability_rule_duplicate(tmp_path, monkeypatch):
    client = _client(tmp_path, handler=lambda request: httpx.Response(200, json=_response(noul=0.1)))
    left = _summary("left")
    right = _summary("right")
    monkeypatch.setattr(planner, "_display_duplicate", lambda *args, **kwargs: (True, "centroid", 0.75))
    monkeypatch.setattr(planner, "_summary_centroid", lambda summary: None)

    hot, focus, regular, positive = resolve_display_duplicates([], [], [left, right], [], jev_client=client)
    assert len(regular) == 2
    assert not focus and not positive


def test_jev_display_dedup_merges_a_high_probability_unruled_pair(tmp_path, monkeypatch):
    client = _client(tmp_path, handler=lambda request: httpx.Response(200, json=_response(noul=0.9)))
    left = _summary("left")
    right = _summary("right")
    monkeypatch.setattr(planner, "_display_duplicate", lambda *args, **kwargs: (False, "", 0.70))
    monkeypatch.setattr(planner, "_summary_centroid", lambda summary: None)

    hot, focus, regular, positive = resolve_display_duplicates([], [], [left, right], [], jev_client=client)
    assert len(regular) == 1


def test_impact_audit_attaches_jev_importance(tmp_path):
    client = _client(tmp_path)
    article = Article(
        url="https://example.com/a",
        title="A",
        source_name="Reuters",
        published_at=datetime.now(tz=timezone.utc),
        content="A body",
    )
    cluster = ArticleCluster(topic_category="A", articles=[article])
    cluster.impact = ImpactAssessment(cluster_key="a", composite=0.5)

    assessor = ImpactAssessor.__new__(ImpactAssessor)
    assessor.jev_client = client
    assessor.source_regions = {"Reuters": "us"}
    assessor.audit_importance([cluster], report_date="2026-10-21")

    assert cluster.impact.jev_importance == pytest.approx(0.8)
