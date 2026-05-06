from datetime import datetime, timezone

from newsprism.config import Config, SourceConfig
from newsprism.repo.db import (
    get_clusters_for_date,
    init_db,
    insert_cluster,
    insert_cluster_quality_report,
)
from newsprism.service.quality import QualityAssessor
from newsprism.service.storyline import StorylineStateMachine
from newsprism.types import Article, ArticleCluster, Cluster


def _config(editorial_values=None) -> Config:
    return Config(
        raw={},
        sources=[
            SourceConfig(
                name="Reuters",
                name_en="Reuters",
                url="https://reuters.example",
                rss_url=None,
                type="rss",
                weight=1.0,
                language="en",
                region="us",
                tier="editorial",
            ),
            SourceConfig(
                name="Example Official",
                name_en="Example Official",
                url="https://official.example",
                rss_url=None,
                type="rss",
                weight=1.0,
                language="en",
                region="us",
                tier="editorial",
            ),
            SourceConfig(
                name="BBC",
                name_en="BBC",
                url="https://bbc.example",
                rss_url=None,
                type="rss",
                weight=1.0,
                language="en",
                region="gb",
                tier="editorial",
            ),
        ],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={},
        active_search={},
        editorial_values=editorial_values or {},
        topic_equivalence={},
    )


def _article(
    title: str,
    source: str = "Reuters",
    content: str | None = None,
    *,
    official: bool = False,
    region: str = "us",
) -> Article:
    return Article(
        url=f"https://example.com/{title.replace(' ', '-')}",
        title=title,
        source_name=source,
        published_at=datetime.now(tz=timezone.utc),
        content=content or f"{title}. Officials provided details and other sources confirmed the event.",
        topics=["World News"],
        origin_region=region,
        source_kind="official_web" if official else "news",
        is_official_source=official,
    )


def test_quality_gate_marks_high_risk_single_source_for_more_evidence():
    assessor = QualityAssessor(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[_article("Missile attack escalates conflict near border")],
    )

    report = assessor.assess_cluster(cluster)

    assert report.status == "seek_more_evidence"
    assert report.decision.needs_more_evidence is True
    assert "high_risk_topic" in report.flags
    assert "single_source" in report.flags


def test_quality_report_scores_multi_source_confirmation_as_publishable():
    assessor = QualityAssessor(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[
            _article("Trade deal signed after talks", "Reuters", region="us"),
            _article("Trade deal signed after talks", "BBC", region="gb"),
        ],
    )

    report = assessor.assess_cluster(cluster)

    assert report.status == "publishable"
    assert report.overall_score >= 0.45
    assert report.source_diversity > 0.4
    assert report.confirmed_claims


def test_quality_persistence_keeps_old_cluster_reads_compatible(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    assessor = QualityAssessor(_config())
    cluster = ArticleCluster(
        topic_category="World News",
        articles=[_article("Trade deal signed after talks")],
    )
    report = assessor.assess_cluster(cluster)
    stored = Cluster(
        topic_category="World News",
        article_ids=[1],
        summary="**Trade deal**\n\nTrade deal signed after talks.",
        perspectives={},
        report_date="2026-05-05",
        quality_status=report.status,
        quality_score=report.overall_score,
    )

    cluster_id = insert_cluster(stored, db_path)
    insert_cluster_quality_report(cluster_id, report, db_path)

    rows = get_clusters_for_date("2026-05-05", db_path)
    assert rows[0].quality_status == report.status
    assert rows[0].quality_score == report.overall_score


def test_storyline_state_machine_detects_correction_and_developing():
    machine = StorylineStateMachine()
    correction = ArticleCluster(
        topic_category="World News",
        articles=[_article("Agency issues correction after earlier report")],
        storyline_key="agency-report",
    )
    assert machine.resolve_state(correction, [], None, datetime(2026, 5, 5).date()) == "correction"

    developing = ArticleCluster(
        topic_category="World News",
        articles=[_article("Trade talks continue after deadline")],
        storyline_key="trade-talks",
    )
    history = [
        Cluster(
            topic_category="World News",
            article_ids=[1],
            summary="Trade talks opened on Monday",
            perspectives={},
            report_date="2026-05-04",
            storyline_key="trade-talks",
        )
    ]

    assert machine.resolve_state(developing, history, None, datetime(2026, 5, 5).date()) == "developing"
