"""subject_regions flows from the LLM item through to the assessment."""
from types import SimpleNamespace
from newsprism.service.impact import ImpactAssessor, ImpactItem
from newsprism.types import Article, ArticleCluster
from datetime import datetime, timezone


def _cfg():
    return SimpleNamespace(
        litellm_model="m", litellm_api_key="k", litellm_base_url="u",
        editorial_values={}, sources=[], output={},
    )


def _cluster():
    art = Article(
        url="https://x/1", title="t", source_name="AP",
        published_at=datetime.now(tz=timezone.utc), content="c" * 50, origin_region="us",
    )
    return ArticleCluster(topic_category="国际时政", articles=[art])


def test_build_assessment_carries_subject_regions():
    assessor = ImpactAssessor(_cfg())
    item = ImpactItem(cluster_index=1, scope=7, severity=6, subject_regions=["IL", " ir ", ""])
    a = assessor._build_assessment(_cluster(), item, assessor.weights())
    # normalized: lowercased, trimmed, empties dropped, capped at 3
    assert a.subject_regions == ["il", "ir"]


def test_signal_only_fallback_has_empty_subject_regions():
    assessor = ImpactAssessor(_cfg())
    a = assessor._build_assessment(_cluster(), None, assessor.weights())
    assert a.subject_regions == []
