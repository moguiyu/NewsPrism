import numpy as np

from newsprism.config import Config
from newsprism.service.freshness import FreshnessEvaluator
from newsprism.types import Cluster


class _FakeModel:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def encode(self, texts, normalize_embeddings=True, show_progress_bar=False):
        self.calls.extend(texts)
        rows = []
        for text in texts:
            length = float(len(text) or 1)
            rows.append(np.array([length, 1.0], dtype=float))
        return np.vstack(rows)


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
        summarizer={},
        output={},
        active_search={},
        topic_equivalence={},
    )


def test_score_text_to_historical_cluster_caches_text_embedding(monkeypatch):
    fake_model = _FakeModel()
    monkeypatch.setattr("newsprism.service.freshness._MODEL", fake_model)

    evaluator = FreshnessEvaluator(_config())
    hist_a = Cluster(id=1, topic_category="World News", article_ids=[1], summary="伊朗局势升级", perspectives={}, report_date="2026-03-14")
    hist_b = Cluster(id=2, topic_category="World News", article_ids=[2], summary="霍尔木兹风险上升", perspectives={}, report_date="2026-03-14")

    evaluator.score_text_to_historical_cluster("美国空袭伊朗目标", hist_a)
    evaluator.score_text_to_historical_cluster("美国空袭伊朗目标", hist_b)

    assert fake_model.calls.count("美国空袭伊朗目标") == 1
    assert "伊朗局势升级" in fake_model.calls
    assert "霍尔木兹风险上升" in fake_model.calls
