from pathlib import Path

import pytest
import yaml

from newsprism.config import load_config
from newsprism.repo import (
    get_search_candidate_review,
    init_db,
    insert_search_candidate_review,
)
from newsprism.runtime.search_review import approve_review_binding
from newsprism.types import SearchCandidateReview


def _insert_review(db_path: Path, verdict: str = "country_editorial") -> int:
    review_id = insert_search_candidate_review(
        SearchCandidateReview(
            url="https://local-paper.example/event",
            domain="local-paper.example",
            title="Local event report",
            source_name="local-paper.example",
            target_label="France",
            target_region="fr",
            target_role="country",
            stage="country",
            verdict=verdict,
            decision="pending_review",
            reason="candidate_pending_review",
            identity_evidence={
                "source_type": verdict,
                "publisher_entity": "Local Paper",
                "publisher_region": "fr",
            },
        ),
        db_path=db_path,
    )
    assert review_id is not None
    return review_id


def test_approve_review_promotes_binding_to_sidecar_and_closes_queue(tmp_path):
    db_path = tmp_path / "newsprism.db"
    bindings_path = tmp_path / "search-source-bindings.yaml"
    init_db(db_path)
    review_id = _insert_review(db_path)

    result = approve_review_binding(review_id, db_path, bindings_path)

    payload = yaml.safe_load(bindings_path.read_text(encoding="utf-8"))
    assert result["domain"] == "local-paper.example"
    assert payload["source_verdicts"]["local-paper.example"] == {
        "verdict": "country_editorial",
        "region": "fr",
    }
    assert get_search_candidate_review(review_id, db_path)["decision"] == "accepted"


def test_config_loader_merges_promoted_sidecar_bindings(tmp_path):
    db_path = tmp_path / "newsprism.db"
    bindings_path = tmp_path / "data" / "search-source-bindings.yaml"
    config_path = tmp_path / "config" / "config.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        "sources: []\nfilter: {}\nclustering: {}\nactive_search:\n"
        "  reviewed_bindings_file: data/search-source-bindings.yaml\n"
        "  source_verdicts: {}\n",
        encoding="utf-8",
    )
    init_db(db_path)
    review_id = _insert_review(db_path)
    approve_review_binding(review_id, db_path, bindings_path)

    cfg = load_config(str(config_path))

    assert cfg.active_search["source_verdicts"]["local-paper.example"] == {
        "verdict": "country_editorial",
        "region": "fr",
    }


def test_approve_review_refuses_conflicting_publisher_region(tmp_path):
    db_path = tmp_path / "newsprism.db"
    init_db(db_path)
    review_id = insert_search_candidate_review(
        SearchCandidateReview(
            url="https://foreign.example/france",
            domain="foreign.example",
            title="France report",
            source_name="foreign.example",
            target_label="France",
            target_region="fr",
            target_role="country",
            stage="country",
            verdict="country_editorial",
            decision="pending_review",
            identity_evidence={"publisher_region": "qa"},
        ),
        db_path=db_path,
    )
    assert review_id is not None

    with pytest.raises(ValueError, match="conflicts"):
        approve_review_binding(
            review_id,
            db_path,
            tmp_path / "search-source-bindings.yaml",
        )
