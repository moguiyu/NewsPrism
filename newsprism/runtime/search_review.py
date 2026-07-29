"""Human approval workflow for discovered Active Search publisher bindings."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from newsprism.repo import (
    get_search_candidate_review,
    list_pending_search_candidate_reviews,
    update_search_candidate_review_decision,
)


def format_pending_reviews(limit: int = 50, db_path: Path = Path("data/newsprism.db")) -> str:
    rows = list_pending_search_candidate_reviews(limit=limit, db_path=db_path)
    if not rows:
        return "No pending Active Search source reviews."
    lines = []
    for row in rows:
        lines.append(
            f"#{row['id']} {row['target_label']}/{row['target_region']} "
            f"{row['stage']} {row['domain']} verdict={row['verdict']} reason={row['reason']}"
        )
    return "\n".join(lines)


def approve_review_binding(
    review_id: int,
    db_path: Path = Path("data/newsprism.db"),
    bindings_path: Path = Path("data/search-source-bindings.yaml"),
) -> dict[str, Any]:
    """Promote one reviewed candidate into the sidecar acceptance cache."""
    row = get_search_candidate_review(review_id, db_path=db_path)
    if row is None:
        raise ValueError(f"Search candidate review #{review_id} does not exist")
    if row["decision"] != "pending_review":
        raise ValueError(
            f"Search candidate review #{review_id} is {row['decision']}, not pending_review"
        )
    verdict = str(row["verdict"] or "")
    if verdict not in {"official_web", "country_editorial"}:
        raise ValueError(
            "Only official_web and country_editorial domains can be promoted here; "
            "social ownership must use an exact account binding"
        )

    domain = str(row["domain"] or "").strip().lower().removeprefix("www.")
    region = str(row["target_region"] or "").strip().lower()
    target_label = str(row["target_label"] or "").strip()
    if not domain or not region or not target_label:
        raise ValueError("Candidate is missing domain, target region, or target label")

    evidence: dict[str, Any] = {}
    if row.get("identity_evidence"):
        try:
            evidence = json.loads(str(row["identity_evidence"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            evidence = {}
    publisher_region = str(evidence.get("publisher_region") or "").strip().lower()
    if publisher_region and publisher_region != region:
        raise ValueError(
            f"Publisher region {publisher_region} conflicts with target region {region}"
        )

    bindings_path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        yaml.safe_load(bindings_path.read_text(encoding="utf-8"))
        if bindings_path.exists()
        else {}
    ) or {}
    source_verdicts = dict(payload.get("source_verdicts", {}) or {})
    binding: dict[str, str] = {"verdict": verdict, "region": region}
    if verdict == "official_web":
        binding["entity"] = target_label
    source_verdicts[domain] = binding
    payload["source_verdicts"] = source_verdicts
    payload.setdefault("official_account_bindings", {})

    temporary = bindings_path.with_suffix(bindings_path.suffix + ".tmp")
    temporary.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(bindings_path)
    update_search_candidate_review_decision(
        review_id,
        "accepted",
        "approved_by_editor",
        db_path=db_path,
    )
    return {"review_id": review_id, "domain": domain, **binding}


def reject_review(
    review_id: int,
    reason: str,
    db_path: Path = Path("data/newsprism.db"),
) -> None:
    if get_search_candidate_review(review_id, db_path=db_path) is None:
        raise ValueError(f"Search candidate review #{review_id} does not exist")
    update_search_candidate_review_decision(
        review_id,
        "rejected",
        reason.strip() or "rejected_by_editor",
        db_path=db_path,
    )
