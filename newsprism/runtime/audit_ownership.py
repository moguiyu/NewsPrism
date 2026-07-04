"""One-off audit: how many stored clusters are cross-border 内政 from blocked sources.

Usage: python -m newsprism.runtime.audit_ownership [--output report.json]

This script is read-only — it does not modify the database or published reports.
It answers the question: "how many medias are pointing fingers to other countries'
home affairs?"
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter, defaultdict
from pathlib import Path

from newsprism.config import load_config
from newsprism.repo.db import (
    DB_PATH,
    get_articles_by_ids,
    get_clusters_for_date,
    get_conn,
)
from newsprism.service.impact import ImpactAssessor
from newsprism.types import (
    OWNERSHIP_GATE_REVIEW,
    OWNERSHIP_GATE_SUPPRESS,
    ArticleCluster,
)

logger = logging.getLogger(__name__)


def load_clusters_for_audit(db_path: Path) -> list[ArticleCluster]:
    """Load all clusters across all dates; reconstruct ArticleCluster from stored data."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT report_date FROM clusters ORDER BY report_date"
        ).fetchall()
        report_dates = [row["report_date"] for row in rows]

    all_clusters: list[ArticleCluster] = []
    for report_date in report_dates:
        clusters = get_clusters_for_date(report_date, db_path=db_path)
        for cluster in clusters:
            if not cluster.article_ids:
                continue
            articles = get_articles_by_ids(cluster.article_ids, db_path=db_path)
            if not articles:
                continue
            ac = ArticleCluster(
                topic_category=cluster.topic_category,
                articles=articles,
                storyline_key=cluster.storyline_key,
                storyline_name=cluster.storyline_name,
                storyline_role=cluster.storyline_role,
                storyline_confidence=cluster.storyline_confidence,
                storyline_state=cluster.storyline_state,
            )
            all_clusters.append(ac)

    return all_clusters


def audit(config_path: str = "config/config.yaml") -> dict:
    """Run the ownership audit over the full stored corpus."""
    cfg = load_config(config_path)
    assessor = ImpactAssessor(cfg)

    clusters = load_clusters_for_audit(DB_PATH)

    stats = {
        "total_clusters": len(clusters),
        "by_source": defaultdict(lambda: {"count": 0, "samples": []}),  # source -> stats
        "by_target_region": Counter(),  # target_region -> count
        "by_ownership": Counter(),  # ownership tier -> count
    }
    gated_clusters = 0

    for cluster in clusters:
        # Re-run classification (the impact eval LLM call)
        assessments = assessor.assess_clusters([cluster])
        if not assessments:
            continue
        assessment = assessments[0]

        target = assessment.target_region
        is_ha = assessment.is_home_affairs
        if target is None or not is_ha:
            continue

        for article in cluster.articles:
            ownership = assessor.source_ownerships.get(article.source_name, "state_influenced_review")
            source_region = assessor.source_regions.get(article.source_name)
            if source_region is None or source_region == target:
                continue

            stats["by_target_region"][target] += 1

            if ownership in OWNERSHIP_GATE_SUPPRESS:
                stats["by_source"][article.source_name]["count"] += 1
                if len(stats["by_source"][article.source_name]["samples"]) < 3:
                    stats["by_source"][article.source_name]["samples"].append(
                        article.title[:80]
                    )
                stats["by_ownership"]["state_controlled_block"] += 1
                gated_clusters += 1
            elif ownership in OWNERSHIP_GATE_REVIEW:
                stats["by_ownership"]["needs_review"] += 1
                gated_clusters += 1

    stats["gated_clusters"] = gated_clusters
    stats["pct_of_total"] = (
        round(100 * gated_clusters / stats["total_clusters"], 1)
        if stats["total_clusters"] else 0
    )
    return stats


def print_report(stats: dict) -> None:
    """Pretty-print the audit report to stdout."""
    print("=" * 72)
    print("Ownership Gate Audit — Cross-Border 内政 Report")
    print("=" * 72)
    print(f"\nTotal clusters scanned: {stats['total_clusters']}")
    print(f"Gated (would be blocked/flagged): {stats['gated_clusters']} "
          f"({stats['pct_of_total']}%)\n")

    print("By source (state_controlled_block on foreign 内政):")
    print("-" * 60)
    for source, data in sorted(stats["by_source"].items(), key=lambda x: -x[1]["count"]):
        if data["count"] == 0:
            continue
        sample = data["samples"][0] if data["samples"] else ""
        print(f"  {source:24s} {data['count']:4d} clusters   (sample: \"{sample}\")")

    print(f"\nBy target country (whose 内政 is being reported on):")
    print("-" * 40)
    for region, count in stats["by_target_region"].most_common(15):
        print(f"  {region:4s} {count:5d}")

    print(f"\nBy ownership tier:")
    print("-" * 30)
    for tier, count in sorted(stats["by_ownership"].items()):
        print(f"  {tier:30s} {count:5d}")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit ownership gate impact")
    parser.add_argument("--output", type=str, default=None, help="Optional JSON output path")
    parser.add_argument("--config", type=str, default="config/config.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    stats = audit(args.config)
    print_report(stats)

    if args.output:
        Path(args.output).write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Report saved to {args.output}")


if __name__ == "__main__":
    main()
