#!/usr/bin/env python
"""Retrospective shadow check for the impact evaluation brain.

Rebuilds one report date's candidate pool from the articles table, runs
LLM clustering + impact evaluation (2 LLM calls), and compares the
impact-ranked top-N against the historically published report (golden
data.json). Overlap is computed on article URLs, so it is robust to
different headline wording.

Usage:
  .venv/bin/python scripts/impact_shadow_check.py --date 2026-06-12 \
      --golden tests/goldens/2026-06-12.json [--top 20] [--window-hours 48]
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from newsprism.config import load_config                      # noqa: E402
from newsprism.repo.db import DB_PATH, _row_to_article        # noqa: E402
from newsprism.service.impact import ImpactAssessor           # noqa: E402
from newsprism.service.llm_clusterer import LLMClusterer      # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s: %(message)s")
for noisy in ("httpx", "httpcore", "LiteLLM", "litellm"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
logger = logging.getLogger("shadow-check")


def load_window_articles(report_date: str, window_hours: int, publish_utc_hour: int, cap: int):
    """Articles published within window_hours before the date's publish moment."""
    end = f"{report_date}T{publish_utc_hour:02d}:00:00"
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT * FROM articles
               WHERE published_at <= ?
                 AND published_at >= datetime(?, ?)
                 AND is_searched = 0
               ORDER BY published_at DESC
               LIMIT ?""",
            (end, end, f"-{window_hours} hours", cap),
        ).fetchall()
    return [_row_to_article(row) for row in rows]


def golden_stories(golden_path: Path) -> list[tuple[str, set[str]]]:
    """(headline, article-url-set) per story in the published golden report."""
    payload = json.loads(golden_path.read_text(encoding="utf-8"))
    stories: list[tuple[str, set[str]]] = []

    def _urls(entry: dict) -> set[str]:
        urls: set[str] = set()
        for key in ("articles", "footer_sources", "footer_sources_en", "source_groups"):
            for item in entry.get(key) or []:
                if isinstance(item, dict):
                    url = item.get("url") or item.get("link")
                    if url:
                        urls.add(url)
        if isinstance(entry.get("url"), str):
            urls.add(entry["url"])
        return urls

    def _walk(entries):
        for entry in entries or []:
            headline = entry.get("headline") or entry.get("topic") or "?"
            urls = _urls(entry)
            if urls:
                stories.append((headline, urls))

    _walk(payload.get("clusters"))
    _walk(payload.get("positive_stories"))
    for family in (payload.get("hot_topics") or []) + (payload.get("focus_storylines") or []):
        _walk(family.get("clusters") or family.get("summaries"))
    return stories


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--golden", required=True, type=Path)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--window-hours", type=int, default=48)
    parser.add_argument("--publish-utc-hour", type=int, default=6)
    parser.add_argument("--article-cap", type=int, default=450)
    args = parser.parse_args()

    cfg = load_config()
    articles = load_window_articles(args.date, args.window_hours, args.publish_utc_hour, args.article_cap * 8)
    # Per-source trim (newest 15) mirrors the production volume guard.
    per_source: dict[str, int] = {}
    trimmed = []
    for article in articles:  # already newest-first
        count = per_source.get(article.source_name, 0)
        if count >= 15:
            continue
        per_source[article.source_name] = count + 1
        trimmed.append(article)
    articles = trimmed[: args.article_cap]
    logger.info("Candidate pool: %d articles for %s (after per-source trim)", len(articles), args.date)
    if not articles:
        sys.exit("no articles in window — wrong DB?")

    clusters = LLMClusterer(cfg).cluster(articles)
    logger.info("Clustering: %d clusters", len(clusters))

    assessor = ImpactAssessor(cfg, weights_loader=lambda: {}, policy_loader=lambda: None)
    assessments = assessor.assess_clusters(clusters)

    ranked = sorted(zip(clusters, assessments), key=lambda pair: -pair[1].composite)
    kept = [(c, a) for c, a in ranked if a.status != "suppress"]
    top = kept[: args.top]

    print(f"\n{'=' * 100}\nIMPACT-RANKED TOP {len(top)} for {args.date}\n{'=' * 100}")
    for position, (cluster, assessment) in enumerate(top, 1):
        dims = " ".join(f"{d[:3]}={assessment.dim(d):.0f}" for d in
                        ("scope", "severity", "novelty", "actor_influence", "decision_relevance", "feelgood"))
        lead = cluster.articles[0].title[:64] if cluster.articles else "?"
        print(
            f"{position:>2}. [{assessment.composite:.3f}] sig={assessment.signal:.2f} "
            f"{assessment.status:<16} {assessment.display_category} | {dims}\n"
            f"     {lead}  ({len(cluster.sources)} src) — {assessment.rationale}"
        )

    feelgood = sorted(
        ((c, a) for c, a in zip(clusters, assessments) if a.dim("feelgood") >= 7 and a.status != "suppress"),
        key=lambda pair: -pair[1].dim("feelgood"),
    )
    print(f"\n正能量 candidates (feelgood>=7): {len(feelgood)}")
    for cluster, assessment in feelgood[:6]:
        lead = cluster.articles[0].title[:64] if cluster.articles else "?"
        print(f"  [{assessment.dim('feelgood'):.0f}] {lead} — {assessment.rationale}")

    golden = golden_stories(args.golden)
    if golden:
        top_urls: set[str] = set()
        for cluster, _ in top:
            top_urls |= {article.url for article in cluster.articles}
        matched = [(headline, urls) for headline, urls in golden if urls & top_urls]
        missed = [(headline, urls) for headline, urls in golden if not (urls & top_urls)]
        print(f"\n{'=' * 100}\nGOLDEN OVERLAP: {len(matched)}/{len(golden)} published stories "
              f"appear in the impact top-{len(top)} ({100 * len(matched) / len(golden):.0f}%)")
        if missed:
            print("Published stories the impact brain did NOT pick (the disagreements):")
            for headline, _ in missed:
                print(f"  - {headline[:80]}")

    suppressed = [(c, a) for c, a in ranked if a.status == "suppress"]
    print(f"\nsuppressed={len(suppressed)} of {len(clusters)} clusters; "
          f"llm_evaluated={sum(1 for a in assessments if a.evaluated_by_llm)}/{len(assessments)}")


if __name__ == "__main__":
    main()
