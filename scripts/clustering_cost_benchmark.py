"""Compare clustering cost on a read-only news/telemetry export.

Offline by default; --live explicitly spends API tokens using the configured
account. No database writes or report publication. Example:
  .venv/bin/python scripts/clustering_cost_benchmark.py snapshot.json.gz \
      --date 2026-09-03 --date 2026-09-04 --date 2026-09-05 --max-batches 9

Snapshot keys: articles (SQLite rows), clusters (optional prior report rows),
events (optional llm_call_events rows). Historical inputs are reconstructed,
not exact replay manifests. Source text and embeddings stay in the local export.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import gzip
from itertools import combinations
import json
from pathlib import Path
import sys
from unittest.mock import patch
from uuid import uuid4
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import litellm

from newsprism.config import load_config
from newsprism.service.clustering_input import compact_same_source_near_duplicates
from newsprism.service.history import EventClusterValidator
from newsprism.service.llm_clusterer import (
    LLMClusterer, build_clustering_user_prompt, build_compact_clustering_user_prompt,
)
from newsprism.service.llm_telemetry import TrackedCompletion, _usage_fields
from newsprism.types import Article, ArticleCluster, is_real_article

PRICING_SOURCE = "https://api-docs.deepseek.com/zh-cn/quick_start/pricing/"
PRICING_AS_OF = "2026-09-05"


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def estimate_cost(event: dict) -> float | None:
    """CNY estimate at dated Flash rates; missing cache counts assume no hits."""
    model = str(event.get("model", "")).removeprefix("openai/").removeprefix("deepseek/")
    prompt, output = event.get("prompt_tokens"), event.get("completion_tokens")
    if model != "deepseek-v4-flash" or prompt is None or output is None:
        return None
    hit = event.get("prompt_cache_hit_tokens") or 0
    if min(prompt, output, hit) < 0 or hit > prompt or not event.get("created_at"):
        return None
    beijing = _datetime(event["created_at"]).astimezone(ZoneInfo("Asia/Shanghai"))
    peak = beijing.weekday() < 5 and (9 <= beijing.hour < 12 or 14 <= beijing.hour < 18)
    return (hit * .05 + (prompt - hit) * 1.5 + output * 4.5) / 1_000_000 * (2 if peak else 1)


def compare_clusters(baseline: list[ArticleCluster], candidate: list[ArticleCluster]) -> dict:
    def pairs(clusters):
        return {pair for c in clusters for pair in combinations(sorted({a.url for a in c.articles}), 2)}

    left, right = pairs(baseline), pairs(candidate)
    return {
        "baseline_cluster_count": len(baseline), "candidate_cluster_count": len(candidate),
        "baseline_article_count": len({a.url for c in baseline for a in c.articles}),
        "candidate_article_count": len({a.url for c in candidate for a in c.articles}),
        "baseline_pairs": len(left), "candidate_pairs": len(right),
        "shared_pairs": len(left & right),
        "baseline_only_pairs": [list(pair) for pair in sorted(left - right)],
        "candidate_only_pairs": [list(pair) for pair in sorted(right - left)],
    }


def _batches(snapshot: dict, dates: list[str], size: int, limit: int) -> list[tuple[str, list[Article]]]:
    selected = []
    for day_index, day in enumerate(dates):
        quota = limit // len(dates) + (day_index < limit % len(dates))
        if not quota:
            continue
        calls = [e for e in snapshot.get("events", []) if e.get("report_date") == day and e["stage"] == "clustering"]
        first = min(calls, key=lambda e: e["created_at"]) if calls else None
        cutoff = (_datetime(first["created_at"]) - timedelta(milliseconds=first.get("duration_ms") or 0)
                  if first else _datetime(day + "T05:20:00+00:00"))
        used = set()
        for cluster in snapshot.get("clusters", []):
            if cluster["report_date"] < day:
                ids = cluster["article_ids"]
                used.update(json.loads(ids) if isinstance(ids, str) else ids)
        articles = []
        for row in snapshot["articles"]:
            # Match the scheduler's SQL string cutoff, including the historical
            # ISO-T versus SQLite-space date format, for a comparable sample.
            if (row.get("id") in used or row.get("is_placeholder")
                    or row["published_at"] < (cutoff - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
                    or _datetime(row["published_at"]) > cutoff
                    or _datetime(row["created_at"]) > cutoff):
                continue
            embedding = row.get("embedding")
            article = Article(
                id=row.get("id"), url=row["url"], title=row["title"], source_name=row["source_name"],
                published_at=_datetime(row["published_at"]), content=row.get("content") or "",
                embedding=json.loads(embedding) if isinstance(embedding, str) else embedding,
                origin_region=row.get("origin_region"), is_searched=bool(row.get("is_searched")),
            )
            if is_real_article(article):
                articles.append(article)
        articles.sort(key=lambda a: a.published_at, reverse=True)
        chunks = [articles[i:i + size] for i in range(0, len(articles), size)]
        count = min(quota, len(chunks))
        indices = {round(i * (len(chunks) - 1) / max(1, count - 1)) for i in range(count)}
        selected.extend((day, chunks[i]) for i in sorted(indices))
    return selected


def _live_cluster(cfg, articles: list[Article], compact: bool, cold_cache: bool = False) -> tuple[list[ArticleCluster], list[dict]]:
    calls = []
    complete = litellm.completion

    def measured_completion(**kwargs):
        # Prevent SDK retries from silently multiplying this experiment's bill;
        # production's bounded parser recovery remains active and is measured.
        kwargs.update(num_retries=0, max_retries=0, timeout=60)
        if cold_cache:
            # Equalize cache exposure when old prompts already exist in the
            # provider cache. Keep production rules/input unchanged after a
            # unique, benchmark-only prefix of the same shape for both variants.
            messages = [dict(message) for message in kwargs["messages"]]
            messages[0]["content"] = f"Experiment ID: {uuid4()}\n" + messages[0]["content"]
            kwargs["messages"] = messages
        started = datetime.now(timezone.utc).isoformat()
        event = dict(model=kwargs["model"], created_at=started, status="api_error",
                     prompt_tokens=None, completion_tokens=None, total_tokens=None,
                     prompt_cache_hit_tokens=None, prompt_cache_miss_tokens=None,
                     estimated_cny=None, finish_reason=None, response=None)
        calls.append(event)
        try:
            response = complete(**kwargs)
        except Exception as exc:
            event["error_type"] = type(exc).__name__
            raise
        prompt, output, total, hit, miss = _usage_fields(response)
        event.update(prompt_tokens=prompt, completion_tokens=output, total_tokens=total,
                     prompt_cache_hit_tokens=hit, prompt_cache_miss_tokens=miss, status="ok",
                     finish_reason=response.choices[0].finish_reason,
                     response=response.choices[0].message.content)
        event["estimated_cny"] = estimate_cost(event)
        tracked = TrackedCompletion(response, None, Path("/unused"))
        tracked.mark = lambda status: event.update(status=status)
        return tracked

    config = replace(cfg, llm_telemetry_enabled=False, clustering={
        **cfg.clustering, "compact_protocol_enabled": compact, "incremental_enabled": False,
    })
    with patch("newsprism.service.llm_telemetry.litellm.completion", side_effect=measured_completion):
        clusters = LLMClusterer(config).cluster(articles)
    return clusters, calls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--date", action="append", required=True, dest="dates")
    parser.add_argument("--max-batches", type=int, choices=range(1, 13), default=6)
    parser.add_argument("--live", action="store_true", help="Spend tokens on bounded paired API calls")
    parser.add_argument("--cold-cache", action="store_true", help="Add unique experimental prefixes to equalize cache exposure")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    opener = gzip.open if args.snapshot.suffix == ".gz" else open
    with opener(args.snapshot, "rt") as stream:
        snapshot = json.load(stream)
    cfg = load_config()
    if args.live and not cfg.litellm_api_key:
        parser.error("--live requires the existing configured LLM API key")
    dates = list(dict.fromkeys(args.dates))
    stages = Counter()
    unpriced = unknown_cache = 0
    for event in snapshot.get("events", []):
        if event.get("report_date") not in dates:
            continue
        cost = estimate_cost(event)
        if cost is None:
            unpriced += 1
        else:
            stages[event["stage"]] += cost
        unknown_cache += event.get("prompt_cache_hit_tokens") is None
    result = dict(mode="live" if args.live else "offline", api_calls=0, cold_cache=args.cold_cache,
                  pricing_as_of=PRICING_AS_OF,
                  pricing_source=PRICING_SOURCE, historical_estimated_cny_by_stage=dict(stages),
                  historical_unpriced_events=unpriced, historical_unknown_cache_events=unknown_cache,
                  input_note="Reconstructed sample, not exact original publish inputs or a full report replay.",
                  batches=[])
    clusterer = LLMClusterer(cfg)
    batches = _batches(snapshot, dates, clusterer.max_articles_per_call, args.max_batches)
    if not batches:
        parser.error("No eligible articles for the requested dates")
    for day, articles in batches:
        representatives, _ = compact_same_source_near_duplicates(articles, cfg)
        item = dict(date=day, article_count=len(articles), representative_count=len(representatives),
                    legacy_prompt_chars=len(build_clustering_user_prompt(representatives, clusterer.article_snippet_chars)),
                    compact_prompt_chars=len(build_compact_clustering_user_prompt(representatives, clusterer.compact_snippet_chars)))
        if args.live:
            baseline, baseline_calls = _live_cluster(cfg, articles, compact=False, cold_cache=args.cold_cache)
            candidate, candidate_calls = _live_cluster(cfg, articles, compact=True, cold_cache=args.cold_cache)
            item.update(comparison=compare_clusters(baseline, candidate),
                        validated_comparison=compare_clusters(
                            EventClusterValidator(cfg).validate(baseline),
                            EventClusterValidator(cfg).validate(candidate)),
                        baseline_calls=baseline_calls, candidate_calls=candidate_calls,
                        usage_complete=all(e["estimated_cny"] is not None for e in baseline_calls + candidate_calls),
                        articles=[dict(url=a.url, title=a.title, source=a.source_name) for a in articles])
            result["api_calls"] += len(baseline_calls) + len(candidate_calls)
            print(f"Compared {day}: {len(articles)} articles", file=sys.stderr, flush=True)
        result["batches"].append(item)
        if args.output:
            args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    if not args.output:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
