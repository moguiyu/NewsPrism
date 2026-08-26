"""Inspect DeepSeek cache accounting from llm_call_events telemetry.

Usage:
    python scripts/cache_probe.py PATH/TO/newsprism.db [--day YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from newsprism.runtime.cache_stats import cache_stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db", type=Path, help="Path to SQLite telemetry DB")
    parser.add_argument("--day", default=None, help="Filter to YYYY-MM-DD")
    parser.add_argument("--json", action="store_true", help="Print JSON")
    args = parser.parse_args()

    stats = cache_stats(args.db, day=args.day)
    if args.json:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    else:
        print(f"events: {stats['events']}")
        print(f"total_tokens: {stats['total_tokens']}")
        print(f"prompt_tokens: {stats['prompt_tokens']}")
        print(f"cache_hit_tokens: {stats['cache_hit_tokens']}")
        print(f"cache_miss_tokens: {stats['cache_miss_tokens']}")
        print(f"cache_hit_rate: {stats['cache_hit_rate']:.3f}")
        print(f"cache_share_of_prompt: {stats['cache_share_of_prompt']:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
