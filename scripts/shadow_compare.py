"""Compare a baseline report data.json against a candidate data.json.

Usage:
    python scripts/shadow_compare.py BASELINE.json CANDIDATE.json [--json OUT]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from newsprism.runtime.shadow_compare import compare_reports


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="Baseline data.json path")
    parser.add_argument("candidate", type=Path, help="Candidate data.json path")
    parser.add_argument("--json", type=Path, default=None, help="Optional JSON output path")
    args = parser.parse_args()

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    candidate = json.loads(args.candidate.read_text(encoding="utf-8"))
    result = compare_reports(baseline, candidate)

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(result["metrics"], ensure_ascii=False, indent=2))
    if result["failures"]:
        print("FAILURES:", file=sys.stderr)
        for failure in result["failures"]:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
