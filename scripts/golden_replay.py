"""Golden-replay regression harness for NewsPrism rendered reports.

For each tests/goldens/<date>.json, compare the headline set against the
corresponding output/<date>/data.json (if present).  Overlap is measured with
fuzzy matching (rapidfuzz token_set_ratio >= threshold) because LLM-generated
headlines change wording between runs.

Usage:
    python scripts/golden_replay.py [--date YYYY-MM-DD] [--output-dir DIR]
                                    [--threshold 0.6] [--goldens-dir DIR]

Exit codes:
    0  all dates meet the coverage threshold (or no fresh output found)
    1  one or more dates fall below the threshold
"""

import argparse
import json
import sys
from pathlib import Path

# Allow imports from the repo root without installing the package
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from rapidfuzz import fuzz


def _load_headlines(data_path: Path) -> list[str]:
    """Return the list of headlines from a data.json file."""
    try:
        payload = json.loads(data_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  [warn] Could not read {data_path}: {exc}")
        return []
    return [str(c.get("headline") or "").strip() for c in payload.get("clusters") or [] if c.get("headline")]


def _is_covered(golden_headline: str, fresh_headlines: list[str], threshold: int) -> bool:
    """Return True if any fresh headline fuzzy-matches the golden one."""
    for fresh in fresh_headlines:
        if fuzz.token_set_ratio(golden_headline, fresh) >= threshold:
            return True
    return False


def run(
    goldens_dir: Path,
    output_dir: Path,
    dates: list[str] | None,
    threshold: float,
) -> bool:
    """Run the replay check. Returns True if all dates pass, False if any fail."""
    fuzzy_threshold = int(threshold * 100)  # convert 0.6 → 60 for rapidfuzz int API

    golden_files = sorted(goldens_dir.glob("*.json"))
    if dates:
        golden_files = [f for f in golden_files if f.stem in dates]

    if not golden_files:
        print("No golden files found.")
        return True

    any_failed = False
    checked = 0

    for golden_path in golden_files:
        date_str = golden_path.stem
        fresh_path = output_dir / date_str / "data.json"

        if not fresh_path.exists():
            print(f"{date_str}: skipped (no fresh output at {fresh_path})")
            continue

        checked += 1
        golden_headlines = _load_headlines(golden_path)
        fresh_headlines = _load_headlines(fresh_path)

        if not golden_headlines:
            print(f"{date_str}: skipped (no headlines in golden file)")
            continue

        covered = [h for h in golden_headlines if _is_covered(h, fresh_headlines, fuzzy_threshold)]
        uncovered = [h for h in golden_headlines if not _is_covered(h, fresh_headlines, fuzzy_threshold)]
        pct = len(covered) / len(golden_headlines)
        status = "OK" if pct >= threshold else "FAIL"
        print(
            f"{date_str}: {len(covered)}/{len(golden_headlines)} golden stories covered "
            f"({pct:.0%}) [{status}]"
        )
        if uncovered:
            for h in uncovered:
                print(f"  - uncovered: {h}")
        if pct < threshold:
            any_failed = True

    if checked == 0:
        print("No dates had both a golden file and fresh output — nothing compared.")
    elif any_failed:
        print(f"\nSummary: FAIL — one or more dates below {threshold:.0%} coverage threshold.")
    else:
        print(f"\nSummary: OK — all {checked} date(s) meet the {threshold:.0%} coverage threshold.")

    return not any_failed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fuzzy-match golden headlines against freshly-rendered reports."
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        action="append",
        dest="dates",
        help="Limit to this date (can be repeated). Default: all goldens.",
    )
    parser.add_argument(
        "--output-dir",
        metavar="DIR",
        default="output",
        help="Directory containing <date>/data.json rendered reports (default: output).",
    )
    parser.add_argument(
        "--goldens-dir",
        metavar="DIR",
        default=str(_REPO_ROOT / "tests" / "goldens"),
        help="Directory containing <date>.json golden snapshots.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.6,
        help="Minimum fraction of golden headlines that must be covered (default: 0.6).",
    )
    args = parser.parse_args()

    goldens_dir = Path(args.goldens_dir)
    output_dir = Path(args.output_dir)

    if not goldens_dir.exists():
        print(f"Goldens directory not found: {goldens_dir}")
        sys.exit(0)

    passed = run(goldens_dir, output_dir, args.dates, args.threshold)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
