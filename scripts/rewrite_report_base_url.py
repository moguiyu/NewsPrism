"""Backfill generated public reports when the canonical hostname changes.

The migration is deliberately bounded to rendered HTML and top-level discovery
files. It never changes report JSON, SQLite data, or source code, and defaults
to a dry run so an operator can review the exact targets first.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re


_ALLOWED_NAMES = {"index.html", "sitemap.xml", "robots.txt", "feed.xml"}
_CHINESE_HTML = re.compile(r'<html\b[^>]*\blang=["\']zh(?:-CN)?["\']', re.IGNORECASE)
_LOGO_HREF = re.compile(r'(<a\s+class=["\']logo["\']\s+href=["\'])[^"\']*(["\'])')
_RELOAD_HANDLER = re.compile(
    r'\s+onclick=["\']window\.location\.reload\(\);\s*return false;["\']'
)


@dataclass(frozen=True)
class MigrationResult:
    changed_files: list[Path]


def _rewrite_html(value: str, *, from_base_url: str, to_base_url: str) -> str:
    rewritten = value.replace(from_base_url, to_base_url)
    home_href = "/cn/" if _CHINESE_HTML.search(rewritten) else "/"
    rewritten = _LOGO_HREF.sub(rf"\g<1>{home_href}\g<2>", rewritten)
    return _RELOAD_HANDLER.sub("", rewritten)


def rewrite_report_base_url(
    output_dir: Path,
    *,
    from_base_url: str,
    to_base_url: str,
    apply: bool,
) -> MigrationResult:
    """Return and optionally apply the generated artifacts changed by a hostname move."""
    output_dir = Path(output_dir)
    old_base = from_base_url.rstrip("/")
    new_base = to_base_url.rstrip("/")
    changed_files: list[Path] = []

    for path in sorted(output_dir.rglob("*")):
        if not path.is_file() or path.name not in _ALLOWED_NAMES:
            continue
        original = path.read_text(encoding="utf-8")
        rewritten = (
            _rewrite_html(original, from_base_url=old_base, to_base_url=new_base)
            if path.name == "index.html"
            else original.replace(old_base, new_base)
        )
        if rewritten == original:
            continue
        changed_files.append(path)
        if apply:
            path.write_text(rewritten, encoding="utf-8")

    return MigrationResult(changed_files=changed_files)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--from-base-url", required=True)
    parser.add_argument("--to-base-url", required=True)
    parser.add_argument("--apply", action="store_true", help="write changes instead of only listing them")
    args = parser.parse_args()

    result = rewrite_report_base_url(
        args.output_dir,
        from_base_url=args.from_base_url,
        to_base_url=args.to_base_url,
        apply=args.apply,
    )
    action = "updated" if args.apply else "would update"
    for path in result.changed_files:
        print(f"{action}: {path}")
    print(f"{action} {len(result.changed_files)} generated artifact(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
