#!/usr/bin/env python3
"""Download self-hosted Google Fonts woff2 files with unicode-range subsetting.

Run once (or when updating font weights):
    python scripts/download_fonts.py

Downloads the same unicode-range-split woff2 files that Google Fonts CDN
would serve, then rewrites the CSS to point to local /fonts/ paths.
"""

import re
import urllib.request
from pathlib import Path

FONTS_DIR = Path(__file__).resolve().parent.parent / "newsprism" / "static" / "fonts"
GOOGLE_FONTS_URL = (
    "https://fonts.googleapis.com/css2"
    "?family=IBM+Plex+Mono:wght@400;500;600;700"
    "&family=Noto+Sans+SC:wght@400;500;700;900"
    "&family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700;8..60,800"
    "&display=swap"
)


def fetch_css() -> str:
    req = urllib.request.Request(
        GOOGLE_FONTS_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        },
    )
    with urllib.request.urlopen(req) as resp:
        import gzip

        raw = resp.read()
        try:
            return gzip.decompress(raw).decode()
        except Exception:
            return raw.decode()


def download_fonts(css: str) -> str:
    FONTS_DIR.mkdir(parents=True, exist_ok=True)

    urls = re.findall(r"url\((https://fonts\.gstatic\.com/[^)]+\.woff2)\)", css)
    unique_urls = sorted(set(urls))
    print(f"Found {len(unique_urls)} unique woff2 files")

    # Map gstatic URL -> local filename
    url_to_local: dict[str, str] = {}
    for i, url in enumerate(unique_urls):
        local_name = f"{i:03d}.woff2"
        local_path = FONTS_DIR / local_name
        if not local_path.exists():
            print(f"  Downloading {local_name}...", end="", flush=True)
            try:
                with urllib.request.urlopen(url) as resp:
                    data = resp.read()
                local_path.write_bytes(data)
                local_path.chmod(0o644)
                print(f" {len(data):,} bytes")
            except Exception as e:
                print(f" FAILED: {e}")
                continue
        else:
            print(f"  {local_name} already exists, skipping")
        url_to_local[url] = local_name

    # Rewrite CSS: replace gstatic URLs with /fonts/ local paths
    def replace_url(m: re.Match) -> str:
        original = m.group(1)
        local = url_to_local.get(original)
        if local:
            return f"url(/fonts/{local})"
        return m.group(0)

    local_css = re.sub(r"url\((https://fonts\.gstatic\.com/[^)]+\.woff2)\)", replace_url, css)
    return local_css


def main() -> None:
    print("Fetching Google Fonts CSS...")
    css = fetch_css()
    print(f"CSS fetched ({len(css):,} chars)")

    local_css = download_fonts(css)

    css_path = FONTS_DIR / "fonts.css"
    css_path.write_text(local_css, encoding="utf-8")
    css_path.chmod(0o644)
    print(f"\nWrote {css_path} ({len(local_css):,} chars)")


if __name__ == "__main__":
    main()
