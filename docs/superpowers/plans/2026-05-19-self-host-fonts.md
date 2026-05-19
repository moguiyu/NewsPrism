# Self-Host Fonts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Self-host all 3 Google Font families (Source Serif 4, IBM Plex Mono, Noto Sans SC) to fix CSP font blocking and eliminate external CDN dependency.

**Architecture:** Download woff2 font files from Google Fonts with unicode-range subsetting (exact same optimization Google uses). Store in `newsprism/static/fonts/`. A download script fetches the CSS, parses woff2 URLs, downloads all files, and generates a `fonts.css` with local paths. The template links to this local CSS. The renderer copies fonts to the output directory during render. Nginx CSP is tightened to remove external font domains.

**Tech Stack:** Python (urllib, re), woff2 font format, Jinja2 template, nginx

---

### Task 1: Write font download script

**Files:**
- Create: `scripts/download_fonts.py`
- Output: `newsprism/static/fonts/` (directory + ~129 woff2 files + `fonts.css`)

- [ ] **Step 1: Write the download script**

```python
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
        import gzip, io

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
```

- [ ] **Step 2: Run the download script**

Run: `python3 scripts/download_fonts.py`
Expected: Downloads ~129 woff2 files + generates `newsprism/static/fonts/fonts.css`

- [ ] **Step 3: Verify font files and CSS exist**

Run: `ls newsprism/static/fonts/ | head -20 && echo "---" && wc -l newsprism/static/fonts/fonts.css`
Expected: List of numbered .woff2 files + fonts.css with ~130+ lines

- [ ] **Step 4: Commit font files**

```bash
git add newsprism/static/fonts/
git commit -m "feat: add self-hosted Google Fonts (woff2 with unicode-range subsetting)"
```

---

### Task 2: Update template to use local fonts

**Files:**
- Modify: `templates/report-template.html:18-23` (replace Google Fonts link with local CSS)

- [ ] **Step 1: Replace Google Fonts CDN link with local CSS**

In `templates/report-template.html`, replace lines 18-23:

```html
  <!-- Editorial typography -->
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link
    href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=Noto+Sans+SC:wght@400;500;700;900&family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700;8..60,800&display=swap"
    rel="stylesheet">
```

With:

```html
  <!-- Editorial typography (self-hosted) -->
  <link rel="stylesheet" href="/fonts/fonts.css">
```

- [ ] **Step 2: Verify no other Google Fonts references remain**

Run: `grep -n 'fonts.googleapis\|fonts.gstatic' templates/report-template.html`
Expected: No output (no matches)

- [ ] **Step 3: Commit template change**

```bash
git add templates/report-template.html
git commit -m "feat: use self-hosted fonts instead of Google Fonts CDN"
```

---

### Task 3: Update renderer to copy fonts to output

**Files:**
- Modify: `newsprism/runtime/renderer.py:409-428` (add `_write_fonts` method)
- Modify: `newsprism/runtime/renderer.py:904-905` (call `_write_fonts` in render)
- Test: `tests/test_renderer.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_renderer.py`:

```python
def test_render_copies_fonts_to_output(renderer, tmp_path):
    """Renderer copies static/fonts/ to output/fonts/ on render."""
    static_fonts = Path(__file__).resolve().parent.parent / "newsprism" / "static" / "fonts"
    if not (static_fonts / "fonts.css").exists():
        pytest.skip("Font files not downloaded yet — run scripts/download_fonts.py")

    summaries = [_make_summary("Font test")]
    renderer.render(summaries, date(2026, 5, 19), update_latest=False)

    output_fonts = tmp_path / "fonts"
    assert output_fonts.is_dir()
    assert (output_fonts / "fonts.css").exists()
    # At least one woff2 file
    woff2_files = list(output_fonts.glob("*.woff2"))
    assert len(woff2_files) > 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_renderer.py::test_render_copies_fonts_to_output -v`
Expected: FAIL (no `_write_fonts` method yet)

- [ ] **Step 3: Add `_write_fonts` method to renderer**

In `newsprism/runtime/renderer.py`, add after `_write_pwa_assets` (after line 428):

```python
    def _write_fonts(self) -> None:
        """Copy static/fonts/ to output/fonts/ (idempotent, skips existing)."""
        src_dir = Path(__file__).resolve().parent.parent / "static" / "fonts"
        if not src_dir.is_dir():
            return
        dest_dir = self.output_dir / "fonts"
        dest_dir.mkdir(parents=True, exist_ok=True)
        for src_file in src_dir.iterdir():
            if not src_file.is_file():
                continue
            dest_file = dest_dir / src_file.name
            if dest_file.exists():
                continue
            import shutil
            shutil.copy2(src_file, dest_file)
            dest_file.chmod(0o644)
```

- [ ] **Step 4: Call `_write_fonts` in `render()` method**

In `newsprism/runtime/renderer.py`, at line 905 (inside `render()`), add the call:

```python
        self._write_static_favicon(report_dir)
        self._write_pwa_assets()
        self._write_fonts()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_renderer.py::test_render_copies_fonts_to_output -v`
Expected: PASS

- [ ] **Step 6: Run existing renderer tests to verify no regressions**

Run: `.venv/bin/pytest tests/test_renderer.py -v`
Expected: All existing tests pass

- [ ] **Step 7: Commit**

```bash
git add newsprism/runtime/renderer.py tests/test_renderer.py
git commit -m "feat: renderer copies self-hosted fonts to output directory"
```

---

### Task 4: Tighten nginx CSP

**Files:**
- Modify: `config/nginx.conf:14` (remove external font domains from CSP)
- Modify: `config/nginx.conf:40` (add woff2 to static assets cache)

- [ ] **Step 1: Update CSP header**

In `config/nginx.conf`, replace line 14:

```
    add_header Content-Security-Policy "default-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; script-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; worker-src 'self'; manifest-src 'self';" always;
```

With:

```
    add_header Content-Security-Policy "default-src 'self'; style-src 'self' 'unsafe-inline'; font-src 'self'; script-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; worker-src 'self'; manifest-src 'self';" always;
```

- [ ] **Step 2: Add woff2 to static assets cache block**

In `config/nginx.conf`, update line 40 to include woff2:

```
    location ~* \.(css|js|json|ico|png|svg|woff2)$ {
        expires 7d;
        add_header Cache-Control "public, immutable";
    }
```

(Font files are content-hashed by Google and never change — safe to cache aggressively with 7 day expiry.)

- [ ] **Step 3: Commit**

```bash
git add config/nginx.conf
git commit -m "feat: tighten CSP — remove external font CDN, cache woff2 assets"
```

---

### Task 5: End-to-end verification

- [ ] **Step 1: Run full test suite**

Run: `.venv/bin/pytest`
Expected: All tests pass

- [ ] **Step 2: Run local publish to generate a report**

Run: `.venv/bin/python -m newsprism publish`
Expected: Report generated in `output/` with `fonts/` directory present

- [ ] **Step 3: Verify font files in output**

Run: `ls output/fonts/ | head -10 && echo "---" && ls output/fonts/ | wc -l`
Expected: List of woff2 files + fonts.css, count matches static/fonts/

- [ ] **Step 4: Open the generated report in a browser**

Open `output/latest/index.html` in a browser with DevTools Network tab.
Verify:
- Fonts load from `/fonts/*.woff2` with HTTP 200
- No CSP errors in console
- Source Serif 4 renders in headings
- IBM Plex Mono renders in code/metadata
- Noto Sans SC renders in Chinese text (or system CJK fallback)

- [ ] **Step 5: Deploy to fnOS and verify**

```bash
bash scripts/deploy.sh
```

Then re-scan with Cloudflare Radar to confirm clean scan.
