# Self-Host Fonts & Fix CSP

**Date:** 2026-05-19
**Status:** Approved

## Problem

Cloudflare Radar scan of `news.moguiyu.top` shows all Google Fonts blocked by CORS/CSP violations. The 3 font families (Source Serif 4, IBM Plex Mono, Noto Sans SC) never load — the page falls back to system fonts. This also prevents correct rendering for China-based users where `fonts.gstatic.com` is blocked by the GFW.

Additionally, the Cloudflare Web Analytics beacon script violates the existing CSP `script-src` directive.

## Solution

Self-host all font files and remove external font CDN dependencies entirely.

## Changes

### 1. Font files — `newsprism/static/fonts/`

Download woff2 files for the exact weights currently referenced in the Google Fonts CSS:

- **Source Serif 4** — 400, 600, 700, 800 (optical size 8..60)
- **IBM Plex Mono** — 400, 500, 600, 700
- **Noto Sans SC** — 400, 500, 700, 900

Naming: `{Family}-{Weight}.woff2` (e.g., `SourceSerif4-400.woff2`).

### 2. Template — `templates/report-template.html`

Replace lines 19-23 (Google Fonts preconnect + stylesheet link) with inline `@font-face` declarations in the existing `<style>` block. Each `@font-face` points to `/fonts/{filename}.woff2`.

### 3. Renderer — `newsprism/runtime/renderer.py`

Add `_write_fonts()` method following the pattern of `_write_pwa_assets()`:
- Copy `static/fonts/*.woff2` to `output/fonts/`
- Skip if target files already exist (idempotent)
- Call it once in the render flow (alongside `_write_pwa_assets`)

### 4. Nginx — `config/nginx.conf`

- Remove `https://fonts.googleapis.com` from `style-src`
- Remove `https://fonts.gstatic.com` from `font-src`
- Add `woff2` extension to the static assets cache block (or rely on existing `.*\.(css|js|...)$` pattern with long expiry)

## Verification

1. Local render: `.venv/bin/python -m newsprism publish`
2. Open output HTML in browser with DevTools — confirm fonts load from `/fonts/` with 200 status
3. No CSP console errors
4. Deploy to fnOS and re-scan with Cloudflare Radar
