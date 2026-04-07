#!/usr/bin/env python3
"""Validate all configured RSS feed URLs and newsnow sources; report status.

Usage:
    python check_sources.py                           # check RSS feeds (fast)
    python check_sources.py --newsnow                 # also validate newsnow
    python check_sources.py --newsnow-url http://...  # specify newsnow base URL

For each source, tries: rss_url → rss_fallback → rsshub_url → scrape test
and reports how many articles each feed returned.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import feedparser
import httpx

sys.path.insert(0, ".")
from newsprism.config import SourceConfig, load_config

TIMEOUT = 15
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


@dataclass
class FeedResult:
    source: SourceConfig
    url: str
    label: str        # "primary" | "fallback" | "rsshub" | "scrape"
    status: str       # "ok" | "empty" | "error"
    article_count: int = 0
    http_code: int = 0
    error: str = ""


def check_feed(source: SourceConfig, url: str, label: str) -> FeedResult:
    try:
        with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url, headers={"User-Agent": "NewsPrism/1.0 (RSS reader)"})
        code = resp.status_code
        if code != 200:
            return FeedResult(source, url, label, "error", http_code=code,
                              error=f"HTTP {code}")
        feed = feedparser.parse(resp.text)
        count = len(feed.entries)
        if count == 0:
            return FeedResult(source, url, label, "empty", http_code=code,
                              error="Feed parsed but 0 entries")
        return FeedResult(source, url, label, "ok", article_count=count, http_code=code)
    except Exception as exc:
        return FeedResult(source, url, label, "error", error=str(exc)[:80])


def check_scrape(source: SourceConfig) -> FeedResult:
    """Quick check that the scrape index URL is reachable and returns HTML."""
    url = source.scrape_index_url or source.url
    try:
        with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return FeedResult(source, url, "scrape", "error",
                              http_code=resp.status_code, error=f"HTTP {resp.status_code}")
        # Count <a href> links as proxy for usable content
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        domain = url.split("/")[2]
        links = [a["href"] for a in soup.find_all("a", href=True)
                 if domain in a.get("href", "")]
        # Detect JS-rendered
        body_text = soup.find("body").get_text().strip() if soup.find("body") else ""
        is_spa = len(body_text) < 500
        note = " [⚠ JS-rendered SPA]" if is_spa else ""
        count = len(links)
        status = "empty" if count < 3 else "ok"
        return FeedResult(source, url, f"scrape{note}", status,
                          article_count=count, http_code=resp.status_code)
    except Exception as exc:
        return FeedResult(source, url, "scrape", "error", error=str(exc)[:80])


def run_checks(sources: list[SourceConfig]) -> list[tuple[SourceConfig, list[FeedResult]]]:
    tasks: list[tuple[SourceConfig, str, str]] = []
    for src in sources:
        if src.rss_url:
            tasks.append((src, src.rss_url, "primary RSS"))
        if src.rss_fallback:
            tasks.append((src, src.rss_fallback, "rss_fallback"))
        if src.rsshub_url:
            tasks.append((src, src.rsshub_url, "rsshub_url"))

    results: dict[str, list[FeedResult]] = {s.name: [] for s in sources}

    with ThreadPoolExecutor(max_workers=8) as ex:
        future_map = {
            ex.submit(check_feed, src, url, label): src.name
            for src, url, label in tasks
        }
        for future in as_completed(future_map):
            name = future_map[future]
            result = future.result()
            results[name].append(result)

    # Check scrape sources separately (sequential to avoid rate limits)
    for src in sources:
        if src.type == "scrape" or (not src.rss_url and src.scrape_index_url):
            results[src.name].append(check_scrape(src))

    return [(src, results[src.name]) for src in sources]


def check_newsnow(source: SourceConfig, newsnow_base: str) -> FeedResult:
    """Validate a newsnow source by calling its /api/s?id= endpoint."""
    api_url = f"{newsnow_base.rstrip('/')}/api/s?id={source.newsnow_id}"
    try:
        with httpx.Client(timeout=TIMEOUT) as client:
            resp = client.get(api_url, headers={"User-Agent": "NewsPrism/1.0"})
        if resp.status_code != 200:
            return FeedResult(source, api_url, "newsnow", "error",
                              http_code=resp.status_code, error=f"HTTP {resp.status_code}")
        data = resp.json()
        status_val = data.get("status", "")
        if status_val not in ("success", "cache"):
            return FeedResult(source, api_url, "newsnow", "error",
                              error=f"status={status_val!r}")
        items = data.get("items", [])
        if not items:
            return FeedResult(source, api_url, "newsnow", "empty",
                              http_code=resp.status_code, error="0 items returned")
        return FeedResult(source, api_url, "newsnow", "ok",
                          article_count=len(items), http_code=resp.status_code)
    except Exception as exc:
        return FeedResult(source, api_url, "newsnow", "error", error=str(exc)[:80])


def run_newsnow_checks(
    sources: list[SourceConfig], newsnow_base: str
) -> list[tuple[SourceConfig, FeedResult]]:
    newsnow_sources = [s for s in sources if s.newsnow_id]
    results: list[tuple[SourceConfig, FeedResult]] = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        future_map = {
            ex.submit(check_newsnow, src, newsnow_base): src
            for src in newsnow_sources
        }
        for future in as_completed(future_map):
            src = future_map[future]
            results.append((src, future.result()))
    results.sort(key=lambda t: t[0].name)
    return results


def print_newsnow_report(checks: list[tuple[SourceConfig, FeedResult]], base_url: str) -> None:
    print(f"\n{BOLD}{'─'*70}{RESET}")
    print(f"{BOLD}  NewsPrism — newsnow Source Validation  ({base_url}){RESET}")
    print(f"{'─'*70}\n")

    ok = fail = 0
    for src, r in checks:
        region_flag = {"cn": "🇨🇳", "us": "🇺🇸", "pl": "🇵🇱", "de": "🇩🇪", "nl": "🇳🇱"}.get(src.region, "🌐")
        if r.status == "ok":
            print(f"   {GREEN}✓{RESET} {region_flag} {src.name}  — {r.article_count} items  {CYAN}(id={src.newsnow_id}){RESET}")
            ok += 1
        elif r.status == "empty":
            print(f"   {YELLOW}○{RESET} {region_flag} {src.name}  — reached but 0 items  {CYAN}(id={src.newsnow_id}){RESET}")
            fail += 1
        else:
            print(f"   {RED}✗{RESET} {region_flag} {src.name}  — {r.error}  {CYAN}(id={src.newsnow_id}){RESET}")
            fail += 1

    print(f"\n{'─'*70}")
    print(f"{GREEN}✓ OK: {ok}{RESET}   {RED}✗ Failed: {fail}{RESET}   Total: {ok + fail}")
    print(f"{'─'*70}\n")

    if fail > 0:
        print(f"{BOLD}Troubleshooting:{RESET}")
        print("  • Is newsnow running?  docker compose ps newsnow")
        print(f"  • Is it reachable?     curl {base_url}/api/s?id=thepaper")
        print("  • Source IDs are in config/config.yaml → newsnow_id fields\n")


def print_report(checks: list[tuple[SourceConfig, list[FeedResult]]]) -> None:
    print(f"\n{BOLD}{'─'*70}{RESET}")
    print(f"{BOLD}  NewsPrism — RSS/Scrape Source Validation Report{RESET}")
    print(f"{'─'*70}\n")

    ok_count = warn_count = fail_count = 0

    for src, feed_results in checks:
        region_flag = {"cn": "🇨🇳", "us": "🇺🇸", "pl": "🇵🇱", "de": "🇩🇪", "nl": "🇳🇱"}.get(src.region, "🌐")
        print(f"{BOLD}{region_flag}  {src.name}  [{src.name_en}]{RESET}")

        if not feed_results:
            print(f"   {YELLOW}⚠ No feed URLs configured (newsnow-only source){RESET}")
            warn_count += 1
        else:
            source_ok = False
            for r in feed_results:
                if r.status == "ok":
                    print(f"   {GREEN}✓{RESET} [{r.label}] {r.article_count} articles  {CYAN}{r.url}{RESET}")
                    source_ok = True
                elif r.status == "empty":
                    print(f"   {YELLOW}○{RESET} [{r.label}] Feed reached but empty  {CYAN}{r.url}{RESET}")
                else:
                    print(f"   {RED}✗{RESET} [{r.label}] {r.error}  {CYAN}{r.url[:60]}{RESET}")

            if source_ok:
                ok_count += 1
            else:
                fail_count += 1
                print(f"   {RED}→ No working feed — needs fix{RESET}")

        print()

    total = ok_count + warn_count + fail_count
    print(f"{'─'*70}")
    print(f"{GREEN}✓ Working: {ok_count}{RESET}   {YELLOW}⚠ Partial: {warn_count}{RESET}   {RED}✗ Failed: {fail_count}{RESET}   Total: {total}")
    print(f"{'─'*70}\n")

    if fail_count > 0:
        print(f"{BOLD}Suggestions for failed sources:{RESET}")
        print("  1. Run a local RSSHub instance: https://docs.rsshub.app/deploy/")
        print("  2. Point failed sources at your local RSSHub endpoint in config/config.yaml")
        print("  3. For JS-rendered sites, use newsnow (preferred) or RSSHub\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate NewsPrism source feeds")
    parser.add_argument(
        "--newsnow", action="store_true",
        help="Also validate newsnow sources (requires newsnow to be running)"
    )
    parser.add_argument(
        "--newsnow-url",
        default=os.environ.get("NEWSNOW_BASE_URL", "http://localhost:3001"),
        help="newsnow base URL (default: $NEWSNOW_BASE_URL or http://localhost:3001)",
    )
    args = parser.parse_args()

    cfg = load_config()

    if args.newsnow:
        newsnow_sources = [s for s in cfg.sources if s.newsnow_id]
        print(f"Checking {len(newsnow_sources)} newsnow sources via {args.newsnow_url}...")
        t0 = time.monotonic()
        newsnow_checks = run_newsnow_checks(cfg.sources, args.newsnow_url)
        print_newsnow_report(newsnow_checks, args.newsnow_url)
        print(f"newsnow check completed in {time.monotonic() - t0:.1f}s\n")

    print(f"Checking {len(cfg.sources)} RSS/scrape sources... (parallel, ~{TIMEOUT}s timeout each)")
    t0 = time.monotonic()
    checks = run_checks(cfg.sources)
    elapsed = time.monotonic() - t0
    print_report(checks)
    print(f"RSS check completed in {elapsed:.1f}s\n")
