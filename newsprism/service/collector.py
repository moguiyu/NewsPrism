"""Article collector — newsnow → RSS → RSSHub → site API → HTML scrape.

Fetch chain per source (stops at first success):
  1. newsnow_id  → query self-hosted newsnow API (best for Chinese sources)
  2. rss_url     → feedparser
  3. rss_fallback → feedparser (FeedX mirror, etc.)
  4. rsshub_url  → feedparser (self-hosted RSSHub)
  5. site API    → site-specific JSON endpoint (wallstreetcn)
  6. scrape      → BeautifulSoup + trafilatura (static HTML only)

newsnow (https://github.com/ourongxing/newsnow) is a self-hosted service that
handles all the hard parts: undocumented JSON APIs, GB2312 encoding, anti-bot
headers, adaptive caching — for 40+ Chinese news sources. Run it locally or on
a machine you control and set NEWSNOW_BASE_URL in .env.

JS-rendered Chinese SPAs (澎湃, 华尔街见闻, 今日头条, 凤凰) have no public RSS
and can't be scraped with plain httpx. newsnow is the only practical solution
without a headless browser.

Layer: service (imports types, config; never imports repo or runtime)
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import feedparser
import httpx
import trafilatura
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from newsprism.config import Config, SourceConfig
from newsprism.types import RawArticle

logger = logging.getLogger(__name__)

_domain_last: dict[str, float] = {}


def _rate_limit(url: str, delay: float) -> None:
    domain = urlparse(url).netloc
    wait = delay - (time.monotonic() - _domain_last.get(domain, 0.0))
    if wait > 0:
        time.sleep(wait)
    _domain_last[domain] = time.monotonic()


# ─── SITE-SPECIFIC LINK SELECTORS ─────────────────────────────────────────────
# Used by the HTML scrape fallback. Without these, the generic scraper grabs
# navigation/footer links along with real articles.

ARTICLE_LINK_SELECTORS: dict[str, str] = {
    "tech.ifeng.com":    "a[href*='/tech/']",
    "news.ifeng.com":    "a[href*='/c/']",
    "www.163.com":       "a[href*='//www.163.com/']",
    "tech.163.com":      "a[href*='tech.163.com']",
    "36kr.com":          "a[href*='/p/']",
    "www.kaopu.news":    "article a, .post a, h2 a, h3 a",
    "niebezpiecznik.pl": "h2 a, h3 a, .entry-title a",
    "spidersweb.pl":     "h2 a, article a",
    "antyweb.pl":        "h2 a, article a",
    "www.heise.de":      "a[href*='/news/'], a[href*='/meldung/']",
    "www.golem.de":      "a[href$='.html']",
    "t3n.de":            "a[href*='/news/']",
    "tweakers.net":      "a[href*='/nieuws/']",
    "www.techzine.nl":   "h2 a, article a",
    "www.nu.nl":         "a[href*='/tech/']",
}

ARTICLE_URL_PATTERNS: dict[str, str] = {
    "36kr.com":      "/p/",
    "www.heise.de":  "/news/",
    "www.golem.de":  ".html",
    "tweakers.net":  "/nieuws/",
}

# Correct wallstreetcn API (from newsnow source analysis)
WALLSTREETCN_API = (
    "https://api-one.wallstcn.com/apiv1/content/information-flow"
    "?channel=global-channel&accept=article&limit=30"
)


class Collector:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.rate_delay = cfg.collection.get("rate_limit_delay", 2.0)
        self.timeout = cfg.collection.get("request_timeout", 20)
        self.max_age_hours = cfg.collection.get("max_age_hours", 6)
        # newsnow base URL from env; fall back to public instance
        self.newsnow_base = (
            os.environ.get("NEWSNOW_BASE_URL", "").rstrip("/")
            or "https://newsnow.busiyi.world"
        )

    async def collect_all(self) -> list[RawArticle]:
        import asyncio
        sem = asyncio.Semaphore(4)

        async def _one(src: SourceConfig) -> list[RawArticle]:
            async with sem:
                loop = asyncio.get_running_loop()
                try:
                    return await loop.run_in_executor(None, self._collect_source, src)
                except Exception as exc:
                    logger.error("Failed collecting %s: %s", src.name, exc)
                    return []

        results = await asyncio.gather(*[_one(s) for s in self.cfg.sources])
        articles = [a for batch in results for a in batch]
        logger.info("Collected %d raw articles total", len(articles))
        return articles

    # ─── DISPATCH ────────────────────────────────────────────────────────────

    def _collect_source(self, src: SourceConfig) -> list[RawArticle]:
        """Try each collection method in order, return first success."""

        # 1. newsnow API — best option for Chinese sources
        if src.newsnow_id:
            articles = self._try_newsnow(src)
            if articles:
                return articles
            logger.warning("[%s] newsnow failed, falling back", src.name)

        # 2. Primary RSS
        if src.rss_url:
            articles = self._try_rss(src, src.rss_url)
            if articles:
                return articles
            logger.warning("[%s] Primary RSS failed", src.name)

        # 3. RSS fallback (FeedX mirror etc.)
        if src.rss_fallback:
            articles = self._try_rss(src, src.rss_fallback)
            if articles:
                return articles

        # 4. RSSHub (self-hosted preferred)
        if src.rsshub_url:
            articles = self._try_rss(src, src.rsshub_url)
            if articles:
                return articles

        # 5. Wallstreetcn JSON API (no RSS; newsnow should handle this but keep as safety net)
        if "wallstreetcn.com" in src.url:
            return self._collect_wallstreetcn(src)

        # 6. Static HTML scrape (last resort; won't work on JS-rendered SPAs)
        if src.type == "scrape" or src.scrape_index_url:
            return self._collect_scrape(src)

        logger.warning("[%s] All collection methods failed", src.name)
        return []

    # ─── NEWSNOW ─────────────────────────────────────────────────────────────

    def _try_newsnow(self, src: SourceConfig) -> list[RawArticle]:
        try:
            return self._fetch_newsnow(src)
        except Exception as exc:
            logger.debug("[%s] newsnow error: %s", src.name, exc)
            return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=2, max=15))
    def _fetch_newsnow(self, src: SourceConfig) -> list[RawArticle]:
        """
        Query newsnow /api/s?id={newsnow_id} — returns article title + URL list.
        Then fetch full content for each article via trafilatura.

        newsnow handles: anti-bot headers, GB2312 encoding, undocumented JSON APIs,
        adaptive caching (5–30 min TTL per source type).
        """
        api_url = f"{self.newsnow_base}/api/s?id={src.newsnow_id}"
        _rate_limit(api_url, 1.0)  # newsnow is our own service; lighter rate limit
        logger.info("newsnow: %s → %s", src.name, api_url)

        with httpx.Client(timeout=self.timeout) as client:
            resp = client.get(api_url, headers={"User-Agent": "NewsPrism/1.0"})
            resp.raise_for_status()

        data = resp.json()
        if data.get("status") not in ("success", "cache"):
            raise ValueError(f"Unexpected newsnow status: {data.get('status')}")

        items = data.get("items", [])
        if not items:
            raise ValueError("newsnow returned empty items list")

        # newsnow provides updatedTime (cache refresh time) in ms
        # Individual articles don't always have timestamps; use batch time
        batch_ts_ms = data.get("updatedTime", 0)
        batch_dt = (
            datetime.fromtimestamp(batch_ts_ms / 1000, tz=timezone.utc)
            if batch_ts_ms else datetime.now(tz=timezone.utc)
        )

        articles: list[RawArticle] = []
        for item in items[:25]:
            url = item.get("url", "").strip()
            title = item.get("title", "").strip()
            if not url or not title:
                continue

            # Per-article timestamp: prefer item.pubDate or extra.date over batch time.
            # wallstreetcn and 36kr-quick include extra.date (Unix ms).
            # thepaper, toutiao, cankaoxiaoxi do not → fall back to batch_dt.
            pub_dt = batch_dt
            raw_date = item.get("pubDate") or (item.get("extra") or {}).get("date")
            if raw_date:
                try:
                    ts = float(raw_date)
                    if ts > 1e10:   # milliseconds
                        ts /= 1000
                    pub_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                except (ValueError, TypeError, OSError):
                    pass

            # Content strategy — newsnow only returns title+URL.
            # We try to get the article body, with source-aware fallbacks:
            #
            #  SSR pages (澎湃, 参考消息, 凤凰, 联合早报, 头条 external links):
            #    → trafilatura usually succeeds
            #
            #  SPA pages (36氪 article pages, 华尔街见闻 articles):
            #    → trafilatura gets empty shell
            #    → fall back to extra.hover (description, present in 36kr-renqi)
            #    → or use title as minimal content (enough for keyword tagging)

            extra = item.get("extra") or {}
            hover = extra.get("hover", "")   # description field (36kr-renqi only)

            content = self._fetch_article_content(url)

            if not content or len(content) < 150:
                # Use description from newsnow if available (better than title alone)
                content = hover or title
                # Chinese headlines are information-dense at 10–20 chars; 30 was too strict
                if len(content) < 10:
                    continue
                logger.debug("[%s] Using snippet content for: %s", src.name, title[:60])

            articles.append(RawArticle(
                url=url,
                title=title,
                source_name=src.name,
                published_at=pub_dt,
                content=content,
            ))

        logger.info("  → %d articles from %s (newsnow)", len(articles), src.name)
        return articles

    # ─── RSS ─────────────────────────────────────────────────────────────────

    def _try_rss(self, src: SourceConfig, feed_url: str) -> list[RawArticle]:
        try:
            return self._fetch_rss(src, feed_url)
        except Exception as exc:
            logger.debug("[%s] RSS %s failed: %s", src.name, feed_url, exc)
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    def _fetch_rss(self, src: SourceConfig, feed_url: str) -> list[RawArticle]:
        _rate_limit(feed_url, self.rate_delay)
        logger.info("RSS: %s → %s", src.name, feed_url)

        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            resp = client.get(feed_url, headers={"User-Agent": "NewsPrism/1.0 (RSS reader)"})
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        if not feed.entries:
            raise ValueError(f"Empty feed: {feed_url}")

        cutoff = datetime.now(tz=timezone.utc).timestamp() - self.max_age_hours * 3600
        articles: list[RawArticle] = []

        for entry in feed.entries:
            url = entry.get("link", "").strip()
            title = entry.get("title", "").strip()
            if not url or not title:
                continue

            pub = entry.get("published_parsed") or entry.get("updated_parsed")
            pub_dt = datetime(*pub[:6], tzinfo=timezone.utc) if pub else datetime.now(tz=timezone.utc)
            if pub_dt.timestamp() < cutoff:
                continue

            content = self._content_from_entry(entry) or self._fetch_article_content(url)
            if not content or len(content) < 150:
                continue

            articles.append(RawArticle(
                url=url, title=title, source_name=src.name,
                published_at=pub_dt, content=content,
            ))

        logger.info("  → %d articles from %s (RSS)", len(articles), src.name)
        return articles

    # ─── WALLSTREETCN JSON API ───────────────────────────────────────────────

    def _collect_wallstreetcn(self, src: SourceConfig) -> list[RawArticle]:
        """Safety-net API for wallstreetcn (newsnow should handle this first)."""
        _rate_limit(WALLSTREETCN_API, self.rate_delay)
        logger.info("API: %s (wallstreetcn)", src.name)
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.get(WALLSTREETCN_API, headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("[%s] wallstreetcn API failed: %s", src.name, exc)
            return []

        cutoff = datetime.now(tz=timezone.utc).timestamp() - self.max_age_hours * 3600
        articles: list[RawArticle] = []

        for item in (data.get("data", {}).get("items") or []):
            try:
                title = (item.get("title") or item.get("content_short", ""))[:200].strip()
                if not title:
                    continue
                ts = item.get("display_time", 0)
                pub_dt = datetime.fromtimestamp(ts / 1000 if ts > 1e10 else ts, tz=timezone.utc)
                if pub_dt.timestamp() < cutoff:
                    continue
                uri = item.get("uri") or f"https://wallstreetcn.com/articles/{item.get('id', '')}"
                content = item.get("content_text") or item.get("summary") or title
                articles.append(RawArticle(
                    url=uri, title=title, source_name=src.name,
                    published_at=pub_dt, content=content,
                ))
            except (KeyError, TypeError, ValueError):
                continue

        logger.info("  → %d articles from %s (API)", len(articles), src.name)
        return articles

    # ─── HTML SCRAPE ─────────────────────────────────────────────────────────

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=20))
    def _collect_scrape(self, src: SourceConfig) -> list[RawArticle]:
        """Static HTML scraper. Warns if page appears JS-rendered."""
        index_url = src.scrape_index_url or src.url
        _rate_limit(index_url, self.rate_delay)
        logger.info("Scrape: %s (%s)", src.name, index_url)

        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            resp = client.get(index_url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            })
            resp.raise_for_status()

        if self._is_js_rendered(resp.text):
            logger.warning(
                "[%s] Page appears JS-rendered — add newsnow_id or rsshub_url in config", src.name
            )

        soup = BeautifulSoup(resp.text, "lxml")
        domain = urlparse(index_url).netloc
        links = self._extract_article_links(soup, domain, index_url)

        articles: list[RawArticle] = []
        for url in links[:25]:
            _rate_limit(url, self.rate_delay)
            try:
                raw_html = self._fetch_raw_html(url)
                if not raw_html:
                    continue
                content = trafilatura.extract(
                    raw_html, include_comments=False, include_tables=False, favor_recall=True
                )
                if not content or len(content) < 150:
                    continue
                meta = trafilatura.extract_metadata(raw_html)
                title = (meta.title if meta and meta.title else "") or _title_from_soup(raw_html)
                if not title:
                    continue
                pub_dt = _parse_meta_date(meta) or datetime.now(tz=timezone.utc)
                articles.append(RawArticle(
                    url=url, title=title.strip()[:200], source_name=src.name,
                    published_at=pub_dt, content=content,
                ))
            except Exception as exc:
                logger.debug("Skipping %s: %s", url, exc)

        logger.info("  → %d articles from %s (scrape)", len(articles), src.name)
        return articles

    # ─── HELPERS ─────────────────────────────────────────────────────────────

    def _content_from_entry(self, entry: dict) -> str:
        raw = (
            entry.get("content", [{}])[0].get("value", "")
            or entry.get("summary", "")
        )
        if raw and len(raw) > 300:
            return BeautifulSoup(raw, "lxml").get_text(separator="\n").strip()
        return ""

    def _fetch_article_content(self, url: str) -> str:
        raw = self._fetch_raw_html(url)
        return trafilatura.extract(raw, include_comments=False, favor_recall=True) or "" if raw else ""

    def _fetch_raw_html(self, url: str) -> str:
        _rate_limit(url, self.rate_delay)
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                resp = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                # Fallback to utf-8 if the server doesn't provide a charset
                # httpx defaults to ISO-8859-1/latin-1 for text/html without charset,
                # which causes mojibake on Chinese sites like Zaobao.
                if not resp.charset_encoding:
                    resp.encoding = 'utf-8'
                return resp.text
        except Exception as exc:
            logger.debug("Fetch failed %s: %s", url, exc)
            return ""

    def _extract_article_links(self, soup: BeautifulSoup, domain: str, base_url: str) -> list[str]:
        selector = ARTICLE_LINK_SELECTORS.get(domain)
        url_must_contain = ARTICLE_URL_PATTERNS.get(domain, "")
        tags = soup.select(selector) if selector else soup.find_all("a", href=True)

        links: list[str] = []
        for tag in tags:
            href = tag.get("href", "")
            if not href:
                continue
            full = urljoin(base_url, href)
            if domain not in urlparse(full).netloc:
                continue
            if url_must_contain and url_must_contain not in full:
                continue
            if any(x in urlparse(full).path for x in
                   ["/tag/", "/category/", "/page/", "/author/", "/search", "/about", "/contact", "#"]):
                continue
            links.append(full)

        return list(dict.fromkeys(links))

    def _is_js_rendered(self, html: str) -> bool:
        soup = BeautifulSoup(html, "lxml")
        body = soup.find("body")
        return not body or len(body.get_text().strip()) < 500


# ─── UTILS ───────────────────────────────────────────────────────────────────

def _title_from_soup(html: str) -> str:
    tag = BeautifulSoup(html, "lxml").find("title")
    return tag.get_text().strip() if tag else ""


def _parse_meta_date(meta) -> datetime | None:
    date_str = getattr(meta, "date", None) if meta else None
    if not date_str:
        return None
    from dateutil import parser as dp
    try:
        dt = dp.parse(str(date_str))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
