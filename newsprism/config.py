"""Configuration loader — reads config.yaml + .env + input/keywords.txt."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SourceConfig:
    name: str
    name_en: str
    url: str
    rss_url: str | None
    type: str               # "rss" | "scrape" | "api"
    weight: float
    language: str
    region: str = "cn"
    tier: str = "tech"          # "editorial" | "tech" | "portal" (default: tech)
    perspective: str = ""
    scrape_index_url: str | None = None
    rsshub_url: str | None = None    # RSSHub fallback (tried if rss_url fails)
    rss_fallback: str | None = None  # Secondary RSS fallback URL
    newsnow_id: str | None = None    # newsnow source ID — tried first for Chinese sources
    enabled: bool = True
    skip_body_fetch: bool = False    # skip article body fetch; rely on newsnow hover field (for SPAs)


@dataclass
class Config:
    raw: dict[str, Any]
    sources: list[SourceConfig]
    topics: dict[str, list[str]]   # category → keyword list
    schedule: dict[str, Any]
    collection: dict[str, Any]
    filter: dict[str, Any]
    clustering: dict[str, Any]
    dedup: dict[str, Any]
    summarizer: dict[str, Any]
    output: dict[str, Any]
    active_search: dict[str, Any]
    editorial_values: dict[str, Any] = field(default_factory=dict)
    feelgood_keywords: dict[str, Any] = field(default_factory=dict)
    evolution: dict[str, Any] = field(default_factory=dict)

    # Topic equivalence: canonical topic → list of equivalent topics
    topic_equivalence: dict[str, list[str]] = field(default_factory=dict)

    # Feature flags
    use_llm_clustering: bool = True

    # From env
    litellm_api_key: str = field(default_factory=lambda: os.environ.get("LITELLM_API_KEY", ""))
    litellm_model: str = field(default_factory=lambda: os.environ.get("LITELLM_MODEL", "deepseek/deepseek-chat"))
    litellm_base_url: str = field(default_factory=lambda: os.environ.get("LITELLM_BASE_URL", "https://api.deepseek.com"))
    telegram_bot_token: str = field(default_factory=lambda: os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = field(default_factory=lambda: os.environ.get("TELEGRAM_CHAT_ID", ""))
    report_base_url: str = field(default_factory=lambda: os.environ.get("REPORT_BASE_URL", "http://localhost:8080"))
    
    # Active Search
    tavily_api_key: str = field(default_factory=lambda: os.environ.get("TAVILY_API_KEY", ""))
    brightdata_api_key: str = field(default_factory=lambda: os.environ.get("BRIGHTDATA_API_KEY", ""))
    brightdata_zone: str = field(default_factory=lambda: os.environ.get("BRIGHTDATA_ZONE", "serp_api1"))
    evaluator_model: str = field(default_factory=lambda: os.environ.get("EVALUATOR_MODEL", "deepseek/deepseek-chat"))
    x_bearer_token: str = field(default_factory=lambda: os.environ.get("X_BEARER_TOKEN", ""))
    youtube_api_key: str = field(default_factory=lambda: os.environ.get("YOUTUBE_API_KEY", ""))


def _parse_keywords(keywords_file: str | None) -> dict[str, list[str]]:
    """Parse keywords.txt into {category: [keywords]} dict.

    Selection is impact-driven, not keyword-driven; this only survives for
    optional legacy keyword files. Missing/unset → empty taxonomy.
    """
    if not keywords_file:
        return {}
    path = Path(keywords_file)
    if not path.exists():
        return {}
    topics: dict[str, list[str]] = {}
    current_category: str | None = None

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            current_category = None
            continue
        if line.startswith("#"):
            # Category header like "# AI & LLM"
            current_category = line.lstrip("#").strip()
            topics[current_category] = []
        elif current_category is not None:
            topics[current_category].append(line)

    return topics


def _resolve_config_path(config_root: Path, configured: str) -> Path:
    path = Path(configured)
    if path.is_absolute():
        return path
    return config_root / path


def _load_yaml_file(config_root: Path, configured: str, default: Any) -> Any:
    path = _resolve_config_path(config_root, configured)
    if not path.exists():
        return default
    return yaml.safe_load(path.read_text(encoding="utf-8")) or default


def load_config(config_path: str = "config/config.yaml") -> Config:
    path = Path(config_path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    config_root = path.parent.parent
    editorial_values_path = _resolve_config_path(
        config_root,
        raw.get("editorial_values_file", "config/editorial-values.yaml"),
    )
    editorial_values: dict[str, Any] = {}
    if editorial_values_path.exists():
        editorial_values = yaml.safe_load(editorial_values_path.read_text(encoding="utf-8")) or {}
    feelgood_keywords = _load_yaml_file(
        config_root,
        raw.get("feelgood_keywords_file", "config/feelgood_keywords.yaml"),
        {},
    )

    sources = [
        SourceConfig(
            name=s["name"],
            name_en=s["name_en"],
            url=s["url"],
            rss_url=s.get("rss_url"),
            type=s["type"],
            weight=float(s.get("weight", 1.0)),
            language=s.get("language", "zh"),
            region=s.get("region", "cn"),
            perspective=s.get("perspective", ""),
            scrape_index_url=s.get("scrape_index_url"),
            tier=s.get("tier", "tech"),
            rsshub_url=s.get("rsshub_url"),
            rss_fallback=s.get("rss_fallback"),
            newsnow_id=s.get("newsnow_id"),
            enabled=bool(s.get("enabled", True)),
            skip_body_fetch=bool(s.get("skip_body_fetch", False)),
        )
        for s in raw.get("sources", [])
        if s.get("enabled", True)
    ]

    topics = _parse_keywords(raw.get("filter", {}).get("keywords_file"))

    schedule = raw.get("schedule", {})
    if tz_override := os.environ.get("SCHEDULE_TIMEZONE"):
        schedule = {**schedule, "timezone": tz_override}

    return Config(
        raw=raw,
        sources=sources,
        topics=topics,
        schedule=schedule,
        collection=raw.get("collection", {}),
        filter=raw.get("filter", {}),
        clustering=raw.get("clustering", {}),
        dedup=raw.get("dedup", {}),
        summarizer=raw.get("summarizer", {}),
        output=raw.get("output", {}),
        active_search=raw.get("active_search", {}),
        editorial_values=editorial_values,
        feelgood_keywords=feelgood_keywords,
        evolution=raw.get("evolution", {}),
        topic_equivalence=raw.get("clustering", {}).get("topic_equivalence", {}),
        use_llm_clustering=bool(raw.get("clustering", {}).get("use_llm_clustering", True)),
    )
