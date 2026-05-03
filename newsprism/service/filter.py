"""Keyword-based topic tagger.

Each article is tagged with 0–N topic categories from keywords.txt.
Matching is case-insensitive; CJK keywords match as substrings.

Inclusion rules (based on source tier):
  editorial  — world news sources (Reuters, AP, 澎湃, etc.)
               All articles pass. If no keyword matches → tagged "World News".
  tech       — tech-focused sources (IT之家, Ars Technica, etc.) [DEFAULT]
               All articles pass. If no keyword matches → tagged "Tech-General".
  portal     — mixed-content portals (头条, 网易新闻, 凤凰, etc.)
               Keyword filter still applies: articles with no match are dropped.

Layer: service (imports types, config; never imports repo or runtime)
"""
from __future__ import annotations

import logging
import re

from newsprism.config import Config
from newsprism.types import Article

logger = logging.getLogger(__name__)

# Fallback topic assigned when no keyword matches, by source tier
_TIER_FALLBACK = {
    "editorial": "World News",
    "tech": "Tech-General",
}

_DEFAULT_POSITIVE_INCLUDE_KEYWORDS = [
    "whale calf",
    "baby whale",
    "baby animal",
    "cute",
    "adorable",
    "heartwarming",
    "happy",
    "delight",
    "joy",
    "funny",
    "rescued",
    "rescue",
    "reunion",
    "festival",
    "celebration",
    "breakthrough",
    "小鲸",
    "鲸宝宝",
    "幼鲸",
    "动物宝宝",
    "可爱",
    "暖心",
    "开心",
    "快乐",
    "有趣",
    "搞笑",
    "治愈",
    "团圆",
    "庆祝",
    "节日",
    "获救",
]

_DEFAULT_POSITIVE_EXCLUDE_KEYWORDS = [
    "war",
    "attack",
    "death",
    "dead",
    "killed",
    "injury",
    "injured",
    "crime",
    "shooting",
    "murder",
    "lawsuit",
    "scandal",
    "disaster",
    "crash",
    "sanction",
    "tariff",
    "conflict",
    "crisis",
    "collapse",
    "plunge",
    "战争",
    "袭击",
    "死亡",
    "遇难",
    "伤亡",
    "受伤",
    "犯罪",
    "枪击",
    "谋杀",
    "诉讼",
    "丑闻",
    "灾难",
    "坠毁",
    "制裁",
    "关税",
    "冲突",
    "危机",
    "暴跌",
]


class TopicTagger:
    def __init__(self, cfg: Config) -> None:
        self.topics = cfg.topics  # {category: [keywords]}
        self.max_topics = cfg.filter.get("max_topics_per_article", 3)
        self.min_score = cfg.filter.get("min_topic_score", 0.1)
        positive_cfg = cfg.filter.get("positive_energy_pre_filter", {})
        self.positive_pre_filter_enabled = bool(positive_cfg.get("enabled", True))
        self.positive_pre_filter_topic = (
            str(positive_cfg.get("topic", "Positive Energy")).strip() or "Positive Energy"
        )
        self.positive_include_keywords = [
            str(keyword).strip().lower()
            for keyword in positive_cfg.get("include_keywords", _DEFAULT_POSITIVE_INCLUDE_KEYWORDS)
            if str(keyword).strip()
        ]
        self.positive_exclude_keywords = [
            str(keyword).strip().lower()
            for keyword in positive_cfg.get("exclude_keywords", _DEFAULT_POSITIVE_EXCLUDE_KEYWORDS)
            if str(keyword).strip()
        ]

        # Source name → tier lookup for inclusion decisions
        self.source_tiers: dict[str, str] = {s.name: s.tier for s in cfg.sources}

        # Pre-compile: {category: [(pattern, weight)]}
        self._patterns: dict[str, list[tuple[re.Pattern, float]]] = {}
        for category, keywords in self.topics.items():
            patterns = []
            for kw in keywords:
                # ASCII keywords: word-boundary match; CJK: substring match
                if re.search(r"[\u4e00-\u9fff]", kw):
                    pat = re.compile(re.escape(kw), re.IGNORECASE)
                else:
                    pat = re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
                patterns.append((pat, 1.0))
            self._patterns[category] = patterns

    def tag(self, article: Article) -> list[str]:
        """Return list of matching topic categories, sorted by match score."""
        text = f"{article.title}\n{article.content}"
        scores: dict[str, float] = {}

        for category, patterns in self._patterns.items():
            score = 0.0
            for pat, weight in patterns:
                matches = pat.findall(text)
                if matches:
                    score += weight * (1 + 0.2 * min(len(matches) - 1, 5))
            if score >= self.min_score:
                scores[category] = score

        sorted_cats = sorted(scores, key=lambda c: scores[c], reverse=True)
        return sorted_cats[: self.max_topics]

    def is_positive_energy_candidate(self, article: Article) -> bool:
        """Return True when a dropped article is worth rescuing for final positive review."""
        if not self.positive_pre_filter_enabled:
            return False
        text = f"{article.title}\n{article.content}".lower()
        if any(keyword and keyword in text for keyword in self.positive_exclude_keywords):
            return False
        return any(keyword and keyword in text for keyword in self.positive_include_keywords)

    def tag_all(self, articles: list[Article]) -> list[Article]:
        tagged: list[Article] = []
        dropped = 0
        rescued = 0

        for article in articles:
            topics = self.tag(article)
            positive_candidate = self.is_positive_energy_candidate(article)

            if topics:
                article.topics = list(
                    dict.fromkeys(topics + ([self.positive_pre_filter_topic] if positive_candidate else []))
                )
            else:
                tier = self.source_tiers.get(article.source_name, "tech")
                fallback = _TIER_FALLBACK.get(tier)

                if fallback is None:
                    if positive_candidate:
                        article.topics = [self.positive_pre_filter_topic]
                        rescued += 1
                    else:
                        # portal tier with no keyword match → exclude
                        dropped += 1
                        continue
                else:
                    # editorial/tech with no keyword match → keep with fallback tag
                    article.topics = list(
                        dict.fromkeys([fallback] + ([self.positive_pre_filter_topic] if positive_candidate else []))
                    )
                    logger.debug(
                        "[%s] No keyword match → %s: %s",
                        article.source_name, fallback, article.title[:60],
                    )

            tagged.append(article)

        logger.info(
            "Tagging: %d → %d articles kept (%d positive rescued, %d portal no-match dropped)",
            len(articles),
            len(tagged),
            rescued,
            dropped,
        )
        return tagged
