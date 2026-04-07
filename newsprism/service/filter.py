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


class TopicTagger:
    def __init__(self, cfg: Config) -> None:
        self.topics = cfg.topics  # {category: [keywords]}
        self.max_topics = cfg.filter.get("max_topics_per_article", 3)
        self.min_score = cfg.filter.get("min_topic_score", 0.1)

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

    def tag_all(self, articles: list[Article]) -> list[Article]:
        tagged: list[Article] = []
        dropped = 0

        for article in articles:
            topics = self.tag(article)

            if topics:
                article.topics = topics
            else:
                tier = self.source_tiers.get(article.source_name, "tech")
                fallback = _TIER_FALLBACK.get(tier)

                if fallback is None:
                    # portal tier with no keyword match → exclude
                    dropped += 1
                    continue

                # editorial/tech with no keyword match → keep with fallback tag
                article.topics = [fallback]
                logger.debug(
                    "[%s] No keyword match → %s: %s",
                    article.source_name, fallback, article.title[:60],
                )

            tagged.append(article)

        logger.info(
            "Tagging: %d → %d articles kept (%d portal no-match dropped)",
            len(articles), len(tagged), dropped,
        )
        return tagged
