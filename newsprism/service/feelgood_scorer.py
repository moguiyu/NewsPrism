"""Local scorer for the 今日正能量 lane.

Layer: service (imports types/config only; never imports repo or runtime).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from rapidfuzz import fuzz

from newsprism.config import Config
from newsprism.types import Article, ArticleCluster, ClusterSummary, RawArticle

logger = logging.getLogger(__name__)

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except Exception:  # pragma: no cover - exercised only when optional dependency is absent
    SentimentIntensityAnalyzer = None  # type: ignore[assignment]


_POSITIVE_FALLBACK_WORDS = (
    "adorable",
    "amazing",
    "beautiful",
    "celebrate",
    "cute",
    "delight",
    "funny",
    "happy",
    "heartwarming",
    "joy",
    "kindness",
    "playful",
    "reunion",
    "rescued",
    "stunning",
    "可爱",
    "开心",
    "快乐",
    "暖心",
    "有趣",
    "治愈",
    "惊喜",
    "美丽",
    "团圆",
    "获救",
)

_NEGATIVE_FALLBACK_WORDS = (
    "attack",
    "crime",
    "crisis",
    "dead",
    "death",
    "disaster",
    "injury",
    "killed",
    "lawsuit",
    "war",
    "战争",
    "死亡",
    "遇难",
    "犯罪",
    "危机",
    "灾难",
)

_LOCAL_RISK_BLOCKERS = (
    "accessories",
    "api access",
    "armed forces",
    "army",
    "available via api",
    "best ipad accessories",
    "buying guide",
    "crash",
    "crashed",
    "crew went down",
    "downed",
    "helicopter",
    "hormuz",
    "military",
    "missile",
    "model launch",
    "navy",
    "pilot went down",
    "pilots went down",
    "product roundup",
    "soldier",
    "strait of hormuz",
    "styli",
    "troop",
    "via api",
    "海峡",
    "霍尔木兹",
    "军事",
    "军方",
    "军队",
    "士兵",
    "坠落",
    "直升机",
)

_ENTITY_PATTERNS: dict[str, tuple[str, ...]] = {
    "animal": (
        r"\b(?:animal|dog|puppy|cat|kitten|bird|penguin|panda|otter|whale|dolphin|turtle)\b",
        r"(?:动物|萌宠|小狗|小猫|熊猫|企鹅|海豚|鲸|乌龟)",
    ),
    "child_family": (
        r"\b(?:child|children|kid|family|grandmother|grandfather|teacher|student)\b",
        r"(?:孩子|儿童|家庭|家人|爷爷|奶奶|老师|学生)",
    ),
    "everyday_people": (
        r"\b(?:neighbor|volunteer|community|resident|worker|friend|stranger)\b",
        r"(?:邻居|志愿者|社区|居民|朋友|路人|普通人)",
    ),
    "sports": (
        r"\b(?:athlete|player|fan|team|coach|mascot|match|game|sport)\b",
        r"(?:运动员|球员|球迷|球队|教练|吉祥物|比赛|体育)",
    ),
    "arts_culture": (
        r"\b(?:artist|museum|gallery|exhibition|music|movie|festival|craft|design)\b",
        r"(?:艺术家|博物馆|画廊|展览|音乐|电影|节日|手作|设计)",
    ),
    "science_org": (
        r"\b(?:NASA|telescope|space|scientist|researcher|fossil|ocean|wildlife)\b",
        r"(?:太空|宇宙|科学家|研究人员|化石|海洋|野生动物)",
    ),
    "city_community": (
        r"\b(?:city|town|village|park|library|school|garden|neighborhood)\b",
        r"(?:城市|小镇|村庄|公园|图书馆|学校|花园|街区)",
    ),
}

_NARRATIVE_PATTERNS: tuple[tuple[str, str, float], ...] = (
    ("rescue", r"\b(?:rescue|rescued|saved|adopted)\b|(?:获救|救助|收养)", 0.14),
    ("reunion", r"\b(?:reunion|reunited|finds? its way home)\b|(?:团圆|重逢|回家)", 0.14),
    ("delight", r"\b(?:delights?|charms?|cheers?|surprises?)\b|(?:开心|快乐|惊喜|治愈)", 0.12),
    ("first_time", r"\b(?:first time|first-ever|debut|newborn)\b|(?:首次|第一次|新生)", 0.10),
    ("record", r"\b(?:record-breaking|breaks? a record|milestone)\b|(?:刷新纪录|创纪录|里程碑)", 0.08),
    ("funny_cute", r"\b(?:funny|cute|adorable|playful)\b|(?:搞笑|爆笑|可爱|萌|好玩)", 0.12),
)

_REASON_LABELS = {
    "funny": ("轻松有趣", "Funny"),
    "cute": ("可爱治愈", "Cute"),
    "heartwarming": ("暖心好消息", "Heartwarming"),
    "awe": ("惊喜奇观", "Awe-inspiring"),
    "craft_life": ("生活美学", "Artful living"),
    "sports_moment": ("体育花絮", "Sports moment"),
    "science_wonder": ("科学趣闻", "Science wonder"),
    "light_entertainment": ("轻松娱乐", "Light entertainment"),
}

_ENTITY_REASONS = {
    "animal": ("萌宠动物", "Animals"),
    "child_family": ("亲子家庭", "Family"),
    "everyday_people": ("普通人的善意", "Everyday kindness"),
    "sports": ("运动瞬间", "Sports"),
    "arts_culture": ("文化艺术", "Arts and culture"),
    "science_org": ("科学自然", "Science and nature"),
    "city_community": ("城市生活", "Community"),
}

_PATTERN_REASONS = {
    "rescue": ("救助故事", "Rescue story"),
    "reunion": ("重逢团圆", "Reunion"),
    "delight": ("让人开心", "Uplifting"),
    "first_time": ("新鲜时刻", "First-time moment"),
    "record": ("突破时刻", "Milestone"),
    "funny_cute": ("轻松可爱", "Light and cute"),
}


@dataclass
class FeelgoodCandidate:
    article: Article
    score: float
    reason: str
    reason_en: str
    category: str
    source_weight: float


class FeelgoodScorer:
    """Rank already-collected NewsPrism articles for the positive lane."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.keywords = getattr(cfg, "feelgood_keywords", {}) or {}
        self.themes = self.keywords.get("themes", {}) if isinstance(self.keywords, dict) else {}
        self.blockers = [
            str(keyword).strip().lower()
            for keyword in (
                (self.keywords.get("blockers", {}) or {}).get("keywords", [])
                if isinstance(self.keywords, dict)
                else []
            )
            if str(keyword).strip()
        ]
        self.blockers = list(dict.fromkeys([*self.blockers, *_LOCAL_RISK_BLOCKERS]))
        self.entity_weights = (
            self.keywords.get("entity_weights", {}) if isinstance(self.keywords, dict) else {}
        ) or {}
        self.source_meta = {source.name: source for source in cfg.sources}
        self.analyzer = SentimentIntensityAnalyzer() if SentimentIntensityAnalyzer is not None else None
        positive_cfg = cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}
        self.local_min_score = float(positive_cfg.get("local_min_score", 0.42))
        self.source_diversity = bool(positive_cfg.get("source_diversity", True))

    def select_articles(self, articles: list[Article], limit: int = 5) -> list[ClusterSummary]:
        source_names = set(self.source_meta)
        candidates = [
            candidate
            for article in articles
            if article.source_name in source_names
            if (candidate := self._score_article(article)) is not None
        ]
        deduped = self._deduplicate(candidates)
        selected = self._select_diverse(deduped, limit)
        summaries = [self._to_summary(candidate) for candidate in selected]
        logger.info(
            "Positive energy local selection from existing articles: input=%d eligible=%d deduped=%d selected=%d headlines=%s",
            len(articles),
            len(candidates),
            len(deduped),
            len(summaries),
            [summary.cluster.articles[0].title for summary in summaries],
        )
        return summaries

    def _score_article(self, article: Article) -> FeelgoodCandidate | None:
        text = f"{article.title}\n{article.content}".strip()
        text_lower = text.lower()
        if self._blocked(text_lower):
            return None

        sentiment = self._sentiment_score(text)
        theme_score, theme_name = self._theme_score(text_lower)
        entity_score, entity_name = self._entity_score(text)
        pattern_score, pattern_name = self._pattern_score(text)
        source = self.source_meta.get(article.source_name)
        source_weight = float(getattr(source, "weight", 1.0) or 1.0)
        source_boost = max(-0.05, min(0.08, (source_weight - 1.0) * 0.08))

        score = (
            0.25
            + max(0.0, sentiment) * 0.25
            + theme_score
            + entity_score
            + pattern_score
            + source_boost
        )
        if sentiment < -0.15:
            score += sentiment * 0.25
        score = max(0.0, min(1.0, score))
        if score < self.local_min_score:
            return None

        category = self._category_for(article, theme_name)
        reason, reason_en = self._reason(theme_name, entity_name, pattern_name)
        return FeelgoodCandidate(
            article=article,
            score=round(score, 4),
            reason=reason,
            reason_en=reason_en,
            category=category,
            source_weight=source_weight,
        )



    def _blocked(self, text_lower: str) -> bool:
        for keyword in self.blockers:
            if not keyword:
                continue
            if re.search(r"[\u4e00-\u9fff]", keyword):
                if keyword in text_lower:
                    return True
                continue
            if re.search(r"\b" + re.escape(keyword) + r"\b", text_lower):
                return True
        return False

    def _sentiment_score(self, text: str) -> float:
        if self.analyzer is not None:
            return float(self.analyzer.polarity_scores(text).get("compound", 0.0))
        lowered = text.lower()
        positives = sum(1 for word in _POSITIVE_FALLBACK_WORDS if word.lower() in lowered)
        negatives = sum(1 for word in _NEGATIVE_FALLBACK_WORDS if word.lower() in lowered)
        if positives == negatives == 0:
            return 0.0
        return max(-1.0, min(1.0, (positives - negatives) / max(positives + negatives, 1)))

    def _theme_score(self, text_lower: str) -> tuple[float, str]:
        best_score = 0.0
        best_name = ""
        for theme_name, theme in self.themes.items():
            if not isinstance(theme, dict):
                continue
            keywords = [str(keyword).strip().lower() for keyword in theme.get("keywords", []) if str(keyword).strip()]
            matches = sum(1 for keyword in keywords if keyword in text_lower)
            if not matches:
                continue
            weight = float(theme.get("weight", 0.12) or 0.12)
            score = min(weight * (1 + 0.35 * (matches - 1)), weight * 1.7)
            if score > best_score:
                best_score = score
                best_name = str(theme_name)
        return best_score, best_name

    def _entity_score(self, text: str) -> tuple[float, str]:
        best_score = 0.0
        best_name = ""
        for entity_name, patterns in _ENTITY_PATTERNS.items():
            if not any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns):
                continue
            score = float(self.entity_weights.get(entity_name, 0.08) or 0.08)
            if score > best_score:
                best_score = score
                best_name = entity_name
        return best_score, best_name

    def _pattern_score(self, text: str) -> tuple[float, str]:
        best_score = 0.0
        best_name = ""
        for pattern_name, pattern, score in _NARRATIVE_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                if score > best_score:
                    best_score = score
                    best_name = pattern_name
        return best_score, best_name

    def _deduplicate(self, candidates: list[FeelgoodCandidate]) -> list[FeelgoodCandidate]:
        candidates = sorted(candidates, key=lambda candidate: candidate.score, reverse=True)
        kept: list[FeelgoodCandidate] = []
        seen_urls: set[str] = set()
        for candidate in candidates:
            normalized_url = self._normalize_url(candidate.article.url)
            if normalized_url in seen_urls:
                continue
            if any(fuzz.token_set_ratio(candidate.article.title, existing.article.title) >= 88 for existing in kept):
                continue
            kept.append(candidate)
            seen_urls.add(normalized_url)
        return kept

    def _select_diverse(self, candidates: list[FeelgoodCandidate], limit: int) -> list[FeelgoodCandidate]:
        if not self.source_diversity:
            return candidates[:limit]

        selected: list[FeelgoodCandidate] = []
        used_domains: set[str] = set()
        for candidate in candidates:
            domain = urlparse(candidate.article.url).netloc.lower()
            if domain in used_domains:
                continue
            selected.append(candidate)
            if domain:
                used_domains.add(domain)
            if len(selected) >= limit:
                return selected

        selected_urls = {candidate.article.url for candidate in selected}
        for candidate in candidates:
            if candidate.article.url in selected_urls:
                continue
            selected.append(candidate)
            if len(selected) >= limit:
                break
        return selected

    def _to_summary(self, candidate: FeelgoodCandidate) -> ClusterSummary:
        original = candidate.article
        article = Article(
            url=original.url,
            title=original.title,
            source_name=original.source_name,
            published_at=original.published_at,
            content=original.content,
            topics=list(dict.fromkeys([*original.topics, "Positive Energy", candidate.category])),
            embedding=original.embedding,
            id=original.id,
            clustered=original.clustered,
            is_searched=original.is_searched,
            search_region=original.search_region,
            source_kind=original.source_kind,
            platform=original.platform,
            account_id=original.account_id,
            is_official_source=original.is_official_source,
            origin_region=original.origin_region,
            searched_provider=original.searched_provider,
        )
        cluster = ArticleCluster(topic_category="Positive Energy", articles=[article])
        body = self._summary_body(article.content, article.title)
        summary = ClusterSummary(
            cluster=cluster,
            summary=f"**{article.title}**\n\n{body}",
            perspectives={article.source_name: body},
        )
        summary.positive_energy_score = candidate.score  # type: ignore[attr-defined]
        summary.positive_energy_reason = candidate.reason  # type: ignore[attr-defined]
        summary.positive_energy_reason_en = candidate.reason_en  # type: ignore[attr-defined]
        summary.positive_energy_category = candidate.category  # type: ignore[attr-defined]
        summary.positive_energy_source = article.source_name  # type: ignore[attr-defined]
        summary.selection_score = candidate.score  # type: ignore[attr-defined]
        summary.selection_reasons = [candidate.reason]  # type: ignore[attr-defined]
        return summary

    def _summary_body(self, content: str, fallback: str) -> str:
        text = re.sub(r"\s+", " ", content).strip()
        if not text:
            return fallback
        pieces = [piece.strip() for piece in re.split(r"(?<=[。！？.!?])\s+", text) if piece.strip()]
        body = " ".join(pieces[:2]) if pieces else text
        return body[:260].strip() or fallback

    def _normalize_url(self, url: str) -> str:
        parsed = urlparse(url)
        return parsed._replace(query="", fragment="").geturl().rstrip("/")

    def _category_for(self, article: Article, theme_name: str) -> str:
        for topic in article.topics:
            if topic and topic != "Positive Energy":
                return topic
        return theme_name or "Positive Energy"

    def _reason(self, theme_name: str, entity_name: str, pattern_name: str) -> tuple[str, str]:
        parts: list[tuple[str, str]] = []
        if theme_name:
            parts.append(_REASON_LABELS.get(theme_name, (theme_name, theme_name)))
        if entity_name:
            parts.append(_ENTITY_REASONS.get(entity_name, ("", "")))
        if pattern_name:
            parts.append(_PATTERN_REASONS.get(pattern_name, ("", "")))
        deduped = list(dict.fromkeys(part for part in parts if part[0] and part[1]))
        if not deduped:
            return "轻量好消息", "Uplifting"
        return "、".join(part[0] for part in deduped), ", ".join(part[1] for part in deduped)
