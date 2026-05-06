"""Generic storyline resolution for hotspot families.

Layer: service (imports config, types; never imports runtime or repo)
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import date

import numpy as np

from newsprism.config import Config
from newsprism.types import Article, ArticleCluster, Cluster, ClusterQualityReport, StorylineEvent

logger = logging.getLogger(__name__)

_DEFAULT_ICON = "globe"
_DIRECT_SIGNAL_TYPES = {"conflict", "trade", "election", "regulation", "disaster", "health"}
_SPILLOVER_SIGNAL_TYPES = {"transport", "energy", "market", "sports", "diplomacy"}
_GENERIC_SIGNAL_PATTERNS: dict[str, tuple[str, ...]] = {
    "conflict": (
        "战争", "战事", "冲突", "危机", "袭击", "空袭", "导弹", "军舰", "部队", "海军", "空军", "封锁", "制裁", "停火",
        "war", "conflict", "crisis", "strike", "attack", "missile", "troops", "navy", "military", "sanction", "ceasefire",
    ),
    "trade": (
        "关税", "贸易", "出口", "进口", "供应链", "芯片禁令", "禁运", "海关", "tariff", "trade", "export", "import",
        "customs", "duty", "quota", "supply chain", "export control",
    ),
    "election": (
        "选举", "投票", "议会", "总统选举", "组阁", "竞选", "计票", "election", "vote", "ballot", "campaign", "parliament",
        "coalition", "recount",
    ),
    "regulation": (
        "法案", "政策", "监管", "法院", "裁决", "调查", "批准", "禁令", "bill", "policy", "regulation", "court", "ruling",
        "approval", "probe", "ban",
    ),
    "disaster": (
        "地震", "洪水", "台风", "山火", "坠毁", "爆炸", "事故", "停运", "earthquake", "flood", "storm", "wildfire",
        "crash", "explosion", "disaster", "landslide",
    ),
    "health": (
        "疫情", "病毒", "医院", "疫苗", "病例", "传染", "outbreak", "virus", "hospital", "vaccine", "epidemic", "disease",
    ),
    "transport": (
        "航运", "海峡", "港口", "航班", "机场", "铁路", "停飞", "改道", "shipping", "strait", "port", "airline", "flight",
        "airport", "rail", "route", "reroute", "closure",
    ),
    "energy": (
        "石油", "原油", "天然气", "电力", "炼油", "输油管", "能源", "oil", "crude", "gas", "power", "pipeline", "refinery", "energy",
    ),
    "market": (
        "股市", "市场", "价格", "暴跌", "飙升", "selloff", "market", "stocks", "price", "surge", "slump", "bond", "rally",
    ),
    "sports": (
        "大奖赛", "比赛", "联赛", "杯赛", "F1", "赛事", "grand prix", "race", "match", "league", "cup", "tournament", "olympic",
    ),
    "diplomacy": (
        "会谈", "谈判", "峰会", "斡旋", "大使馆", "停火协议", "talks", "negotiation", "summit", "mediation", "embassy", "deal",
    ),
}
_ICON_HINTS: dict[str, str] = {
    "conflict": "war",
    "trade": "trade",
    "energy": "energy",
}
_GENERIC_TITLE_STOPWORDS = {
    "表示", "回应", "宣布", "计划", "称", "说", "指出", "推动", "相关", "最新", "继续", "再次", "问题", "局势",
    "official", "says", "said", "plans", "plan", "latest", "amid", "after", "over", "new",
}
_HEADLINEISH_NAME_TOKENS = {
    "表示", "宣布", "警告", "回应", "确认", "威胁", "载有", "运送", "计划", "要求", "呼吁", "突破",
}
_STORYLINE_NAME_STOPWORDS = _GENERIC_TITLE_STOPWORDS | {
    "总统", "总理", "部长", "政府", "白宫", "交通部", "能源设施", "消息", "报道", "主线", "话题", "焦点",
    "战事", "局势", "危机", "冲突", "谈判", "制裁", "贸易", "选情", "政策", "能源", "航运", "赛事", "疫情", "灾情",
}
_STATE_CORRECTION_TERMS = {
    "更正", "纠正", "撤回", "否认", "澄清", "辟谣",
    "correction", "corrected", "retract", "denies", "clarifies",
}
_STATE_TURNING_POINT_TERMS = {
    "突破", "转折", "升级", "扩大", "达成协议", "停火", "批准", "裁定",
    "breakthrough", "turning point", "escalates", "expands", "deal", "ceasefire", "ruling",
}


def _normalized_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def _extract_signal_hits(text: str) -> set[str]:
    normalized = _normalized_text(text)
    hits: set[str] = set()
    for key, aliases in _GENERIC_SIGNAL_PATTERNS.items():
        for alias in aliases:
            if alias.lower() in normalized:
                hits.add(key)
                break
    return hits


def _char_ngrams(text: str, n: int = 3) -> set[str]:
    compact = re.sub(r"[^\w\u4e00-\u9fff]+", "", text.lower())
    if not compact:
        return set()
    if len(compact) <= n:
        return {compact}
    return {compact[i : i + n] for i in range(len(compact) - n + 1)}


def _title_overlap(left: str, right: str) -> float:
    left_ngrams = _char_ngrams(left)
    right_ngrams = _char_ngrams(right)
    if not left_ngrams or not right_ngrams:
        return 0.0
    union = left_ngrams | right_ngrams
    if not union:
        return 0.0
    return len(left_ngrams & right_ngrams) / len(union)


def _article_primary_topic(article: Article, fallback: str) -> str:
    return article.topics[0] if article.topics else fallback


def _cluster_primary_topic(articles: list[Article], fallback: str) -> str:
    if not articles:
        return fallback
    counts: dict[str, int] = defaultdict(int)
    for article in articles:
        topic = _article_primary_topic(article, fallback)
        counts[topic] += 1
    return max(counts, key=lambda topic: counts[topic])


def _cluster_text(cluster: ArticleCluster) -> str:
    titles = " ".join(article.title for article in cluster.articles[:3])
    return f"{cluster.topic_category} {titles}".strip()


def _cluster_centroid(cluster: ArticleCluster) -> np.ndarray | None:
    embeddings = [
        np.array(article.embedding, dtype=float)
        for article in cluster.articles
        if article.embedding is not None
    ]
    if not embeddings:
        return None
    centroid = np.mean(embeddings, axis=0)
    norm = np.linalg.norm(centroid)
    if norm == 0:
        return None
    return centroid / norm


def _cosine(left: np.ndarray | None, right: np.ndarray | None) -> float:
    if left is None or right is None:
        return 0.0
    return float(np.dot(left, right))


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9\-]+", "-", value.lower()).strip("-")
    return slug or "storyline"


def _short_name(value: str, max_chars: int) -> str:
    compact = re.sub(r"\s+", "", value).strip()
    compact = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", compact).strip()
    compact = compact[:max_chars].strip(" -:：，,、。.；;")
    return compact or "焦点话题"


def _normalize_english_term(term: str) -> str:
    normalized = term.lower()
    for suffix in ("ians", "ian", "ing", "ed", "es", "s"):
        if normalized.endswith(suffix) and len(normalized) > len(suffix) + 2:
            normalized = normalized[: -len(suffix)]
            break
    return normalized


def _title_terms(text: str) -> set[str]:
    normalized = re.sub(r"\s+", " ", text.lower()).strip()
    english_terms = {
        _normalize_english_term(term)
        for term in re.findall(r"[a-z]{4,}", normalized)
        if term not in _GENERIC_TITLE_STOPWORDS
    }
    chinese_terms: set[str] = set()
    for chunk in re.findall(r"[\u4e00-\u9fff]{2,16}", text):
        if chunk in _GENERIC_TITLE_STOPWORDS:
            continue
        for size in (2, 3):
            if len(chunk) < size:
                continue
            for idx in range(len(chunk) - size + 1):
                term = chunk[idx : idx + size]
                if term not in _GENERIC_TITLE_STOPWORDS:
                    chinese_terms.add(term)
    return english_terms | chinese_terms


def _term_overlap(left: set[str], right: set[str]) -> int:
    return len(left & right)


def _storyline_name_from_cluster(cluster: ArticleCluster, max_chars: int) -> str:
    if cluster.storyline_name:
        return _short_name(cluster.storyline_name, max_chars)
    if cluster.articles:
        return _short_name(cluster.articles[0].title, max_chars)
    return _short_name(cluster.topic_category, max_chars)


def _looks_like_headline_fragment(name: str) -> bool:
    compact = _short_name(name, max(len(name), 10))
    if not compact:
        return True
    if any(char in compact for char in ":：，,。！？!?；;“”‘’\"'《》"):
        return True
    if len(compact) >= 8 and any(token in compact for token in _HEADLINEISH_NAME_TOKENS):
        return True
    return False


def _storyline_name_terms(text: str) -> set[str]:
    terms: set[str] = set()
    for chunk in re.findall(r"[\u4e00-\u9fff]{2,12}", text):
        for size in range(2, min(4, len(chunk)) + 1):
            for idx in range(len(chunk) - size + 1):
                term = chunk[idx : idx + size]
                if term in _STORYLINE_NAME_STOPWORDS:
                    continue
                terms.add(term)
    return terms


def _storyline_signal_suffix(signals: set[str]) -> str:
    if signals & {"trade"}:
        return "贸易"
    if signals & {"election"}:
        return "选情"
    if signals & {"disaster"}:
        return "灾情"
    if signals & {"health"}:
        return "疫情"
    if signals & {"sports"}:
        return "赛事"
    return "局势"


def _fallback_signal_name(signals: set[str]) -> str:
    if signals & {"trade"}:
        return "贸易焦点"
    if signals & {"election"}:
        return "选举焦点"
    if signals & {"disaster"}:
        return "灾害焦点"
    if signals & {"health"}:
        return "健康焦点"
    if signals & {"sports"}:
        return "赛事焦点"
    return "地区局势"


def _synthesized_storyline_name(anchor_profiles: list[dict[str, object]], max_chars: int) -> str:
    if not anchor_profiles:
        return "焦点话题"
    term_counts: dict[str, int] = defaultdict(int)
    combined_signals: set[str] = set()
    required_count = 1 if len(anchor_profiles) == 1 else 2
    for profile in anchor_profiles:
        combined_signals |= set(profile["signals"])
        for term in _storyline_name_terms(str(profile["lead_title"])):
            term_counts[term] += 1

    viable_terms = [
        term
        for term, count in term_counts.items()
        if count >= required_count
    ]
    if viable_terms:
        entity = max(viable_terms, key=lambda term: (term_counts[term], len(term)))
        if entity.endswith(("局势", "战事", "冲突", "危机", "谈判", "贸易", "选情", "政策", "能源", "航运", "赛事", "疫情", "灾情")):
            return _short_name(entity, max_chars)
        return _short_name(f"{entity}{_storyline_signal_suffix(combined_signals)}", max_chars)
    return _short_name(_fallback_signal_name(combined_signals), max_chars)


def _usable_storyline_name(candidate: str | None, anchor_profiles: list[dict[str, object]], max_chars: int) -> str | None:
    normalized = _short_name(candidate or "", max_chars)
    if not normalized or _looks_like_headline_fragment(normalized):
        return None
    return normalized


def _signal_overlap(left: set[str], right: set[str]) -> int:
    return len(left & right)


def _core_signal_score(signals: set[str]) -> int:
    return len(signals & _DIRECT_SIGNAL_TYPES)


def _spillover_signal_score(signals: set[str]) -> int:
    return len(signals & _SPILLOVER_SIGNAL_TYPES)


def _is_broad_policy_profile(profile: dict[str, object]) -> bool:
    signals = set(profile["signals"])
    return bool(signals & {"regulation", "diplomacy"}) and not bool(signals & {"transport", "energy", "market"})


class StorylineStateMachine:
    """Assign explicit lifecycle state and a compact timeline for storyline clusters."""

    STATES = {"emerging", "developing", "turning_point", "correction", "stabilized", "archived"}

    def resolve_state(
        self,
        cluster: ArticleCluster,
        historical_clusters: list[Cluster],
        quality_report: ClusterQualityReport | None = None,
        report_date: date | None = None,
    ) -> str:
        text = _cluster_text(cluster)
        lowered = text.lower()
        quality_report = quality_report or cluster.quality_report
        if any(term in lowered or term in text for term in _STATE_CORRECTION_TERMS):
            return "correction"
        if quality_report and quality_report.contested_claims:
            return "turning_point"
        if any(term in lowered or term in text for term in _STATE_TURNING_POINT_TERMS):
            return "turning_point"
        related_history = self._related_history(cluster, historical_clusters)
        if related_history and quality_report and quality_report.overall_score >= 0.72 and not quality_report.flags:
            return "stabilized"
        if related_history:
            return "developing"
        return "emerging"

    def apply(
        self,
        clusters: list[ArticleCluster],
        historical_clusters: list[Cluster],
        current_date: date,
    ) -> list[ArticleCluster]:
        for cluster in clusters:
            state = self.resolve_state(cluster, historical_clusters, cluster.quality_report, current_date)
            cluster.storyline_state = state
            cluster.storyline_timeline = self.timeline_for_cluster(cluster, historical_clusters, current_date, state)
        return clusters

    def timeline_for_cluster(
        self,
        cluster: ArticleCluster,
        historical_clusters: list[Cluster],
        current_date: date,
        state: str | None = None,
    ) -> list[StorylineEvent]:
        key = cluster.storyline_key or cluster.macro_topic_key or _slugify(cluster.topic_category)
        related = self._related_history(cluster, historical_clusters)
        events: list[StorylineEvent] = []
        for historical in related[:4]:
            events.append(
                StorylineEvent(
                    storyline_key=key,
                    event_date=historical.report_date,
                    title=_short_name(historical.summary or historical.topic_category, 80),
                    state=getattr(historical, "storyline_state", "developing") or "developing",
                    summary=historical.summary,
                    cluster_id=historical.id,
                    quality_score=float(getattr(historical, "quality_score", 0.0) or 0.0),
                    event_type="history",
                )
            )
        lead_title = cluster.articles[0].title if cluster.articles else cluster.topic_category
        events.append(
            StorylineEvent(
                storyline_key=key,
                event_date=current_date.isoformat(),
                title=lead_title,
                state=state or cluster.storyline_state,
                summary=cluster.topic_category,
                cluster_id=None,
                quality_score=(cluster.quality_report.overall_score if cluster.quality_report else 0.0),
                event_type="current",
            )
        )
        events.sort(key=lambda event: event.event_date)
        return events[-5:]

    def _related_history(self, cluster: ArticleCluster, historical_clusters: list[Cluster]) -> list[Cluster]:
        key = cluster.storyline_key or cluster.macro_topic_key
        if not key:
            return []
        related = [
            historical
            for historical in historical_clusters
            if historical.storyline_key == key
        ]
        related.sort(key=lambda historical: (historical.report_date, historical.id or 0), reverse=True)
        return related


class EventClusterValidator:
    def __init__(self, cfg: Config) -> None:
        hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
        self.enabled = hot_cfg.get("enabled", False)
        self.pair_split_similarity = float(hot_cfg.get("cluster_validation_pair_similarity", 0.50))
        self.pair_split_title_overlap = float(hot_cfg.get("cluster_validation_pair_title_overlap", 0.03))
        self.outlier_avg_similarity = float(hot_cfg.get("cluster_validation_outlier_similarity", 0.58))
        self.outlier_avg_title_overlap = float(hot_cfg.get("cluster_validation_outlier_title_overlap", 0.05))

    def validate(self, clusters: list[ArticleCluster]) -> list[ArticleCluster]:
        if not self.enabled:
            return clusters

        validated: list[ArticleCluster] = []
        for cluster in clusters:
            validated.extend(self._validate_cluster(cluster))
        return validated

    def _validate_cluster(self, cluster: ArticleCluster) -> list[ArticleCluster]:
        if len(cluster.articles) <= 1:
            return [cluster]

        articles = list(cluster.articles)
        if len(articles) == 2:
            left = articles[0]
            right = articles[1]
            similarity = _cosine(
                np.array(left.embedding, dtype=float) if left.embedding is not None else None,
                np.array(right.embedding, dtype=float) if right.embedding is not None else None,
            )
            title_overlap = _title_overlap(left.title, right.title)
            if similarity < self.pair_split_similarity and title_overlap < self.pair_split_title_overlap:
                return [
                    ArticleCluster(topic_category=_article_primary_topic(left, cluster.topic_category), articles=[left]),
                    ArticleCluster(topic_category=_article_primary_topic(right, cluster.topic_category), articles=[right]),
                ]
            return [cluster]

        outliers: list[Article] = []
        retained: list[Article] = []
        for idx, article in enumerate(articles):
            sims: list[float] = []
            overlaps: list[float] = []
            left_vec = np.array(article.embedding, dtype=float) if article.embedding is not None else None
            for other_idx, other in enumerate(articles):
                if idx == other_idx:
                    continue
                right_vec = np.array(other.embedding, dtype=float) if other.embedding is not None else None
                sims.append(_cosine(left_vec, right_vec))
                overlaps.append(_title_overlap(article.title, other.title))
            avg_sim = sum(sims) / len(sims) if sims else 1.0
            avg_overlap = sum(overlaps) / len(overlaps) if overlaps else 1.0
            if avg_sim < self.outlier_avg_similarity and avg_overlap < self.outlier_avg_title_overlap:
                outliers.append(article)
            else:
                retained.append(article)

        if not outliers:
            return [cluster]

        logger.info(
            "Event cluster validator split %d outlier article(s) from cluster '%s': %s",
            len(outliers),
            cluster.topic_category,
            ", ".join(article.title for article in outliers[:3]),
        )

        validated: list[ArticleCluster] = []
        if retained:
            validated.append(
                ArticleCluster(
                    topic_category=_cluster_primary_topic(retained, cluster.topic_category),
                    articles=retained,
                )
            )
        for article in outliers:
            validated.append(
                ArticleCluster(
                    topic_category=_article_primary_topic(article, cluster.topic_category),
                    articles=[article],
                )
            )
        return validated


class StorylineResolver:
    def __init__(self, cfg: Config, summarizer, similarity_fn) -> None:
        self.cfg = cfg
        self.summarizer = summarizer
        self.similarity_fn = similarity_fn
        hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
        self.max_name_chars = int(hot_cfg.get("tab_name_max_chars", 10))
        self.max_pair_candidates = int(hot_cfg.get("max_pair_candidates", 60))
        self.blocker_similarity = float(hot_cfg.get("storyline_blocker_similarity", 0.60))
        self.blocker_title_overlap = float(hot_cfg.get("storyline_blocker_title_overlap", 0.06))
        self.edge_confidence_threshold = float(hot_cfg.get("storyline_edge_confidence_threshold", 0.56))
        self.history_similarity_threshold = float(hot_cfg.get("storyline_history_similarity_threshold", 0.48))
        self.admission_similarity = float(hot_cfg.get("storyline_admission_similarity", 0.62))
        self.admission_title_overlap = float(hot_cfg.get("storyline_admission_title_overlap", 0.06))
        self.icon_allowlist = list(hot_cfg.get("icon_allowlist", [_DEFAULT_ICON]))

    def resolve(
        self,
        clusters: list[ArticleCluster],
        historical_clusters: list[Cluster],
        current_date: date,
    ) -> list[ArticleCluster]:
        if not clusters:
            return []

        profiles = [self._build_profile(cluster, index) for index, cluster in enumerate(clusters)]
        history_matches = self._match_history(profiles, historical_clusters, current_date)
        pair_candidates = self._build_pair_candidates(profiles, history_matches)
        relation_results = self._classify_pairs(pair_candidates)
        accepted_edges = [edge for edge in relation_results if edge["relation"] != "not_related" and edge["confidence"] >= self.edge_confidence_threshold]
        components = self._build_components(len(clusters), accepted_edges)
        logger.info(
            "Storyline resolver: profiles=%d history_matches=%d pair_candidates=%d accepted_edges=%d components=%d",
            len(profiles),
            len(history_matches),
            len(pair_candidates),
            len(accepted_edges),
            len(components),
        )
        self._assign_storylines(clusters, profiles, components, accepted_edges, history_matches)
        return clusters

    def _build_profile(self, cluster: ArticleCluster, index: int) -> dict[str, object]:
        text = _cluster_text(cluster)
        lead_title = cluster.articles[0].title if cluster.articles else cluster.topic_category
        signals = _extract_signal_hits(text)
        return {
            "index": index,
            "cluster": cluster,
            "text": text,
            "lead_title": lead_title,
            "title_terms": _title_terms(lead_title),
            "signals": signals,
            "centroid": _cluster_centroid(cluster),
            "core_score": _core_signal_score(signals),
            "spillover_score": _spillover_signal_score(signals),
        }

    def _match_history(
        self,
        profiles: list[dict[str, object]],
        historical_clusters: list[Cluster],
        current_date: date,
    ) -> dict[int, dict[str, object]]:
        matches: dict[int, dict[str, object]] = {}
        for profile in profiles:
            best_score = 0.0
            best_match: dict[str, object] | None = None
            for historical in historical_clusters:
                hist_text = f"{historical.topic_category} {historical.summary}"
                hist_signals = _extract_signal_hits(hist_text)
                signal_overlap = _signal_overlap(set(profile["signals"]), hist_signals)
                title_overlap = _title_overlap(str(profile["lead_title"]), _short_name(hist_text, self.max_name_chars))
                if signal_overlap == 0 and title_overlap < self.blocker_title_overlap:
                    continue
                similarity = self.similarity_fn(str(profile["text"]), historical)
                if similarity < self.history_similarity_threshold and signal_overlap == 0:
                    continue
                recency_bonus = 0.0
                try:
                    days_old = max((current_date - date.fromisoformat(historical.report_date)).days, 0)
                    recency_bonus = max(0.0, (5 - days_old) / 5.0) * 0.08
                except ValueError:
                    pass
                score = similarity * 1.8 + signal_overlap * 0.35 + title_overlap * 0.2 + recency_bonus
                if score <= best_score:
                    continue
                best_score = score
                best_match = {
                    "storyline_key": historical.storyline_key,
                    "storyline_name": historical.storyline_name,
                    "storyline_role": historical.storyline_role,
                    "confidence": historical.storyline_confidence,
                    "score": score,
                    "summary": historical.summary,
                    "cluster_id": historical.id,
                }
            if best_match is not None:
                matches[int(profile["index"])] = best_match
        return matches

    def _build_pair_candidates(
        self,
        profiles: list[dict[str, object]],
        history_matches: dict[int, dict[str, object]],
    ) -> list[dict[str, object]]:
        pair_candidates: list[dict[str, object]] = []
        seen: set[tuple[int, int]] = set()
        for idx, left in enumerate(profiles):
            scored_neighbors: list[tuple[float, dict[str, object]]] = []
            for right in profiles[idx + 1:]:
                left_idx = int(left["index"])
                right_idx = int(right["index"])
                pair_key = (left_idx, right_idx)
                if pair_key in seen:
                    continue
                score, context = self._pair_blocker_score(left, right, history_matches)
                if score <= 0:
                    continue
                seen.add(pair_key)
                scored_neighbors.append((score, context))

            scored_neighbors.sort(key=lambda item: item[0], reverse=True)
            for score, context in scored_neighbors[:6]:
                pair_candidates.append({"score": score, **context})

        pair_candidates.sort(key=lambda item: float(item["score"]), reverse=True)
        return pair_candidates[: self.max_pair_candidates]

    def _pair_blocker_score(
        self,
        left: dict[str, object],
        right: dict[str, object],
        history_matches: dict[int, dict[str, object]],
    ) -> tuple[float, dict[str, object]]:
        similarity = _cosine(left["centroid"], right["centroid"])
        title_overlap = _title_overlap(str(left["lead_title"]), str(right["lead_title"]))
        title_term_overlap = _term_overlap(set(left["title_terms"]), set(right["title_terms"]))
        signal_overlap = _signal_overlap(set(left["signals"]), set(right["signals"]))
        left_hist = history_matches.get(int(left["index"]), {})
        right_hist = history_matches.get(int(right["index"]), {})
        shared_history = (
            left_hist.get("storyline_key")
            and right_hist.get("storyline_key")
            and left_hist.get("storyline_key") == right_hist.get("storyline_key")
        )

        current_support = 0.0
        if similarity >= self.blocker_similarity:
            current_support += 2.0 + similarity
        if title_overlap >= self.blocker_title_overlap:
            current_support += 1.4 + title_overlap
        if title_term_overlap > 0:
            current_support += min(2.0, title_term_overlap * 0.7)
        if signal_overlap > 0:
            current_support += signal_overlap * 1.1
        if int(left["core_score"]) > 0 and int(right["spillover_score"]) > 0:
            current_support += 0.8
        if int(right["core_score"]) > 0 and int(left["spillover_score"]) > 0:
            current_support += 0.8

        combined_signals = set(left["signals"]) | set(right["signals"])
        if (
            _is_broad_policy_profile(left)
            or _is_broad_policy_profile(right)
        ) and "conflict" not in combined_signals and title_term_overlap < 2 and title_overlap < self.blocker_title_overlap * 1.5 and signal_overlap < 2 and similarity < self.blocker_similarity + 0.05:
            if not (shared_history and current_support >= 1.2):
                return 0.0, {}

        score = 0.0
        if current_support <= 0 and not shared_history:
            return 0.0, {}
        score += current_support
        if shared_history and current_support >= 1.2:
            score += 1.5
        elif shared_history and title_term_overlap >= 1 and signal_overlap >= 1:
            score += 0.8

        return score, {
            "left_index": int(left["index"]),
            "right_index": int(right["index"]),
            "left_cluster": left["cluster"],
            "right_cluster": right["cluster"],
            "left_history": left_hist,
            "right_history": right_hist,
            "similarity": similarity,
            "title_overlap": title_overlap,
            "title_term_overlap": title_term_overlap,
            "signal_overlap": signal_overlap,
        }

    def _classify_pairs(self, pair_candidates: list[dict[str, object]]) -> list[dict[str, object]]:
        if not pair_candidates:
            return []
        try:
            results = self.summarizer.classify_storyline_relations(pair_candidates)
        except Exception as exc:
            logger.warning("Storyline pair classification failed, falling back to heuristics: %s", exc)
            results = []

        by_pair = {
            (int(result["left_index"]), int(result["right_index"])): result
            for result in results
            if isinstance(result.get("left_index"), int) and isinstance(result.get("right_index"), int)
        }

        normalized: list[dict[str, object]] = []
        for candidate in pair_candidates:
            pair_key = (int(candidate["left_index"]), int(candidate["right_index"]))
            result = by_pair.get(pair_key)
            if result is None:
                result = self._heuristic_relation(candidate)
            normalized.append(result)
        return normalized

    def _heuristic_relation(self, candidate: dict[str, object]) -> dict[str, object]:
        similarity = float(candidate.get("similarity", 0.0))
        title_overlap = float(candidate.get("title_overlap", 0.0))
        title_term_overlap = int(candidate.get("title_term_overlap", 0))
        signal_overlap = int(candidate.get("signal_overlap", 0))
        left = candidate["left_cluster"]
        right = candidate["right_cluster"]
        left_signals = _extract_signal_hits(_cluster_text(left))
        right_signals = _extract_signal_hits(_cluster_text(right))
        if (similarity >= 0.8 and (signal_overlap >= 1 or title_term_overlap >= 1 or title_overlap >= self.admission_title_overlap)) or (
            signal_overlap >= 2 and title_term_overlap >= 1
        ):
            relation = "same_core_storyline"
            confidence = min(0.92, 0.60 + similarity * 0.28 + signal_overlap * 0.04 + title_term_overlap * 0.03)
        elif (similarity >= 0.64 or title_overlap >= self.admission_title_overlap or title_term_overlap >= 2) and (
            signal_overlap >= 1
            or (_core_signal_score(left_signals) > 0 and _spillover_signal_score(right_signals) > 0)
            or (_core_signal_score(right_signals) > 0 and _spillover_signal_score(left_signals) > 0)
        ):
            relation = "same_direct_spillover_storyline"
            confidence = min(
                0.85,
                0.5 + similarity * 0.22 + signal_overlap * 0.03 + title_overlap * 0.15 + title_term_overlap * 0.02,
            )
        else:
            relation = "not_related"
            confidence = max(0.15, 0.45 - similarity * 0.2)
        return {
            "left_index": int(candidate["left_index"]),
            "right_index": int(candidate["right_index"]),
            "relation": relation,
            "confidence": confidence,
        }

    def _build_components(self, node_count: int, edges: list[dict[str, object]]) -> dict[int, list[int]]:
        parents = list(range(node_count))

        def find(idx: int) -> int:
            while parents[idx] != idx:
                parents[idx] = parents[parents[idx]]
                idx = parents[idx]
            return idx

        def union(left: int, right: int) -> None:
            left_root = find(left)
            right_root = find(right)
            if left_root != right_root:
                parents[right_root] = left_root

        for edge in edges:
            union(int(edge["left_index"]), int(edge["right_index"]))

        components: dict[int, list[int]] = defaultdict(list)
        for idx in range(node_count):
            components[find(idx)].append(idx)
        return components

    def _assign_storylines(
        self,
        clusters: list[ArticleCluster],
        profiles: list[dict[str, object]],
        components: dict[int, list[int]],
        edges: list[dict[str, object]],
        history_matches: dict[int, dict[str, object]],
    ) -> None:
        edge_map: dict[int, list[dict[str, object]]] = defaultdict(list)
        pair_edge_map: dict[tuple[int, int], dict[str, object]] = {}
        for edge in edges:
            edge_map[int(edge["left_index"])].append(edge)
            edge_map[int(edge["right_index"])].append(edge)
            pair_edge_map[(int(edge["left_index"]), int(edge["right_index"]))] = edge
            pair_edge_map[(int(edge["right_index"]), int(edge["left_index"]))] = edge

        storyline_counter = 1
        for component_nodes in components.values():
            if len(component_nodes) == 1:
                idx = component_nodes[0]
                self._apply_singleton_storyline(clusters[idx], profiles[idx], history_matches.get(idx))
                continue

            core_candidates = {
                idx for idx in component_nodes if self._is_core_candidate(idx, profiles[idx], edge_map)
            }
            if not core_candidates:
                for idx in component_nodes:
                    self._apply_singleton_storyline(clusters[idx], profiles[idx], history_matches.get(idx))
                continue

            anchor_groups = self._build_anchor_groups(core_candidates, profiles, edge_map, pair_edge_map, history_matches)
            if not anchor_groups:
                for idx in component_nodes:
                    self._apply_singleton_storyline(clusters[idx], profiles[idx], history_matches.get(idx))
                continue

            group_assignments: dict[int, tuple[int, str, float]] = {}
            for group_index, anchor_group in enumerate(anchor_groups):
                for idx in anchor_group:
                    group_assignments[idx] = (group_index, "core", float("inf"))
            rejected_nodes: set[int] = set()
            for idx in component_nodes:
                if idx in group_assignments:
                    continue
                admission_role = "core" if idx in core_candidates else "spillover"
                best_group_index: int | None = None
                best_support = 0.0
                for group_index, anchor_group in enumerate(anchor_groups):
                    if not self._has_anchor_edge(idx, anchor_group, pair_edge_map):
                        continue
                    admitted, support = self._member_support(
                        idx,
                        admission_role,
                        anchor_group,
                        profiles,
                        pair_edge_map,
                        history_matches,
                    )
                    if not admitted:
                        continue
                    if support > best_support:
                        best_group_index = group_index
                        best_support = support
                if best_group_index is None:
                    rejected_nodes.add(idx)
                    continue
                group_assignments[idx] = (best_group_index, admission_role, best_support)

            grouped_members: list[dict[str, set[int]]] = []
            for anchor_group in anchor_groups:
                member_nodes = set(anchor_group)
                for idx, (group_index, _role, _support) in group_assignments.items():
                    if anchor_groups[group_index] == anchor_group:
                        member_nodes.add(idx)
                if len(member_nodes) >= 2:
                    grouped_members.append({"anchors": set(anchor_group), "members": member_nodes})
                else:
                    rejected_nodes.update(member_nodes)
            grouped_members.sort(key=lambda group: (-len(group["members"]), min(group["members"])))
            dominant_anchor_labels = [
                _short_name(str(profiles[idx]["lead_title"]), self.max_name_chars)
                for idx in sorted(grouped_members[0]["anchors"])[:3]
            ] if grouped_members else []

            assigned_nodes: set[int] = set()
            for group in grouped_members:
                anchor_nodes = group["anchors"]
                eligible_nodes = group["members"]
                history_keys = [
                    match.get("storyline_key")
                    for idx in eligible_nodes
                    if (match := history_matches.get(idx)) and match.get("storyline_key")
                ]
                reuse_key = history_keys[0] if history_keys and all(key == history_keys[0] for key in history_keys) else None
                reuse_match = None
                if reuse_key:
                    reuse_match = next(
                        (
                            match
                            for idx in eligible_nodes
                            if (match := history_matches.get(idx)) and match.get("storyline_key") == reuse_key
                        ),
                        None,
                    )

                if reuse_key:
                    storyline_key = str(reuse_key)
                    storyline_name = _short_name(
                        str((reuse_match or {}).get("storyline_name") or reuse_key),
                        self.max_name_chars,
                    )
                else:
                    storyline_name = self._build_storyline_name(anchor_nodes, profiles, clusters)
                    storyline_key = f"{_slugify(storyline_name)}-{storyline_counter}"
                    storyline_counter += 1

                component_edges = [
                    edge for edge in edges
                    if int(edge["left_index"]) in eligible_nodes and int(edge["right_index"]) in eligible_nodes
                ]
                confidence = (
                    sum(float(edge["confidence"]) for edge in component_edges) / len(component_edges)
                    if component_edges else 0.6
                )
                icon_key = self._pick_icon_key(set(eligible_nodes), profiles)
                anchor_labels = [
                    _short_name(str(profiles[idx]["lead_title"]), self.max_name_chars)
                    for idx in sorted(anchor_nodes)[:3]
                ]
                for idx in sorted(eligible_nodes):
                    assigned_nodes.add(idx)
                    role = group_assignments[idx][1]
                    self._apply_storyline(
                        clusters[idx],
                        storyline_key=storyline_key,
                        storyline_name=storyline_name,
                        storyline_role=role,
                        storyline_confidence=confidence,
                        icon_key=icon_key,
                        membership_status=role,
                        anchor_labels=anchor_labels,
                    )

            for idx in component_nodes:
                if idx in rejected_nodes:
                    self._apply_excluded_to_main(
                        clusters[idx],
                        profiles[idx],
                        history_matches.get(idx),
                        dominant_anchor_labels,
                    )
                    continue
                if idx not in assigned_nodes:
                    self._apply_singleton_storyline(clusters[idx], profiles[idx], history_matches.get(idx))
                    continue

    def _is_core_candidate(
        self,
        idx: int,
        profile: dict[str, object],
        edge_map: dict[int, list[dict[str, object]]],
    ) -> bool:
        incident_edges = edge_map.get(idx, [])
        if any(edge["relation"] == "same_core_storyline" for edge in incident_edges):
            return True
        return int(profile["core_score"]) > 0 and int(profile["core_score"]) > int(profile["spillover_score"])

    def _build_anchor_groups(
        self,
        core_candidates: set[int],
        profiles: list[dict[str, object]],
        edge_map: dict[int, list[dict[str, object]]],
        pair_edge_map: dict[tuple[int, int], dict[str, object]],
        history_matches: dict[int, dict[str, object]],
    ) -> list[set[int]]:
        if not core_candidates:
            return []
        scored = []
        for idx in core_candidates:
            same_core_degree = sum(1 for edge in edge_map.get(idx, []) if edge["relation"] == "same_core_storyline")
            total_degree = len(edge_map.get(idx, []))
            history_bonus = 0.4 if history_matches.get(idx, {}).get("storyline_role") == "core" else 0.0
            score = float(profiles[idx]["core_score"]) * 2.0 + same_core_degree * 1.5 + total_degree * 0.3 + history_bonus
            scored.append((score, idx))
        scored.sort(reverse=True)
        ordered_candidates = [idx for _, idx in scored]
        groups: list[set[int]] = []
        seen: set[int] = set()
        for seed in ordered_candidates:
            if seed in seen:
                continue
            stack = [seed]
            group: set[int] = set()
            while stack:
                idx = stack.pop()
                if idx in seen:
                    continue
                seen.add(idx)
                group.add(idx)
                for neighbor in ordered_candidates:
                    if neighbor in seen or neighbor == idx:
                        continue
                    edge = pair_edge_map.get((idx, neighbor))
                    if not edge or edge["relation"] != "same_core_storyline" or float(edge["confidence"]) < self.edge_confidence_threshold:
                        continue
                    title_overlap = _title_overlap(str(profiles[idx]["lead_title"]), str(profiles[neighbor]["lead_title"]))
                    title_term_overlap = _term_overlap(set(profiles[idx]["title_terms"]), set(profiles[neighbor]["title_terms"]))
                    signal_overlap = _signal_overlap(set(profiles[idx]["signals"]), set(profiles[neighbor]["signals"]))
                    shared_history = (
                        history_matches.get(idx, {}).get("storyline_key")
                        and history_matches.get(idx, {}).get("storyline_key") == history_matches.get(neighbor, {}).get("storyline_key")
                    )
                    if (
                        title_overlap >= self.admission_title_overlap
                        or title_term_overlap >= 1
                        or signal_overlap >= 1
                        or shared_history
                    ):
                        stack.append(neighbor)
            groups.append(group)
        groups.sort(key=lambda group: (-len(group), min(group)))
        return groups

    def _has_anchor_edge(
        self,
        idx: int,
        anchor_nodes: set[int],
        pair_edge_map: dict[tuple[int, int], dict[str, object]],
    ) -> bool:
        return any((idx, anchor_idx) in pair_edge_map for anchor_idx in anchor_nodes)

    def _member_support(
        self,
        idx: int,
        admission_role: str,
        anchor_nodes: set[int],
        profiles: list[dict[str, object]],
        pair_edge_map: dict[tuple[int, int], dict[str, object]],
        history_matches: dict[int, dict[str, object]],
    ) -> tuple[bool, float]:
        profile = profiles[idx]
        if admission_role == "spillover" and int(profile["spillover_score"]) == 0:
            return False, 0.0

        best_support = 0.0
        admitted = False
        for anchor_idx in anchor_nodes:
            anchor_profile = profiles[anchor_idx]
            edge = pair_edge_map.get((idx, anchor_idx))
            if edge is None:
                continue
            title_term_overlap = _term_overlap(set(profile["title_terms"]), set(anchor_profile["title_terms"]))
            title_overlap = _title_overlap(str(profile["lead_title"]), str(anchor_profile["lead_title"]))
            similarity = _cosine(profile["centroid"], anchor_profile["centroid"])
            signal_overlap = _signal_overlap(set(profile["signals"]), set(anchor_profile["signals"]))
            current_support = 0.0
            if similarity >= self.admission_similarity:
                current_support += 1.5 + similarity
            if title_overlap >= self.admission_title_overlap:
                current_support += 0.8 + title_overlap
            if title_term_overlap >= 1:
                current_support += 0.6 + title_term_overlap * 0.2
            if signal_overlap >= 1:
                current_support += 0.5 + signal_overlap * 0.2
            if admission_role == "spillover" and edge["relation"] == "same_direct_spillover_storyline":
                current_support += 0.4
            if history_matches.get(idx, {}).get("storyline_key") and history_matches.get(idx, {}).get("storyline_key") == history_matches.get(anchor_idx, {}).get("storyline_key"):
                current_support += 0.3
            best_support = max(best_support, current_support)
            if admission_role == "core":
                if (
                    (similarity >= self.admission_similarity and (title_term_overlap >= 1 or signal_overlap >= 1 or title_overlap >= self.admission_title_overlap))
                    or title_overlap >= self.admission_title_overlap
                    or title_term_overlap >= 1
                    or signal_overlap >= 1
                ):
                    admitted = True
                continue
            if (
                similarity >= max(0.56, self.admission_similarity - 0.04)
                or title_overlap >= self.admission_title_overlap
                or title_term_overlap >= 1
            ) and (signal_overlap >= 1 or edge["relation"] == "same_direct_spillover_storyline"):
                admitted = True

        if admission_role == "core":
            return admitted, best_support
        return admitted or best_support >= 1.25, best_support

    def _passes_member_admission(
        self,
        idx: int,
        admission_role: str,
        anchor_nodes: set[int],
        profiles: list[dict[str, object]],
        pair_edge_map: dict[tuple[int, int], dict[str, object]],
        history_matches: dict[int, dict[str, object]],
    ) -> bool:
        admitted, _ = self._member_support(
            idx,
            admission_role,
            anchor_nodes,
            profiles,
            pair_edge_map,
            history_matches,
        )
        return admitted

    def _build_storyline_name(
        self,
        anchor_nodes: set[int],
        profiles: list[dict[str, object]],
        clusters: list[ArticleCluster],
    ) -> str:
        anchor_clusters = [clusters[idx] for idx in sorted(anchor_nodes)]
        anchor_profiles = [profiles[idx] for idx in sorted(anchor_nodes)]
        name_storyline = getattr(self.summarizer, "name_storyline", None)
        if callable(name_storyline) and len(anchor_clusters) > 1:
            generated = name_storyline(anchor_clusters)
            usable = _usable_storyline_name(generated, anchor_profiles, self.max_name_chars)
            if usable:
                return usable
        deterministic = _usable_storyline_name(
            _storyline_name_from_cluster(anchor_clusters[0], self.max_name_chars),
            anchor_profiles,
            self.max_name_chars,
        )
        if deterministic:
            return deterministic
        return _synthesized_storyline_name(anchor_profiles, self.max_name_chars)

    def _apply_storyline(
        self,
        cluster: ArticleCluster,
        storyline_key: str,
        storyline_name: str,
        storyline_role: str,
        storyline_confidence: float,
        icon_key: str,
        membership_status: str,
        anchor_labels: list[str],
    ) -> None:
        cluster.storyline_key = storyline_key
        cluster.storyline_name = storyline_name
        cluster.storyline_role = storyline_role
        cluster.storyline_confidence = storyline_confidence
        cluster.storyline_membership_status = membership_status
        cluster.storyline_anchor_labels = list(anchor_labels)
        # Compatibility with current renderer/report contract
        cluster.macro_topic_key = storyline_key
        cluster.macro_topic_name = storyline_name
        cluster.macro_topic_icon_key = icon_key

    def _apply_singleton_storyline(
        self,
        cluster: ArticleCluster,
        profile: dict[str, object],
        history_match: dict[str, object] | None,
    ) -> None:
        storyline_name = _storyline_name_from_cluster(cluster, self.max_name_chars)
        storyline_key = history_match.get("storyline_key") if history_match and history_match.get("storyline_key") else f"single-{int(profile['index']) + 1}"
        icon_key = self._pick_icon_key({0}, [profile])
        self._apply_storyline(
            cluster,
            storyline_key=str(storyline_key),
            storyline_name=_short_name(str(history_match.get("storyline_name") or storyline_name), self.max_name_chars) if history_match else storyline_name,
            storyline_role="none",
            storyline_confidence=float(history_match.get("score", 0.0)) if history_match else 0.0,
            icon_key=icon_key,
            membership_status="none",
            anchor_labels=[],
        )

    def _apply_excluded_to_main(
        self,
        cluster: ArticleCluster,
        profile: dict[str, object],
        history_match: dict[str, object] | None,
        anchor_labels: list[str],
    ) -> None:
        self._apply_singleton_storyline(cluster, profile, history_match)
        cluster.storyline_membership_status = "excluded_to_main"
        cluster.storyline_anchor_labels = list(anchor_labels)

    def _pick_icon_key(self, node_indexes: set[int], profiles: list[dict[str, object]]) -> str:
        scores: dict[str, int] = defaultdict(int)
        for idx in node_indexes:
            profile = profiles[idx]
            for signal in set(profile["signals"]):
                icon = _ICON_HINTS.get(signal, _DEFAULT_ICON)
                scores[icon] += 1
        if not scores:
            return self.icon_allowlist[0] if self.icon_allowlist else _DEFAULT_ICON
        best_icon = max(scores, key=lambda key: scores[key])
        if best_icon not in self.icon_allowlist:
            return self.icon_allowlist[0] if self.icon_allowlist else _DEFAULT_ICON
        return best_icon
