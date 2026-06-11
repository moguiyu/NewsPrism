"""Editorial display planning for NewsPrism reports.

Layer: service (imports config and types; never imports runtime or repo).
"""
from __future__ import annotations

import logging
import re
import urllib.parse
from collections import defaultdict

from newsprism.config import Config
from newsprism.types import ArticleCluster, ClusterSummary, EditorialReportPlan

logger = logging.getLogger(__name__)


def _extract_markdown_headline(summary_text: str) -> str:
    for line in summary_text.splitlines():
        match = re.match(r"\*\*(.+?)\*\*", line.strip())
        if match:
            return match.group(1)
    return ""


def _body_only_text(summary_text: str) -> str:
    result: list[str] = []
    headline_consumed = False
    for line in summary_text.splitlines():
        stripped = line.strip()
        if not headline_consumed and re.match(r"\*\*(.+?)\*\*", stripped):
            headline_consumed = True
            continue
        if re.match(r"[•·\-\*]\s*【.+?】", stripped):
            continue
        result.append(line)
    while result and not result[0].strip():
        result.pop(0)
    while result and not result[-1].strip():
        result.pop()
    return "\n".join(result)


class EditorialPlanner:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def base_plan(self, kept_summaries: list[ClusterSummary]) -> EditorialReportPlan:
        hot_topics, focus_storylines, regular_summaries = select_hot_topic_families(kept_summaries, self.cfg)
        return EditorialReportPlan(
            hot_topics=hot_topics,
            focus_storylines=focus_storylines,
            regular_summaries=regular_summaries,
            positive_summaries=[],
        )

    def finalize(
        self,
        base_plan: EditorialReportPlan,
        positive_summaries: list[ClusterSummary] | None = None,
    ) -> EditorialReportPlan:
        filtered_positive_summaries = filter_local_positive_summaries(positive_summaries or [], self.cfg)
        regular_summaries = split_positive_energy_lane(base_plan.regular_summaries, filtered_positive_summaries)
        hot_topics, focus_storylines, regular_summaries, filtered_positive_summaries = resolve_display_duplicates(
            base_plan.hot_topics,
            base_plan.focus_storylines,
            regular_summaries,
            filtered_positive_summaries,
        )
        return EditorialReportPlan(
            hot_topics=hot_topics,
            focus_storylines=focus_storylines,
            regular_summaries=regular_summaries,
            positive_summaries=filtered_positive_summaries,
        )

    def plan(
        self,
        kept_summaries: list[ClusterSummary],
        local_positive_summaries: list[ClusterSummary] | None = None,
    ) -> EditorialReportPlan:
        return self.finalize(self.base_plan(kept_summaries), local_positive_summaries)


_DEFAULT_HOT_TOPIC_ICON_KEY = "globe"


_DEFAULT_HOT_TOPIC_ALLOWLIST = ["globe", "war", "trade", "chip", "ai", "energy"]


_HEADLINEISH_NAME_TOKENS = (
    "表示",
    "宣布",
    "警告",
    "回应",
    "确认",
    "威胁",
    "载有",
    "运送",
    "计划",
    "要求",
    "呼吁",
    "称",
    "说",
    "突破",
)


_EVENT_ENTITY_ALIASES = {
    "美伊": "美国 伊朗",
    "中美": "中国 美国",
    "美中": "美国 中国",
    "对伊": "伊朗",
    "访华": "中国 访问",
    "赴华": "中国 访问",
    "霍尔木兹": "霍尔木兹海峡",
}


_EVENT_ENTITY_TERMS = (
    "特朗普",
    "习近平",
    "拜登",
    "普京",
    "金正恩",
    "柯文哲",
    "扎克伯格",
    "黄仁勋",
    "马斯克",
    "美国",
    "中国",
    "伊朗",
    "以色列",
    "韩国",
    "朝鲜",
    "日本",
    "俄罗斯",
    "乌克兰",
    "欧盟",
    "墨西哥",
    "台湾",
    "菲律宾",
    "孟加拉国",
    "尼泊尔",
    "印度",
    "英国",
    "荷兰",
    "法国",
    "德国",
    "澳大利亚",
    "霍尔木兹海峡",
    "联合国",
    "白宫",
    "五角大楼",
    "OpenAI",
    "Anthropic",
    "Meta",
    "Google",
    "苹果",
    "小米",
    "三星",
    "微软",
    "英伟达",
    "通用汽车",
    "NASA",
    "SpaceX",
    "whale",
    "calf",
)


_EVENT_ACTION_ALIASES: dict[str, tuple[str, ...]] = {
    "visit": ("访华", "赴华", "访问", "出访", "visit", "trip"),
    "talks": ("会晤", "会谈", "谈判", "停战", "协议", "deal", "talk", "talks", "negotiation"),
    "war": ("战争", "军事行动", "打击", "空袭", "袭击", "冲突", "战事", "导弹", "war", "strike", "attack", "conflict"),
    "deadline": ("期限", "最后期限", "推迟", "延期", "delay", "deadline", "postpone"),
    "investment": ("投资", "收购", "融资", "invest", "investment", "acquire", "acquisition"),
    "launch": ("发布", "推出", "上线", "launch", "release", "unveil"),
    "project_shift": ("取消", "暂停", "转向", "建设", "cancel", "cancels", "pause", "pauses", "back", "backs", "shift"),
    "appointment": ("任命", "就任", "当选", "appoint", "appointment", "sworn", "elected"),
    "ruling": ("裁定", "判决", "法院", "罚款", "ruling", "court", "verdict", "sentence"),
    "sanction": ("制裁", "关税", "出口管制", "禁令", "sanction", "tariff", "ban"),
    "accident": ("事故", "坠毁", "爆炸", "遇难", "crash", "accident", "explosion"),
    "rescue": ("救援", "获救", "营救", "rescue", "rescues", "rescued", "rescuing"),
    "poll": ("民调", "调查显示", "poll", "survey"),
}


_EVENT_TIME_PATTERNS = (
    re.compile(r"\d{1,2}月(?:\d{1,2}日|上旬|中旬|下旬)?"),
    re.compile(r"\d{4}年"),
    re.compile(r"\d{1,2}\s*(?:to|-|至)\s*\d{1,2}\s*days?", re.IGNORECASE),
    re.compile(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}\b", re.IGNORECASE),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
)


_EVENT_TOKEN_STOPWORDS = {
    "新闻",
    "报道",
    "最新",
    "表示",
    "宣布",
    "指出",
    "回应",
    "计划",
    "相关",
    "official",
    "says",
    "said",
    "latest",
    "news",
    "the",
    "lead",
    "wall",
    "street",
    "may",
    "january",
    "february",
    "march",
    "april",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
}


_POSITIVE_ENERGY_FINAL_BLOCKERS = (
    "ai-generated",
    "ineligible",
    "eligible",
    "eligibility",
    "oscar",
    "oscars",
    "academy",
    "rule",
    "rules",
    "policy",
    "regulation",
    "regulatory",
    "antitrust",
    "lawsuit",
    "sanction",
    "tariff",
    "trade",
    "market",
    "shares",
    "stock",
    "downed",
    "helicopter",
    "hormuz",
    "strait of hormuz",
    "military",
    "army",
    "armed forces",
    "navy",
    "missile",
    "troop",
    "war",
    "attack",
    "death",
    "injury",
    "crime",
    "crisis",
    "disaster",
    "新规",
    "规则",
    "资格",
    "无缘",
    "政策",
    "监管",
    "反垄断",
    "诉讼",
    "制裁",
    "关税",
    "贸易",
    "市场",
    "股价",
    "军事",
    "军方",
    "军队",
    "武装部队",
    "海军",
    "导弹",
    "霍尔木兹",
    "海峡",
    "直升机",
    "坠毁",
    "击落",
    "战争",
    "袭击",
    "死亡",
    "受伤",
    "犯罪",
    "危机",
    "灾难",
)


def _normalize_storyline_name(name: str | None, summary: ClusterSummary | None, max_chars: int) -> str:
    candidate = re.sub(r"\s+", "", (name or "").strip())
    candidate = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", candidate).strip()
    candidate = candidate[:max_chars].strip(" -:：，,、。.；;")
    if candidate:
        return candidate
    if summary is not None:
        headline = _extract_markdown_headline(summary.summary) or summary.cluster.topic_category or "全球焦点"
        headline = re.sub(r"\s+", "", headline)
        headline = headline[:max_chars].strip(" -:：，,、。.；;")
        if headline:
            return headline
    return "全球焦点"


def _normalized_storyline_candidate(name: str | None, max_chars: int) -> str:
    candidate = re.sub(r"\s+", "", (name or "").strip())
    candidate = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", candidate).strip()
    return candidate[:max_chars].strip(" -:：，,、。.；;")


def _looks_like_headline_fragment(name: str) -> bool:
    if not name:
        return True
    if any(char in name for char in ":：，,。！？!?；;“”‘’\"'《》"):
        return True
    if len(name) >= 8 and any(token in name for token in _HEADLINEISH_NAME_TOKENS):
        return True
    return False


def _best_storyline_label(candidates: list[str | None], max_chars: int) -> str | None:
    counts: dict[str, int] = defaultdict(int)
    originals: dict[str, str] = {}
    for candidate in candidates:
        normalized = _normalized_storyline_candidate(candidate, max_chars)
        if not normalized or _looks_like_headline_fragment(normalized):
            continue
        counts[normalized] += 1
        originals.setdefault(normalized, normalized)
    if not counts:
        return None
    best = max(counts, key=lambda key: (counts[key], -len(key)))
    return originals[best]


def _normalize_icon_key(icon_key: str | None, allowlist: list[str]) -> str:
    if icon_key in allowlist:
        return icon_key
    return allowlist[0] if allowlist else _DEFAULT_HOT_TOPIC_ICON_KEY


def _storyline_group_key(cluster: ArticleCluster, index: int) -> str:
    return cluster.storyline_key or cluster.macro_topic_key or f"single-{index + 1}"


def _reset_hot_topic_metadata(clusters: list[ArticleCluster]) -> None:
    for index, cluster in enumerate(clusters, 1):
        cluster.is_hot_topic = False
        cluster.macro_topic_member_count = 0
        if not cluster.storyline_key:
            cluster.storyline_key = f"single-{index}"
        if not cluster.storyline_name:
            cluster.storyline_name = cluster.macro_topic_name or cluster.topic_category
        if not cluster.storyline_membership_status:
            cluster.storyline_membership_status = "none"
        if cluster.storyline_anchor_labels is None:
            cluster.storyline_anchor_labels = []
        if not cluster.macro_topic_key:
            cluster.macro_topic_key = cluster.storyline_key
        if not cluster.macro_topic_name:
            cluster.macro_topic_name = cluster.storyline_name
        if not cluster.macro_topic_icon_key:
            cluster.macro_topic_icon_key = _DEFAULT_HOT_TOPIC_ICON_KEY


def _is_storyline_hot_member(role: str | None, membership_status: str | None) -> bool:
    if role not in {"core", "spillover"}:
        return False
    if membership_status == "excluded_to_main":
        return False
    if membership_status in {"core", "spillover"}:
        return True
    return membership_status in {None, "", "none"}


def _cluster_is_hot_member(cluster: ArticleCluster) -> bool:
    return _is_storyline_hot_member(
        getattr(cluster, "storyline_role", "none"),
        getattr(cluster, "storyline_membership_status", "none"),
    )


def _summary_is_hot_member(summary: ClusterSummary) -> bool:
    return _is_storyline_hot_member(
        summary.storyline_role or getattr(summary.cluster, "storyline_role", "none"),
        summary.storyline_membership_status or getattr(summary.cluster, "storyline_membership_status", "none"),
    )


def _summary_valid_perspective_count(summary: ClusterSummary) -> int:
    if summary.grouped_perspectives:
        return len(summary.grouped_perspectives)
    if summary.perspectives:
        return len(summary.perspectives)
    return 0


def _summary_organic_source_count(summary: ClusterSummary) -> int:
    return len({article.source_name for article in summary.cluster.articles if not article.is_searched})


def _summary_region_count(summary: ClusterSummary) -> int:
    regions = {
        article.origin_region or getattr(article, "search_region", None)
        for article in summary.cluster.articles
        if article.origin_region or getattr(article, "search_region", None)
    }
    return len({region for region in regions if region})


def _selection_score(summary: ClusterSummary) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    perspective_count = _summary_valid_perspective_count(summary)
    organic_source_count = _summary_organic_source_count(summary)
    region_count = _summary_region_count(summary)

    if perspective_count:
        value = min(perspective_count, 4) * 2.0
        score += value
        reasons.append(f"perspectives:+{value:.1f}")
    if organic_source_count:
        value = min(organic_source_count, 5) * 1.2
        score += value
        reasons.append(f"organic_sources:+{value:.1f}")
    if region_count:
        value = min(region_count, 4) * 0.8
        score += value
        reasons.append(f"regions:+{value:.1f}")

    freshness_state = getattr(summary, "freshness_state", "new")
    if freshness_state == "new":
        score += 2.0
        reasons.append("freshness:+2.0")
    elif freshness_state == "developing":
        score += 0.8
        reasons.append("developing:+0.8")
    elif freshness_state == "stale":
        score -= 10.0
        reasons.append("stale:-10.0")

    broad_topic = summary.cluster.topic_category.lower()
    if any(token in broad_topic for token in ("ai", "tech", "chip", "smartphone", "science", "finance", "space")):
        score += 1.0
        reasons.append("topic_balance:+1.0")

    searched_articles = [article for article in summary.cluster.articles if article.is_searched]
    weak_search = [
        article
        for article in searched_articles
        if getattr(article, "search_acceptance_status", "accepted") != "accepted"
        or getattr(article, "result_freshness_state", "fresh") not in {"fresh", "unknown_official_allowed"}
    ]
    if weak_search:
        penalty = min(len(weak_search), 3) * 1.5
        score -= penalty
        reasons.append(f"weak_search:-{penalty:.1f}")

    if summary.cluster.is_multi_source and perspective_count <= 1 and len(summary.cluster.sources) >= 3:
        score -= 1.5
        reasons.append("same_angle_pileup:-1.5")

    short_articles = [article for article in summary.cluster.articles if len(article.content.strip()) < 80]
    if short_articles and len(short_articles) == len(summary.cluster.articles):
        score -= 1.0
        reasons.append("low_content:-1.0")

    summary.selection_score = round(score, 4)  # type: ignore[attr-defined]
    summary.selection_reasons = reasons  # type: ignore[attr-defined]
    return score, reasons


def _broad_topic_bucket(summary: ClusterSummary) -> str:
    topic = summary.cluster.topic_category
    if any(token in topic for token in ("AI", "Tech", "Smartphones", "Chips", "Space", "Robotics", "Science", "Finance")):
        return "non_geopolitics"
    if topic in {"World News", "Geopolitics", "Regions"} or "Geopolitics" in topic:
        return "geopolitics"
    return "other"


def _rank_main_summaries(summaries: list[ClusterSummary], limit: int) -> list[ClusterSummary]:
    scored = [(index, summary, _selection_score(summary)[0]) for index, summary in enumerate(summaries)]
    scored.sort(key=lambda item: (-item[2], item[0]))
    ranked = [summary for _index, summary, _score in scored]
    if limit <= 0:
        return []
    selected = ranked[:limit]
    if limit >= 5 and selected:
        non_geo_selected = [summary for summary in selected if _broad_topic_bucket(summary) == "non_geopolitics"]
        geo_selected = [summary for summary in selected if _broad_topic_bucket(summary) == "geopolitics"]
        if not non_geo_selected and len(geo_selected) / len(selected) >= 0.75:
            reserve = next(
                (
                    summary
                    for summary in ranked[limit:]
                    if _broad_topic_bucket(summary) == "non_geopolitics"
                    and getattr(summary, "selection_score", 0.0) >= 4.0
                ),
                None,
            )
            if reserve is not None:
                replacement_index = max(
                    range(len(selected)),
                    key=lambda idx: (
                        _broad_topic_bucket(selected[idx]) == "geopolitics",
                        -float(getattr(selected[idx], "selection_score", 0.0)),
                    ),
                )
                selected[replacement_index] = reserve
    return selected


def _normalized_anchor_label(label: str) -> str:
    return re.sub(r"\s+", "", label.strip().lower())


def _summary_anchor_labels(summary: ClusterSummary) -> set[str]:
    return {
        _normalized_anchor_label(label)
        for label in summary.storyline_anchor_labels
        if _normalized_anchor_label(label)
    }


def _normalized_event_text(value: str) -> str:
    return re.sub(r"\s+", "", value.lower())


def _event_core_text(summary: ClusterSummary) -> str:
    headline = _extract_markdown_headline(summary.summary) or ""
    body = _body_only_text(summary.summary)
    return f"{headline} {body}".strip()


def _event_analysis_text(summary: ClusterSummary) -> str:
    core = _event_core_text(summary)
    titles = " ".join(article.title for article in summary.cluster.articles)
    return f"{core} {titles}".strip()


def _expand_event_aliases(text: str) -> str:
    expanded = text
    for alias, replacement in _EVENT_ENTITY_ALIASES.items():
        expanded = expanded.replace(alias, f"{alias} {replacement}")
    return expanded


def _cjk_ngrams(value: str, min_n: int = 2, max_n: int = 4) -> set[str]:
    terms: set[str] = set()
    for chunk in re.findall(r"[\u4e00-\u9fff]{2,24}", value):
        for size in range(min_n, min(max_n, len(chunk)) + 1):
            for index in range(len(chunk) - size + 1):
                term = chunk[index : index + size]
                if term not in _EVENT_TOKEN_STOPWORDS:
                    terms.add(term)
    return terms


def _event_token_set(value: str) -> set[str]:
    expanded = _expand_event_aliases(value)
    normalized = _normalized_event_text(expanded)
    english = {
        term
        for term in re.findall(r"[a-z][a-z0-9]{2,}", normalized)
        if term not in _EVENT_TOKEN_STOPWORDS
    }
    return english | _cjk_ngrams(expanded)


def _event_entities(value: str) -> set[str]:
    expanded = _expand_event_aliases(value)
    entities = {term for term in _EVENT_ENTITY_TERMS if re.search(re.escape(term), expanded, re.IGNORECASE)}
    for term in re.findall(r"\b[A-Z][A-Za-z0-9&.-]{2,}\b", expanded):
        if term.lower() not in _EVENT_TOKEN_STOPWORDS:
            entities.add(term.lower())
    return entities


def _event_actions(value: str) -> set[str]:
    expanded = _expand_event_aliases(value).lower()
    actions: set[str] = set()
    for key, aliases in _EVENT_ACTION_ALIASES.items():
        if any(alias.lower() in expanded for alias in aliases):
            actions.add(key)
    return actions


def _event_time_anchors(value: str) -> set[str]:
    anchors: set[str] = set()
    for pattern in _EVENT_TIME_PATTERNS:
        anchors.update(match.group(0).lower().replace(" ", "") for match in pattern.finditer(value))
    if "数周" in value or re.search(r"\bweeks?\b", value, re.IGNORECASE):
        anchors.add("weeks")
    if "中旬" in value:
        anchors.add("mid-month")
    return anchors


def _event_context_anchors(value: str) -> set[str]:
    expanded = _expand_event_aliases(value).lower()
    anchors: set[str] = set()
    if ("伊朗" in expanded or "iran" in expanded) and any(
        token in expanded for token in ("战争", "军事行动", "打击", "霍尔木兹", "war", "strike", "hormuz")
    ):
        anchors.add("iran-war")
    if ("特朗普" in expanded or "trump" in expanded) and ("中国" in expanded or "china" in expanded) and any(
        token in expanded for token in ("访华", "访问", "会晤", "visit", "trip", "talk")
    ):
        anchors.add("trump-china-visit")
    if ("nasa" in expanded or "NASA" in value) and ("月球" in expanded or "moon" in expanded):
        anchors.add("nasa-moon")
    return anchors


def _event_signature(summary: ClusterSummary) -> dict[str, list[str]]:
    text = _event_analysis_text(summary)
    signature = {
        "entities": sorted(_event_entities(text)),
        "actions": sorted(_event_actions(text)),
        "times": sorted(_event_time_anchors(text)),
        "contexts": sorted(_event_context_anchors(text)),
    }
    summary.event_signature = signature  # type: ignore[attr-defined]
    return signature


def _summary_event_signature(summary: ClusterSummary) -> dict[str, list[str]]:
    existing = getattr(summary, "event_signature", None)
    if isinstance(existing, dict):
        return existing
    return _event_signature(summary)


def _event_token_overlap_ratio(left: ClusterSummary, right: ClusterSummary) -> float:
    left_tokens = _event_token_set(_event_analysis_text(left))
    right_tokens = _event_token_set(_event_analysis_text(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _event_core_token_overlap_ratio(left: ClusterSummary, right: ClusterSummary) -> float:
    left_tokens = _event_token_set(_event_core_text(left))
    right_tokens = _event_token_set(_event_core_text(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _signature_from_frozen(
    summary: ClusterSummary,
    frozen_signatures: dict[int, dict[str, list[str]]] | None,
) -> dict[str, list[str]]:
    if frozen_signatures is not None:
        frozen = frozen_signatures.get(_summary_display_identity(summary))
        if frozen is not None:
            return frozen
    return _summary_event_signature(summary)


def _strong_duplicate_anchor(
    left: ClusterSummary,
    right: ClusterSummary,
    left_sig: dict[str, list[str]],
    right_sig: dict[str, list[str]],
) -> tuple[bool, str, float]:
    if _article_title_overlap(left, right) >= 1 and _source_overlap(left, right) >= 1:
        return True, "shared_article_title", 0.9

    shared_entities = set(left_sig["entities"]) & set(right_sig["entities"])
    shared_actions = set(left_sig["actions"]) & set(right_sig["actions"])
    shared_times = set(left_sig["times"]) & set(right_sig["times"])
    shared_contexts = set(left_sig["contexts"]) & set(right_sig["contexts"])
    core_overlap = _event_core_token_overlap_ratio(left, right)
    broad_overlap = _event_token_overlap_ratio(left, right)

    if _summary_anchor_labels(left) & _summary_anchor_labels(right) and core_overlap >= 0.34:
        return True, "shared_anchor_label", 0.86
    if shared_contexts == {"nasa-moon"} and shared_entities and shared_actions:
        return True, "shared_context:nasa-moon", 0.84
    if len(shared_entities) >= 3 and shared_actions and (shared_times or core_overlap >= 0.2):
        return True, "entity_action_core_overlap", 0.82
    if len(shared_entities) >= 2 and shared_actions and core_overlap >= 0.26:
        return True, "entity_action_core_overlap", 0.8
    if shared_contexts and len(shared_entities) >= 2 and shared_actions and core_overlap >= 0.24:
        return True, f"shared_context:{','.join(sorted(shared_contexts))}", 0.78
    if core_overlap >= 0.38 and shared_entities:
        return True, "high_core_text_similarity", 0.78
    if "rescue" in shared_actions and len(shared_entities) >= 2:
        return True, "rescue_entity_overlap", 0.76
    if broad_overlap >= 0.48 and len(shared_entities) >= 2 and shared_actions:
        return True, "high_text_similarity_with_entities", 0.76
    return False, "", max(0.0, min(0.74, max(core_overlap, broad_overlap)))


def _event_signature_duplicate(
    left: ClusterSummary,
    right: ClusterSummary,
    frozen_signatures: dict[int, dict[str, list[str]]] | None = None,
) -> tuple[bool, str, float]:
    left_sig = _signature_from_frozen(left, frozen_signatures)
    right_sig = _signature_from_frozen(right, frozen_signatures)
    shared_entities = set(left_sig["entities"]) & set(right_sig["entities"])
    shared_actions = set(left_sig["actions"]) & set(right_sig["actions"])
    shared_times = set(left_sig["times"]) & set(right_sig["times"])
    overlap_ratio = _event_token_overlap_ratio(left, right)

    anchored, reason, confidence = _strong_duplicate_anchor(left, right, left_sig, right_sig)
    if anchored:
        return True, reason, confidence
    if len(shared_entities) >= 3 and shared_actions and shared_times and overlap_ratio >= 0.3:
        return True, "entity_action_time_overlap", 0.82
    if overlap_ratio >= 0.46 and len(shared_entities) >= 2 and shared_actions:
        return True, "high_text_similarity_with_entities", 0.78
    return False, "", max(0.0, min(0.74, overlap_ratio))


def _article_title_overlap(left: ClusterSummary, right: ClusterSummary) -> int:
    left_titles = {
        _normalized_event_text(article.title)
        for article in left.cluster.articles
    }
    right_titles = {
        _normalized_event_text(article.title)
        for article in right.cluster.articles
    }
    return len(left_titles & right_titles)


def _source_overlap(left: ClusterSummary, right: ClusterSummary) -> int:
    return len(set(left.cluster.sources) & set(right.cluster.sources))


def _normalized_perspective_set(summary: ClusterSummary) -> set[str]:
    return {
        re.sub(r"\s+", " ", text).strip()
        for text in summary.perspectives.values()
        if text.strip()
    }


def _same_event_candidate(left: ClusterSummary, right: ClusterSummary) -> bool:
    signature_match, _reason, _confidence = _event_signature_duplicate(left, right)
    if signature_match:
        return True
    if _summary_anchor_labels(left) & _summary_anchor_labels(right):
        return True
    shared_storyline = (
        bool(left.storyline_key)
        and bool(right.storyline_key)
        and left.storyline_key == right.storyline_key
    )
    if _article_title_overlap(left, right) >= 1 and _source_overlap(left, right) >= 1:
        return True
    left_tokens = _event_token_set(_event_analysis_text(left))
    right_tokens = _event_token_set(_event_analysis_text(right))
    if not left_tokens or not right_tokens:
        return False
    token_overlap = len(left_tokens & right_tokens)
    entity_overlap = len(set(_summary_event_signature(left)["entities"]) & set(_summary_event_signature(right)["entities"]))
    return token_overlap >= 3 and (_source_overlap(left, right) >= 1 or shared_storyline or entity_overlap >= 2)


def _same_angle_candidate(left: ClusterSummary, right: ClusterSummary) -> bool:
    signature_match, _reason, _confidence = _event_signature_duplicate(left, right)
    if signature_match:
        return True
    left_tokens = _event_token_set(_event_analysis_text(left))
    right_tokens = _event_token_set(_event_analysis_text(right))
    left_groups = _summary_valid_perspective_count(left)
    right_groups = _summary_valid_perspective_count(right)
    left_perspectives = _normalized_perspective_set(left)
    right_perspectives = _normalized_perspective_set(right)
    if left_perspectives and right_perspectives and left_perspectives == right_perspectives:
        return True
    if left_groups != right_groups or left_groups == 0:
        return False
    token_overlap = len(left_tokens & right_tokens)
    title_overlap = _article_title_overlap(left, right)
    source_overlap = _source_overlap(left, right)
    if title_overlap >= 1 and source_overlap >= 1 and token_overlap >= 2:
        return True
    if (
        left.storyline_key
        and right.storyline_key
        and left.storyline_key == right.storyline_key
        and title_overlap >= 1
        and token_overlap >= 3
    ):
        return True
    return False


def _prefer_summary(left: ClusterSummary, right: ClusterSummary) -> ClusterSummary:
    left_rank = (
        _summary_valid_perspective_count(left),
        _summary_organic_source_count(left),
        len(left.cluster.sources),
        1 if left.freshness_state == "new" else 0,
    )
    right_rank = (
        _summary_valid_perspective_count(right),
        _summary_organic_source_count(right),
        len(right.cluster.sources),
        1 if right.freshness_state == "new" else 0,
    )
    return left if left_rank > right_rank else right


def _merge_duplicate_summary(target: ClusterSummary, duplicate: ClusterSummary, reason: str, confidence: float) -> None:
    seen_urls = {article.url for article in target.cluster.articles}
    for article in duplicate.cluster.articles:
        if article.url in seen_urls:
            continue
        target.cluster.articles.append(article)
        seen_urls.add(article.url)
        if article.source_name not in target.cluster.sources:
            target.cluster.sources.append(article.source_name)

    for source, perspective in duplicate.perspectives.items():
        if source not in target.perspectives and perspective.strip():
            target.perspectives[source] = perspective

    grouped_by_text = {
        re.sub(r"\s+", " ", group.perspective).strip(): group
        for group in target.grouped_perspectives
        if group.perspective.strip()
    }
    for group in duplicate.grouped_perspectives:
        key = re.sub(r"\s+", " ", group.perspective).strip()
        if not key:
            continue
        existing = grouped_by_text.get(key)
        if existing is None:
            target.grouped_perspectives.append(group)
            grouped_by_text[key] = group
            continue
        for source in group.sources:
            if source not in existing.sources:
                existing.sources.append(source)

    target.duplicate_action = "merged"  # type: ignore[attr-defined]
    target.duplicate_reason = reason  # type: ignore[attr-defined]
    target.duplicate_confidence = round(confidence, 4)  # type: ignore[attr-defined]
    duplicate.duplicate_action = "suppressed"  # type: ignore[attr-defined]
    duplicate.duplicate_reason = reason  # type: ignore[attr-defined]
    duplicate.duplicate_confidence = round(confidence, 4)


def _summary_display_identity(summary: ClusterSummary) -> int:
    return id(summary)


def _flatten_display_summaries(
    hot_topics: list[dict[str, object]],
    focus_storylines: list[dict[str, object]],
    regular_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
) -> list[ClusterSummary]:
    summaries: list[ClusterSummary] = []
    for family in hot_topics + focus_storylines:
        family_summaries = family.get("summaries", [])
        if isinstance(family_summaries, list):
            summaries.extend(summary for summary in family_summaries if isinstance(summary, ClusterSummary))
    summaries.extend(positive_summaries)
    summaries.extend(regular_summaries)
    return summaries


def _filter_family_summaries(families: list[dict[str, object]], keep_ids: set[int]) -> list[dict[str, object]]:
    filtered: list[dict[str, object]] = []
    for family in families:
        family_summaries = family.get("summaries", [])
        if not isinstance(family_summaries, list):
            continue
        kept = [
            summary
            for summary in family_summaries
            if isinstance(summary, ClusterSummary) and _summary_display_identity(summary) in keep_ids
        ]
        if not kept:
            continue
        family["summaries"] = kept
        family["member_count"] = len(kept)
        filtered.append(family)
    return filtered


def resolve_display_duplicates(
    hot_topics: list[dict[str, object]],
    focus_storylines: list[dict[str, object]],
    regular_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[ClusterSummary], list[ClusterSummary]]:
    displayed = _flatten_display_summaries(hot_topics, focus_storylines, regular_summaries, positive_summaries)
    frozen_signatures: dict[int, dict[str, list[str]]] = {}
    for summary in displayed:
        frozen_signatures[_summary_display_identity(summary)] = _event_signature(summary)
        summary.duplicate_action = "kept"  # type: ignore[attr-defined]
        summary.duplicate_reason = ""  # type: ignore[attr-defined]
        summary.duplicate_confidence = 0.0  # type: ignore[attr-defined]

    suppressed_ids: set[int] = set()
    for left_index, left in enumerate(displayed):
        if _summary_display_identity(left) in suppressed_ids:
            continue
        for right in displayed[left_index + 1:]:
            if _summary_display_identity(right) in suppressed_ids:
                continue
            duplicate, reason, confidence = _event_signature_duplicate(left, right, frozen_signatures)
            if not duplicate:
                continue
            preferred = _prefer_summary(left, right)
            suppressed = right if preferred is left else left
            _merge_duplicate_summary(preferred, suppressed, reason, confidence)
            suppressed_ids.add(_summary_display_identity(suppressed))
            logger.info(
                "Display duplicate suppressed: kept='%s' suppressed='%s' reason=%s confidence=%.2f",
                _extract_markdown_headline(preferred.summary) or preferred.cluster.topic_category,
                _extract_markdown_headline(suppressed.summary) or suppressed.cluster.topic_category,
                reason,
                confidence,
            )
            if suppressed is left:
                break

    keep_ids = {_summary_display_identity(summary) for summary in displayed} - suppressed_ids
    return (
        _filter_family_summaries(hot_topics, keep_ids),
        _filter_family_summaries(focus_storylines, keep_ids),
        [summary for summary in regular_summaries if _summary_display_identity(summary) in keep_ids],
        [summary for summary in positive_summaries if _summary_display_identity(summary) in keep_ids],
    )


def _deduplicate_main_lane(summaries: list[ClusterSummary]) -> list[ClusterSummary]:
    kept: list[ClusterSummary] = []
    for candidate in summaries:
        duplicate_index: int | None = None
        for index, existing in enumerate(kept):
            if not _same_event_candidate(candidate, existing):
                continue
            if not _same_angle_candidate(candidate, existing):
                continue
            duplicate_index = index
            break
        if duplicate_index is None:
            kept.append(candidate)
            continue
        preferred = _prefer_summary(candidate, kept[duplicate_index])
        kept[duplicate_index] = preferred
    return kept


def _summary_family_link(left: ClusterSummary, right: ClusterSummary) -> bool:
    if _summary_anchor_labels(left) & _summary_anchor_labels(right):
        return True
    signature_match, _reason, _confidence = _event_signature_duplicate(left, right)
    return signature_match


def _coherent_family_component(members: list[ClusterSummary]) -> list[ClusterSummary]:
    if not members:
        return []
    if not any(_summary_anchor_labels(member) for member in members):
        return members

    components: list[list[ClusterSummary]] = []
    seen: set[int] = set()
    for start_index, start_member in enumerate(members):
        if start_index in seen:
            continue
        stack = [start_index]
        component_indexes: list[int] = []
        while stack:
            index = stack.pop()
            if index in seen:
                continue
            seen.add(index)
            component_indexes.append(index)
            member = members[index]
            for neighbor_index, neighbor in enumerate(members):
                if neighbor_index in seen:
                    continue
                if _summary_family_link(member, neighbor):
                    stack.append(neighbor_index)
        components.append([members[index] for index in sorted(component_indexes)])

    components.sort(key=lambda group: (-len(group), members.index(group[0])))
    return components[0]


def _validated_family_name(members: list[ClusterSummary], max_chars: int) -> str:
    if not members:
        return "全球焦点"
    stable_name = _best_storyline_label(
        [
            member.storyline_name
            for member in members
        ]
        + [
            member.macro_topic_name
            for member in members
        ]
        + [
            member.short_topic_name
            for member in members
        ],
        max_chars,
    )
    if stable_name:
        anchor_name = _best_storyline_label(
            [label for member in members for label in member.storyline_anchor_labels],
            max_chars,
        )
        if anchor_name and len(anchor_name) <= len(stable_name):
            return anchor_name
        return stable_name

    anchor_name = _best_storyline_label(
        [label for member in members for label in member.storyline_anchor_labels],
        max_chars,
    )
    if anchor_name:
        return anchor_name

    return _normalize_storyline_name(
        members[0].storyline_name or members[0].macro_topic_name or members[0].short_topic_name,
        members[0],
        max_chars,
    )


def _cluster_has_topic(cluster: ArticleCluster, topic: str) -> bool:
    return any(topic in article.topics for article in cluster.articles)


def _positive_energy_topic(cfg: Config) -> str:
    positive_cfg = cfg.filter.get("positive_energy_pre_filter", {}) if isinstance(cfg.filter, dict) else {}
    return str(positive_cfg.get("topic", "Positive Energy")).strip() or "Positive Energy"


def select_report_clusters(
    clusters: list[ArticleCluster],
    cfg: Config,
    _source_regions: dict[str, str] | None = None,
) -> tuple[list[ArticleCluster], list[ArticleCluster]]:
    hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
    positive_topic = _positive_energy_topic(cfg)
    positive_extra_limit = max(
        0,
        int((cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}).get("max_items", 5)),
    )
    main_limit = cfg.clustering.get("max_clusters_per_report", 20)

    _reset_hot_topic_metadata(clusters)
    if not hot_cfg.get("enabled", False):
        main_clusters = clusters[:main_limit]
        selected_ids = {id(cluster) for cluster in main_clusters}
        for cluster in clusters[main_limit:]:
            if not _cluster_has_topic(cluster, positive_topic):
                continue
            if id(cluster) in selected_ids:
                continue
            main_clusters.append(cluster)
            selected_ids.add(id(cluster))
            if len(main_clusters) >= main_limit + positive_extra_limit:
                break
        return [], main_clusters

    max_topic_tabs = hot_cfg.get("max_topic_tabs", 3)
    min_items_per_topic = hot_cfg.get("min_items_per_topic", 5)

    families: dict[str, dict[str, object]] = {}
    for index, cluster in enumerate(clusters):
        key = _storyline_group_key(cluster, index)
        info = families.setdefault(
            key,
            {
                "clusters": [],
                "first_index": index,
                "storyline_name": cluster.storyline_name or cluster.macro_topic_name or cluster.topic_category,
                "macro_topic_icon_key": cluster.macro_topic_icon_key or _DEFAULT_HOT_TOPIC_ICON_KEY,
            },
        )
        info["clusters"].append(cluster)

    ranked_families = [
        family
        for family in families.values()
        if len([cluster for cluster in family["clusters"] if _cluster_is_hot_member(cluster)]) >= min_items_per_topic
        and any(
            cluster.storyline_role == "core"
            for cluster in family["clusters"]
            if _cluster_is_hot_member(cluster)
        )
    ]
    ranked_families.sort(
        key=lambda family: (
            -len([cluster for cluster in family["clusters"] if _cluster_is_hot_member(cluster)]),
            int(family["first_index"]),
        ),
    )
    selected_families = ranked_families[:max_topic_tabs]
    hot_keys = {
        cluster.storyline_key or cluster.macro_topic_key or _storyline_group_key(cluster, index)
        for family in selected_families
        for index, cluster in enumerate(family["clusters"])
        if _cluster_is_hot_member(cluster)
    }

    hot_clusters: list[ArticleCluster] = []
    main_clusters: list[ArticleCluster] = []
    family_sizes = {
        cluster.storyline_key or cluster.macro_topic_key: len(
            [member for member in family["clusters"] if _cluster_is_hot_member(member)]
        )
        for family in selected_families
        for cluster in family["clusters"]
        if _cluster_is_hot_member(cluster)
    }
    for cluster in clusters:
        if (cluster.storyline_key in hot_keys) and _cluster_is_hot_member(cluster):
            cluster.is_hot_topic = True
            cluster.macro_topic_member_count = family_sizes.get(cluster.storyline_key, 0)
            hot_clusters.append(cluster)
            continue
        main_clusters.append(cluster)
        if len(main_clusters) >= main_limit:
            break

    selected_ids = {id(cluster) for cluster in hot_clusters + main_clusters}
    for cluster in clusters:
        if len(main_clusters) >= main_limit + positive_extra_limit:
            break
        if id(cluster) in selected_ids:
            continue
        if not _cluster_has_topic(cluster, positive_topic):
            continue
        main_clusters.append(cluster)
        selected_ids.add(id(cluster))

    return hot_clusters, main_clusters


def select_hot_topic_families(
    summaries: list[ClusterSummary],
    cfg: Config,
    _source_regions: dict[str, str] | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[ClusterSummary]]:
    hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
    max_name_chars = hot_cfg.get("tab_name_max_chars", 10)
    max_topic_tabs = hot_cfg.get("max_topic_tabs", 3)
    min_items_per_topic = hot_cfg.get("min_items_per_topic", 5)
    allowlist = list(hot_cfg.get("icon_allowlist", _DEFAULT_HOT_TOPIC_ALLOWLIST)) or list(_DEFAULT_HOT_TOPIC_ALLOWLIST)
    main_limit = cfg.clustering.get("max_clusters_per_report", 20)

    for summary in summaries:
        _selection_score(summary)
        summary.is_hot_topic = getattr(summary.cluster, "is_hot_topic", False)
        summary.storyline_key = getattr(summary.cluster, "storyline_key", None)
        summary.storyline_name = getattr(summary.cluster, "storyline_name", None)
        summary.storyline_role = getattr(summary.cluster, "storyline_role", "none")
        summary.storyline_confidence = getattr(summary.cluster, "storyline_confidence", 0.0)
        summary.storyline_membership_status = getattr(summary.cluster, "storyline_membership_status", "none")
        summary.storyline_anchor_labels = list(getattr(summary.cluster, "storyline_anchor_labels", []))
        summary.macro_topic_key = getattr(summary.cluster, "macro_topic_key", None)
        summary.macro_topic_name = getattr(summary.cluster, "macro_topic_name", None)
        summary.macro_topic_icon_key = getattr(summary.cluster, "macro_topic_icon_key", None)
        summary.macro_topic_member_count = getattr(summary.cluster, "macro_topic_member_count", 0)

    hot_grouped: dict[str, list[ClusterSummary]] = defaultdict(list)
    storyline_grouped: dict[str, list[ClusterSummary]] = defaultdict(list)
    group_order: dict[str, int] = {}
    group_names: dict[str, str] = {}
    group_icons: dict[str, str] = {}
    standalone_candidates: list[ClusterSummary] = []

    for index, summary in enumerate(summaries):
        if summary.freshness_state == "stale":
            continue
        if summary.macro_topic_key and _summary_is_hot_member(summary):
            storyline_grouped[summary.macro_topic_key].append(summary)
            group_order.setdefault(summary.macro_topic_key, index)
            group_names.setdefault(
                summary.macro_topic_key,
                _normalize_storyline_name(summary.storyline_name or summary.macro_topic_name, summary, max_name_chars),
            )
            group_icons.setdefault(
                summary.macro_topic_key,
                _normalize_icon_key(summary.macro_topic_icon_key, allowlist),
            )
            if summary.is_hot_topic:
                hot_grouped[summary.macro_topic_key].append(summary)
            continue
        standalone_candidates.append(summary)

    hot_keys = [
        key
        for key, members in hot_grouped.items()
        if len(members) >= min_items_per_topic
        and any(
            _summary_is_hot_member(member)
            and (member.storyline_role or getattr(member.cluster, "storyline_role", "none")) == "core"
            for member in members
        )
    ]
    hot_keys.sort(key=lambda key: (-len(hot_grouped[key]), group_order.get(key, 0)))
    hot_keys = hot_keys[:max_topic_tabs]

    hot_topics: list[dict[str, object]] = []
    assigned_hot_keys: set[str] = set()
    for position, key in enumerate(hot_keys, 1):
        members = hot_grouped[key]
        validated_members = _coherent_family_component(members)
        if len(validated_members) < min_items_per_topic:
            logger.info(
                "Hot topic family %s suppressed after coherence validation: %d -> %d members",
                key,
                len(members),
                len(validated_members),
            )
            continue
        assigned_hot_keys.add(key)
        hot_topics.append(
            {
                "dom_id": f"hot-topic-{position}",
                "macro_topic_key": key,
                "macro_topic_name": _validated_family_name(validated_members, max_name_chars),
                "storyline_key": key,
                "storyline_name": _validated_family_name(validated_members, max_name_chars),
                "topic_icon_key": group_icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
                "anchor_labels": list(validated_members[0].storyline_anchor_labels) if validated_members else [],
                "member_count": len(validated_members),
                "summaries": validated_members,
            }
        )

    focus_storylines: list[dict[str, object]] = []
    assigned_focus_keys: set[str] = set()
    for key, members in storyline_grouped.items():
        if key in assigned_hot_keys:
            continue
        validated_members = _coherent_family_component(members)
        if len(validated_members) < 2 or len(validated_members) >= min_items_per_topic:
            continue
        if not any(
            (member.storyline_role or getattr(member.cluster, "storyline_role", "none")) == "core"
            for member in validated_members
        ):
            continue
        for summary in validated_members:
            summary.is_hot_topic = False
            summary.cluster.is_hot_topic = False
        assigned_focus_keys.add(key)
        focus_storylines.append(
            {
                "storyline_key": key,
                "storyline_name": _validated_family_name(validated_members, max_name_chars),
                "topic_icon_key": group_icons.get(key, _DEFAULT_HOT_TOPIC_ICON_KEY),
                "member_count": len(validated_members),
                "summaries": validated_members,
            }
        )

    focus_storylines.sort(
        key=lambda family: (
            -int(family["member_count"]),
            group_order.get(str(family["storyline_key"]), 0),
        ),
    )

    main_candidates = list(standalone_candidates)
    for key, members in storyline_grouped.items():
        if key in assigned_hot_keys or key in assigned_focus_keys:
            for summary in members:
                if summary in next(
                    (
                        family["summaries"]
                        for family in hot_topics + focus_storylines
                        if str(family.get("storyline_key") or family.get("macro_topic_key")) == key
                    ),
                    [],
                ):
                    continue
                summary.is_hot_topic = False
                summary.cluster.is_hot_topic = False
                main_candidates.append(summary)
            continue
        for summary in members:
            summary.is_hot_topic = False
            summary.cluster.is_hot_topic = False
            main_candidates.append(summary)

    main_summaries = _rank_main_summaries(_deduplicate_main_lane(main_candidates), main_limit)
    return hot_topics, focus_storylines, main_summaries


def _summary_primary_domain(summary: ClusterSummary) -> str:
    for article in summary.cluster.articles:
        parsed = urllib.parse.urlparse(article.url)
        domain = parsed.netloc.lower().removeprefix("www.").strip(".")
        if domain:
            return domain
    if summary.cluster.sources:
        return summary.cluster.sources[0].lower()
    return ""


def _summary_has_positive_energy_topic(summary: ClusterSummary, cfg: Config) -> bool:
    positive_topic = _positive_energy_topic(cfg)
    return any(positive_topic in article.topics for article in summary.cluster.articles)


def _summary_ids_from_families(families: list[dict[str, object]]) -> set[int]:
    ids: set[int] = set()
    for family in families:
        summaries = family.get("summaries")
        if not isinstance(summaries, list):
            continue
        ids.update(id(summary) for summary in summaries if isinstance(summary, ClusterSummary))
    return ids


def positive_energy_classification_pool(
    kept_summaries: list[ClusterSummary],
    main_summaries: list[ClusterSummary],
    hot_topics: list[dict[str, object]],
    focus_storylines: list[dict[str, object]],
    cfg: Config,
) -> list[ClusterSummary]:
    """Return main summaries plus positive extras that were clipped by the regular feed cap."""
    positive_cfg = cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}
    if not bool(positive_cfg.get("enabled", True)):
        return []

    max_extra = max(0, int(positive_cfg.get("max_items", 5)))
    pool = list(main_summaries)
    selected_ids = {id(summary) for summary in pool}
    excluded_ids = _summary_ids_from_families(hot_topics) | _summary_ids_from_families(focus_storylines)

    for summary in kept_summaries:
        if id(summary) in selected_ids or id(summary) in excluded_ids:
            continue
        if not _summary_has_positive_energy_topic(summary, cfg):
            continue
        pool.append(summary)
        selected_ids.add(id(summary))
        if len(pool) >= len(main_summaries) + max_extra:
            break

    return pool


def _summary_blocked_for_positive_energy(summary: ClusterSummary) -> bool:
    headline = _extract_markdown_headline(summary.summary) or summary.cluster.topic_category
    text = " ".join(
        [
            summary.cluster.topic_category,
            headline,
            _body_only_text(summary.summary),
            " ".join(article.title for article in summary.cluster.articles[:4]),
        ]
    ).lower()
    return any(blocker in text for blocker in _POSITIVE_ENERGY_FINAL_BLOCKERS)


def filter_local_positive_summaries(positive_summaries: list[ClusterSummary], cfg: Config) -> list[ClusterSummary]:
    positive_cfg = cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}
    max_items = max(0, int(positive_cfg.get("max_items", 5)))
    candidates = [
        summary
        for summary in positive_summaries
        if not _summary_blocked_for_positive_energy(summary)
    ]
    _hot_topics, _focus_storylines, _regular_summaries, filtered = resolve_display_duplicates([], [], [], candidates)
    return filtered[:max_items]


def select_positive_energy_summaries(
    summaries: list[ClusterSummary],
    classifications: list[dict[str, object]],
    cfg: Config,
) -> list[ClusterSummary]:
    positive_cfg = cfg.output.get("positive_energy", {}) if isinstance(cfg.output, dict) else {}
    if not bool(positive_cfg.get("enabled", True)):
        return []

    min_items = max(1, int(positive_cfg.get("min_items", 1)))
    max_items = max(min_items, int(positive_cfg.get("max_items", 5)))
    min_confidence = float(positive_cfg.get("min_confidence", 0.78))

    by_index = {
        int(item.get("cluster_index", 0)): item
        for item in classifications
        if isinstance(item.get("cluster_index"), int)
    }
    candidates: list[tuple[float, int, ClusterSummary, dict[str, object]]] = []
    missing_classification = 0
    blocked = 0
    rejected = 0
    for index, summary in enumerate(summaries, 1):
        item = by_index.get(index)
        if item is None:
            missing_classification += 1
            continue
        if _summary_blocked_for_positive_energy(summary):
            blocked += 1
            continue
        good_fit = bool(item.get("good_fit"))
        positive = bool(item.get("positive"))
        fun = bool(item.get("fun"))
        low_conflict = bool(item.get("low_conflict"))
        confidence = max(0.0, min(1.0, float(item.get("confidence", 0.0) or 0.0)))
        if not good_fit or not low_conflict or not (positive or fun) or confidence < min_confidence:
            rejected += 1
            continue
        score = confidence + 0.25 + (0.2 if positive else 0.0) + (0.15 if fun else 0.0)
        candidates.append((score, index, summary, item))

    if len(candidates) < min_items:
        logger.info(
            "Positive energy selection: considered=%d eligible=%d selected=0 blocked=%d rejected=%d missing_classification=%d",
            len(summaries),
            len(candidates),
            blocked,
            rejected,
            missing_classification,
        )
        return []

    candidates.sort(key=lambda row: (-row[0], row[1]))
    selected: list[tuple[float, int, ClusterSummary, dict[str, object]]] = []
    selected_ids: set[int] = set()
    domains: set[str] = set()

    for candidate in candidates:
        _, index, summary, _item = candidate
        domain = _summary_primary_domain(summary)
        if domain and domain in domains:
            continue
        selected.append(candidate)
        selected_ids.add(index)
        if domain:
            domains.add(domain)
        if len(selected) >= max_items:
            break

    if len(selected) < min_items:
        for candidate in candidates:
            _, index, _summary, _item = candidate
            if index in selected_ids:
                continue
            selected.append(candidate)
            selected_ids.add(index)
            if len(selected) >= max_items or len(selected) >= min_items:
                break

    if len(selected) < min_items:
        logger.info(
            "Positive energy selection: considered=%d eligible=%d selected=0 blocked=%d rejected=%d missing_classification=%d",
            len(summaries),
            len(candidates),
            blocked,
            rejected,
            missing_classification,
        )
        return []

    selected.sort(key=lambda row: row[1])
    results: list[ClusterSummary] = []
    for score, _index, summary, item in selected[:max_items]:
        summary.positive_energy_score = round(score, 4)  # type: ignore[attr-defined]
        summary.positive_energy_reason = str(item.get("reason") or "").strip()  # type: ignore[attr-defined]
        results.append(summary)
    logger.info(
        "Positive energy selection: considered=%d eligible=%d selected=%d blocked=%d rejected=%d missing_classification=%d headlines=%s",
        len(summaries),
        len(candidates),
        len(results),
        blocked,
        rejected,
        missing_classification,
        [_extract_markdown_headline(summary.summary) or summary.cluster.topic_category for summary in results],
    )
    return results


def split_positive_energy_lane(
    main_summaries: list[ClusterSummary],
    positive_summaries: list[ClusterSummary],
) -> list[ClusterSummary]:
    positive_ids = {id(summary) for summary in positive_summaries}
    return [summary for summary in main_summaries if id(summary) not in positive_ids]
