"""Daily pipeline orchestrator.

Collect phase (every 4h):  fetch → tag → dedup → store
Publish phase (08:00 CST): cluster → summarize → render HTML → push Telegram

Layer: runtime — the only layer that imports from all others.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import shutil
import time
import urllib.parse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from newsprism.config import Config
from newsprism.repo import (
    delete_clusters_for_date,
    get_articles_by_ids,
    get_clusters_for_date,
    get_recent_clusters,
    get_unclustered_articles,
    init_db,
    insert_article,
    insert_cluster,
    insert_cluster_quality_report,
    mark_articles_clustered,
    reset_articles_clustered,
    update_article_embedding,
    upsert_storyline_state,
)
from newsprism.runtime.publisher import TelegramPublisher
from newsprism.runtime.renderer import HtmlRenderer
from newsprism.service.clusterer import Clusterer
from newsprism.service.llm_clusterer import LLMClusterer
from newsprism.service.collector import Collector
from newsprism.service.dedup import Deduplicator
from newsprism.service.editorial_planner import (
    EditorialPlanner,
    positive_energy_classification_pool,
    select_hot_topic_families,
    select_positive_energy_summaries,
    select_report_clusters,
)
from newsprism.service.feelgood_scorer import FeelgoodScorer
from newsprism.service.filter import TopicTagger
from newsprism.service.freshness import FreshnessEvaluator
from newsprism.service.quality import QualityAssessor
from newsprism.service.seeker import ActiveSeeker
from newsprism.service.storyline import EventClusterValidator, StorylineResolver, StorylineStateMachine
from newsprism.service.summarizer import Summarizer
from newsprism.types import Article, ArticleCluster, Cluster, ClusterSummary, raw_to_articles

logger = logging.getLogger(__name__)

try:
    from newsprism.repo import get_article_id_by_url
except ImportError:
    get_article_id_by_url = None

try:
    from newsprism.repo import get_report_article_ids
except ImportError:
    def get_report_article_ids(report_date: str) -> list[int]:
        article_ids: list[int] = []
        for cluster in get_clusters_for_date(report_date):
            for article_id in getattr(cluster, "article_ids", []) or []:
                if article_id not in article_ids:
                    article_ids.append(article_id)
        return article_ids

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
_HUMAN_INTEREST_OUTLIER_TERMS = (
    "重逢",
    "偶遇",
    "男孩",
    "女孩",
    "reunion",
    "reunited",
    "human interest",
)


def _cluster_storyline_headline(cluster: ArticleCluster) -> str:
    if cluster.articles:
        return cluster.articles[0].title
    return cluster.topic_category


def _storyline_group_key(cluster: ArticleCluster, index: int) -> str:
    return cluster.storyline_key or cluster.macro_topic_key or f"single-{index + 1}"


def _group_clusters_by_storyline(clusters: list[ArticleCluster]) -> dict[str, list[ArticleCluster]]:
    grouped: dict[str, list[ArticleCluster]] = defaultdict(list)
    for index, cluster in enumerate(clusters):
        grouped[_storyline_group_key(cluster, index)].append(cluster)
    return grouped


def _storyline_log_line(storyline_key: str, clusters: list[ArticleCluster]) -> str:
    storyline_name = clusters[0].storyline_name or clusters[0].macro_topic_name or clusters[0].topic_category
    roles = ",".join(sorted({cluster.storyline_role for cluster in clusters}))
    headlines = ", ".join(_cluster_storyline_headline(cluster) for cluster in clusters[:3])
    return f"{storyline_key}/{storyline_name}: {len(clusters)} role={roles} [{headlines}]"


def _log_storyline_stage(stage: str, clusters: list[ArticleCluster]) -> None:
    grouped = _group_clusters_by_storyline(clusters)
    if not grouped:
        logger.info("%s: no storyline families", stage)
        return
    lines = [
        _storyline_log_line(key, members)
        for key, members in sorted(
            grouped.items(),
            key=lambda item: min(
                getattr(cluster, "_storyline_candidate_index", index)
                for index, cluster in enumerate(item[1], 1)
            ),
        )
    ]
    logger.info("%s: %s", stage, " | ".join(lines))


def _normalized_event_text(value: str) -> str:
    return re.sub(r"\s+", "", value.lower())


def _article_event_text(article: Article) -> str:
    return f"{article.title} {article.content}".strip()


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


def _article_event_signature(article: Article) -> dict[str, list[str]]:
    text = _article_event_text(article)
    return {
        "entities": sorted(_event_entities(text)),
        "actions": sorted(_event_actions(text)),
        "times": sorted(_event_time_anchors(text)),
        "contexts": sorted(_event_context_anchors(text)),
    }


def _article_to_text_overlap_ratio(article: Article, text: str) -> float:
    left_tokens = _event_token_set(article.title)
    right_tokens = _event_token_set(text)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _article_has_strong_event_overlap(
    article: Article,
    rest_articles: list[Article],
    article_sig: dict[str, list[str]],
    rest_sig: dict[str, list[str]],
) -> bool:
    shared_entities = set(article_sig["entities"]) & set(rest_sig["entities"])
    shared_actions = set(article_sig["actions"]) & set(rest_sig["actions"])
    shared_contexts = set(article_sig["contexts"]) & set(rest_sig["contexts"])
    rest_title_text = " ".join(rest.title for rest in rest_articles)
    title_overlap = _article_to_text_overlap_ratio(article, rest_title_text)

    if len(shared_entities) >= 3:
        return True
    if shared_actions and len(shared_entities) >= 2:
        return True
    if shared_contexts == {"nasa-moon"} and shared_entities:
        return True
    if shared_contexts and shared_actions and len(shared_entities) >= 2 and title_overlap >= 0.12:
        return True
    if title_overlap >= 0.22 and shared_entities:
        return True
    if title_overlap >= 0.16 and shared_actions:
        return True
    return False


def _article_is_clear_human_interest_shift(
    article: Article,
    rest_articles: list[Article],
    article_sig: dict[str, list[str]],
    rest_sig: dict[str, list[str]],
) -> bool:
    article_text = _article_event_text(article).lower()
    if not any(term in article_text for term in _HUMAN_INTEREST_OUTLIER_TERMS):
        return False
    shared_entities = set(article_sig["entities"]) & set(rest_sig["entities"])
    shared_actions = set(article_sig["actions"]) & set(rest_sig["actions"])
    rest_title_text = " ".join(rest.title for rest in rest_articles)
    title_overlap = _article_to_text_overlap_ratio(article, rest_title_text)
    return not shared_actions and 0 < len(shared_entities) <= 2 and title_overlap < 0.05


def _cluster_signature_from_articles(articles: list[Article]) -> dict[str, list[str]]:
    text = " ".join(_article_event_text(article) for article in articles)
    return {
        "entities": sorted(_event_entities(text)),
        "actions": sorted(_event_actions(text)),
        "times": sorted(_event_time_anchors(text)),
        "contexts": sorted(_event_context_anchors(text)),
    }


def _split_disjoint_event_articles(cluster: ArticleCluster) -> list[ArticleCluster]:
    if len(cluster.articles) < 3:
        return [cluster]

    outliers: list[Article] = []
    retained: list[Article] = []
    signatures = {id(article): _article_event_signature(article) for article in cluster.articles}
    for article in cluster.articles:
        article_sig = signatures[id(article)]
        if not article_sig["entities"] and not article_sig["actions"]:
            retained.append(article)
            continue

        rest_articles = [candidate for candidate in cluster.articles if candidate is not article]
        rest_sig = _cluster_signature_from_articles(rest_articles)
        if _article_is_clear_human_interest_shift(article, rest_articles, article_sig, rest_sig):
            outliers.append(article)
        elif _article_has_strong_event_overlap(article, rest_articles, article_sig, rest_sig):
            retained.append(article)
        else:
            retained.append(article)

    if not outliers or not retained:
        return [cluster]

    logger.info(
        "Pre-summary event splitter separated %d disjoint article(s) from '%s': %s",
        len(outliers),
        cluster.topic_category,
        ", ".join(article.title for article in outliers[:3]),
    )
    split_clusters = [ArticleCluster(topic_category=cluster.topic_category, articles=retained)]
    split_clusters.extend(
        ArticleCluster(topic_category=article.topics[0] if article.topics else cluster.topic_category, articles=[article])
        for article in outliers
    )
    return split_clusters


def split_disjoint_event_articles(clusters: list[ArticleCluster]) -> list[ArticleCluster]:
    split_clusters: list[ArticleCluster] = []
    for cluster in clusters:
        split_clusters.extend(_split_disjoint_event_articles(cluster))
    return split_clusters


def _shared_article_domains(left: ClusterSummary, right: ClusterSummary) -> int:
    left_domains = {
        urllib.parse.urlparse(article.url).netloc.lower().removeprefix("www.")
        for article in left.cluster.articles
        if article.url
    }
    right_domains = {
        urllib.parse.urlparse(article.url).netloc.lower().removeprefix("www.")
        for article in right.cluster.articles
        if article.url
    }
    left_domains.discard("")
    right_domains.discard("")
    return len(left_domains & right_domains)


def _warn_on_storyline_near_miss(clusters: list[ArticleCluster], hot_keys: set[str], stage: str) -> None:
    grouped = _group_clusters_by_storyline(clusters)
    for key, members in grouped.items():
        if key in hot_keys or len(members) < 4:
            continue
        if not any(cluster.storyline_role == "core" for cluster in members):
            continue
        logger.warning(
            "%s: storyline near miss %s/%s with %d items; first headlines=%s",
            stage,
            key,
            members[0].storyline_name or members[0].macro_topic_name or members[0].topic_category,
            len(members),
            ", ".join(_cluster_storyline_headline(cluster) for cluster in members[:3]),
        )


def _warn_on_summary_storyline_near_miss(summaries: list[ClusterSummary], hot_keys: set[str], stage: str) -> None:
    grouped: dict[str, list[ArticleCluster]] = defaultdict(list)
    for summary in summaries:
        if summary.freshness_state == "stale":
            continue
        grouped[summary.storyline_key or summary.macro_topic_key or ""].append(summary.cluster)
    _warn_on_storyline_near_miss(
        [cluster for members in grouped.values() for cluster in members],
        hot_keys,
        stage,
    )


class Scheduler:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        init_db()
        self._pipeline_lock = asyncio.Lock()
        self.schedule_timezone = ZoneInfo(cfg.schedule.get("timezone", "Europe/Warsaw"))
        self.output_dir = Path(cfg.output.get("html_dir", "output"))
        self.staging_dir = self._resolve_output_path(cfg.output.get("staging_dir"), default="staging")
        self.publish_complete_flag = self.staging_dir / cfg.output.get("publish_complete_flag", ".publish_complete")
        self.push_retry_cfg = cfg.schedule.get("push_retry", {})
        self.push_retry_enabled = bool(self.push_retry_cfg.get("enabled", True))
        self.push_retry_max_attempts = int(self.push_retry_cfg.get("max_attempts", 3))
        self.push_retry_interval_minutes = int(self.push_retry_cfg.get("retry_interval_minutes", 5))
        self._apscheduler: AsyncIOScheduler | None = None

        self.collector = Collector(cfg)
        self.feelgood_scorer = FeelgoodScorer(cfg)
        self.tagger = TopicTagger(cfg)
        self.deduplicator = Deduplicator(cfg)
        self.clusterer = LLMClusterer(cfg) if cfg.use_llm_clustering else Clusterer(cfg)
        self.cluster_validator = EventClusterValidator(cfg)
        self.seeker = ActiveSeeker(cfg)
        self.summarizer = Summarizer(cfg)
        self.quality_assessor = QualityAssessor(cfg)
        self.freshness_evaluator = FreshnessEvaluator(cfg)
        self.storyline_state_machine = StorylineStateMachine()
        self.storyline_resolver = StorylineResolver(
            cfg,
            summarizer=self.summarizer,
            similarity_fn=self.freshness_evaluator.score_text_to_historical_cluster,
        )
        self.editorial_planner = EditorialPlanner(cfg)
        self.publisher = TelegramPublisher(cfg)
        self.renderer = HtmlRenderer(
            output_dir=cfg.output.get("html_dir", "output"),
            source_regions={s.name: s.region for s in cfg.sources},
        )
        self.renderer.day_navigation_cfg = cfg.output.get("day_navigation", {}) if isinstance(cfg.output, dict) else {}

    def _positive_energy_cfg(self) -> dict:
        return self.cfg.output.get("positive_energy", {}) if isinstance(self.cfg.output, dict) else {}

    def _use_feelgood_pipeline(self) -> bool:
        positive_cfg = self._positive_energy_cfg()
        return bool(positive_cfg.get("enabled", True)) and not bool(positive_cfg.get("use_llm_classifier", False))

    def _select_positive_article_summaries(self, articles: list[Article]) -> list[ClusterSummary]:
        positive_cfg = self._positive_energy_cfg()
        target_items = max(
            1,
            int(positive_cfg.get("target_items", positive_cfg.get("max_items", 5)) or 5),
        )
        candidate_min_items = int(positive_cfg.get("candidate_min_items", 20) or 20)
        if len(articles) < candidate_min_items:
            logger.warning(
                "Positive energy article pool below target: input=%d target_min=%d",
                len(articles),
                candidate_min_items,
            )
        summaries = self.feelgood_scorer.select_articles(articles, limit=target_items)
        if not summaries:
            logger.warning("Positive energy local selection empty after scoring existing articles")
        elif len(summaries) < target_items:
            logger.info(
                "Positive energy local selection below display target: selected=%d target=%d",
                len(summaries),
                target_items,
            )
        return summaries

    def _resolve_output_path(self, configured: str | None, default: str) -> Path:
        path = Path(configured or default)
        if path.is_absolute():
            return path
        root_parts = self.output_dir.parts
        if root_parts and path.parts[: len(root_parts)] == root_parts:
            return path
        return self.output_dir / path

    @property
    def _staging_subdir(self) -> Path:
        try:
            return self.staging_dir.relative_to(self.output_dir)
        except ValueError as exc:
            raise ValueError("output.staging_dir must be inside output.html_dir") from exc

    def _staging_report_dir(self, report_date: date) -> Path:
        return self.staging_dir / report_date.isoformat()

    def _write_publish_complete(self, report_date: date, total_story_count: int) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "report_date": report_date.isoformat(),
            "total_story_count": total_story_count,
            "created_at": datetime.now(tz=self.schedule_timezone).isoformat(),
        }
        self.publish_complete_flag.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        self.publish_complete_flag.chmod(0o644)

    def _read_publish_complete(self) -> dict[str, object] | None:
        if not self.publish_complete_flag.exists():
            return None
        raw = self.publish_complete_flag.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"report_date": raw}
        if not isinstance(payload, dict):
            return None
        return payload

    def _clear_publish_complete(self) -> None:
        if self.publish_complete_flag.exists():
            self.publish_complete_flag.unlink()

    def _is_publish_complete(self, report_date: date) -> bool:
        payload = self._read_publish_complete()
        if not payload:
            return False
        return payload.get("report_date") == report_date.isoformat()

    def _load_staged_render_payload(self, report_date: date) -> dict[str, object]:
        data_path = self._staging_report_dir(report_date) / "data.json"
        return json.loads(data_path.read_text(encoding="utf-8"))

    def _promote_staged_report(self, report_date: date) -> Path:
        staged_dir = self._staging_report_dir(report_date)
        final_dir = self.output_dir / report_date.isoformat()
        if not staged_dir.exists():
            raise FileNotFoundError(f"staged report directory missing: {staged_dir}")
        if final_dir.exists() or final_dir.is_symlink():
            if final_dir.is_symlink() or final_dir.is_file():
                final_dir.unlink()
            else:
                shutil.rmtree(final_dir)
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(staged_dir), str(final_dir))
        return final_dir

    def _promote_latest_symlink(self, report_date: date, total_story_count: int) -> None:
        if total_story_count <= 0:
            logger.info(
                "Push promotion: staged report has zero stories for %s — preserving existing latest symlink",
                report_date.isoformat(),
            )
            return
        latest = self.output_dir / "latest"
        if latest.is_symlink() or latest.is_file():
            latest.unlink()
        elif latest.exists():
            shutil.rmtree(latest)
        try:
            latest.symlink_to(report_date.isoformat())
        except OSError:
            logger.warning("Push promotion: failed to update latest symlink", exc_info=True)

        cfg_output = getattr(getattr(self, "cfg", None), "output", None)
        day_nav_cfg = cfg_output.get("day_navigation", {}) if isinstance(cfg_output, dict) else {}
        day_link_count = int(day_nav_cfg.get("days", 3)) if isinstance(day_nav_cfg, dict) else 3
        renderer = getattr(self, "renderer", None)
        if renderer is not None:
            renderer._promote_day_symlinks(report_date, day_link_count)

    def _schedule_push_retry(self, report_date: date, attempt: int) -> bool:
        if not self.push_retry_enabled or self._apscheduler is None:
            return False
        if attempt >= self.push_retry_max_attempts:
            return False
        retry_attempt = attempt + 1
        run_at = datetime.now(tz=self.schedule_timezone) + timedelta(minutes=self.push_retry_interval_minutes)
        job_id = f"push_retry_{report_date.isoformat()}_{retry_attempt}"
        self._apscheduler.add_job(
            partial(self.push, report_date=report_date, attempt=retry_attempt),
            DateTrigger(run_date=run_at, timezone=self.schedule_timezone),
            id=job_id,
            replace_existing=True,
        )
        logger.warning(
            "Push retry scheduled: report_date=%s attempt=%d run_at=%s",
            report_date.isoformat(),
            retry_attempt,
            run_at.isoformat(),
        )
        return True

    def _cleanup_old_staging(self) -> None:
        if not self.staging_dir.exists():
            return
        today_str = date.today().isoformat()
        for child in self.staging_dir.iterdir():
            if child == self.publish_complete_flag:
                continue
            if child.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", child.name) and child.name != today_str:
                shutil.rmtree(child, ignore_errors=True)
        payload = self._read_publish_complete()
        if payload and payload.get("report_date") != today_str:
            self._clear_publish_complete()

    # ─── PHASES ──────────────────────────────────────────────────────────────

    async def collect(self, mode: str = "full") -> None:
        """Phase 1: Collect → tag → dedup → persist."""
        phase_name = "COLLECT_DELTA" if mode == "delta" else "COLLECT"
        async with self._pipeline_lock:
            started = time.perf_counter()
            logger.info("=== %s phase started ===", phase_name)

            raw_articles = await self.collector.collect_all(mode=mode)
            db_articles = raw_to_articles(raw_articles)

            tagged = self.tagger.tag_all(db_articles)
            deduped = self.deduplicator.deduplicate(tagged)

            saved = 0
            for article in deduped:
                article_id = insert_article(article)
                if article_id is not None:
                    article.id = article_id
                    if article.embedding:
                        update_article_embedding(article_id, article.embedding)
                    saved += 1

            logger.info(
                "=== %s done: %d new articles saved (raw=%d deduped=%d duration_s=%.2f) ===",
                phase_name,
                saved,
                len(raw_articles),
                len(deduped),
                time.perf_counter() - started,
            )

    async def publish(
        self,
        report_date: date | None = None,
        articles_override: list | None = None,
        push_after_render: bool = True,
    ) -> None:
        """Phase 2: Cluster → summarize → render report, then optionally push Telegram."""
        async with self._pipeline_lock:
            started = time.perf_counter()
            phase_name = "PUBLISH_STAGE" if not push_after_render else "PUBLISH"
            logger.info("=== %s phase started ===", phase_name)
            today = report_date or date.today()

            if articles_override is None:
                existing_article_ids = get_report_article_ids(today.isoformat())
                if existing_article_ids:
                    reset_count = reset_articles_clustered(existing_article_ids)
                    deleted_count = delete_clusters_for_date(today.isoformat())
                    logger.info(
                        "Publish idempotency cleanup: reset %d articles and deleted %d existing clusters for %s",
                        reset_count,
                        deleted_count,
                        today.isoformat(),
                    )
                max_age_hours = self.cfg.clustering.get("time_window_hours", 48)
                articles = get_unclustered_articles(max_age_hours=max_age_hours)
                logger.info(
                    "Publish input: %d unclustered articles found within %d hours",
                    len(articles),
                    max_age_hours,
                )
            else:
                articles = sorted(
                    articles_override,
                    key=lambda article: article.published_at,
                    reverse=True,
                )
                logger.info(
                    "Publish input override: %d replay articles for report_date=%s",
                    len(articles),
                    today.isoformat(),
                )
            if not articles:
                logger.warning("No unclustered articles found — skipping %s", phase_name.lower())
                return

            feelgood_task: asyncio.Task[list[ClusterSummary]] | None = None
            if self._use_feelgood_pipeline():
                feelgood_task = asyncio.create_task(
                    asyncio.to_thread(self._select_positive_article_summaries, list(articles))
                )

            clusters = self.clusterer.cluster(articles)
            if not clusters:
                logger.warning("No clusters formed — skipping %s", phase_name.lower())
                if feelgood_task is not None:
                    feelgood_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await feelgood_task
                return
            logger.info("Event cluster stage: %d clusters formed", len(clusters))

            clusters = self.cluster_validator.validate(clusters)
            logger.info("Event cluster validation stage: %d clusters after validation", len(clusters))
            clusters = split_disjoint_event_articles(clusters)
            logger.info("Pre-summary disjoint event split stage: %d clusters after splitting", len(clusters))

            hot_cfg = self.cfg.output.get("hot_topics", {}) if isinstance(self.cfg.output, dict) else {}
            candidate_window = hot_cfg.get(
                "candidate_window",
                self.cfg.clustering.get("max_clusters_per_report", 20) * 2,
            )
            candidate_window = max(candidate_window, self.cfg.clustering.get("max_clusters_per_report", 20))
            candidate_clusters = clusters[:candidate_window]
            logger.info(
                "Hot topic candidate window: %d of %d clusters retained before enrichment",
                len(candidate_clusters),
                len(clusters),
            )
            for index, cluster in enumerate(candidate_clusters):
                cluster._storyline_candidate_index = index  # type: ignore[attr-defined]

            if hot_cfg.get("enabled", False):
                history_window_days = hot_cfg.get("history_window_days", 5)
                historical_hot_topic_memory = get_recent_clusters(
                    days=history_window_days,
                    anchor_date=today.isoformat(),
                )
                logger.info(
                    "Storyline history stage: %d prior clusters from %d day window",
                    len(historical_hot_topic_memory),
                    history_window_days,
                )
                self.storyline_resolver.resolve(
                    candidate_clusters,
                    historical_hot_topic_memory,
                    today,
                )
                _log_storyline_stage("Storyline stage: resolved candidate families", candidate_clusters)

            hot_clusters, main_clusters = select_report_clusters(candidate_clusters, self.cfg)
            selected_clusters = hot_clusters + main_clusters
            hot_storyline_keys = {cluster.storyline_key or "" for cluster in hot_clusters}
            _warn_on_storyline_near_miss(candidate_clusters, hot_storyline_keys, "Storyline stage")
            _log_storyline_stage("Storyline stage: final candidate families before enrichment", selected_clusters)

            logger.info(
                "Selected %d hotspot candidate items and %d main candidates for enrichment/summarization (from %d total clusters; candidate window=%d)",
                len(hot_clusters),
                len(main_clusters),
                len(clusters),
                len(candidate_clusters),
            )

            # Phase 2.5: Actively seek missing perspectives using Tavily
            selected_clusters = self.seeker.enhance_clusters(selected_clusters)
            for cluster in selected_clusters:
                for article in cluster.articles:
                    if article.id is not None:
                        continue
                    article.id = insert_article(article)
                    if article.id is None and callable(get_article_id_by_url):
                        article.id = get_article_id_by_url(article.url)

            precheck_reports = self.quality_assessor.assess_clusters(selected_clusters)
            quality_stats = Counter(report.status for report in precheck_reports)
            selected_clusters = [
                cluster
                for cluster in selected_clusters
                if not (cluster.quality_report and cluster.quality_report.status == "suppress")
            ]
            logger.info(
                "Quality precheck: %s; %d clusters retained",
                dict(quality_stats),
                len(selected_clusters),
            )
            self.storyline_state_machine.apply(
                selected_clusters,
                get_recent_clusters(
                    days=hot_cfg.get("history_window_days", 5),
                    anchor_date=today.isoformat(),
                ),
                today,
            )

            summaries = self.summarizer.summarize_all_batch(selected_clusters)
            for summary in summaries:
                self.quality_assessor.postcheck_summary(summary)

            # Phase 2.6: Evaluate freshness against historical clusters
            historical = get_recent_clusters(
                days=self.cfg.dedup.get("window_days", 3),
                anchor_date=today.isoformat(),
            )
            logger.info("Freshness check: %d historical clusters from past %d days",
                        len(historical), self.cfg.dedup.get("window_days", 3))

            # Evaluate each cluster's freshness
            freshness_results = self.freshness_evaluator.classify_all(
                [(cs.cluster, cs.summary) for cs in summaries],
                historical,
            )

            # Filter out stale clusters and store freshness metadata
            kept_summaries: list[ClusterSummary] = []
            stats = {"new": 0, "developing": 0, "stale": 0}

            for cs, (cluster, summary, freshness) in zip(summaries, freshness_results):
                stats[freshness.state] += 1

                if freshness.state == "stale":
                    logger.info("Skipping stale cluster: %s", cs.summary[:60])
                    continue

                # Attach freshness metadata to the ClusterSummary for rendering
                cs.freshness_state = freshness.state
                cs.continues_cluster_id = freshness.continues_cluster_id

                # Store cluster with freshness metadata
                cluster_record = Cluster(
                    topic_category=cs.cluster.topic_category,
                    article_ids=[a.id for a in cs.cluster.articles if a.id],
                    summary=cs.summary,
                    perspectives=cs.perspectives,
                    report_date=today.isoformat(),
                    freshness_state=freshness.state,
                    continues_cluster_id=freshness.continues_cluster_id,
                    storyline_key=cs.cluster.storyline_key,
                    storyline_name=cs.cluster.storyline_name,
                    storyline_role=cs.cluster.storyline_role,
                    storyline_confidence=cs.cluster.storyline_confidence,
                    storyline_state=cs.storyline_state or cs.cluster.storyline_state,
                    quality_status=cs.quality_status,
                    quality_score=cs.quality_score,
                )
                cluster_id = insert_cluster(cluster_record)
                if cs.quality_report is not None:
                    insert_cluster_quality_report(cluster_id, cs.quality_report)
                upsert_storyline_state(cluster_id, cs, today.isoformat())
                mark_articles_clustered([a.id for a in cs.cluster.articles if a.id])

                kept_summaries.append(cs)

            logger.info(
                "Freshness results: %d new, %d developing, %d stale (filtered)",
                stats["new"], stats["developing"], stats["stale"],
            )

            hot_topics, focus_storylines, main_summaries = select_hot_topic_families(kept_summaries, self.cfg)
            positive_summaries: list[ClusterSummary] = []
            positive_cfg = self._positive_energy_cfg()
            if self._use_feelgood_pipeline():
                if feelgood_task is not None:
                    try:
                        positive_summaries = await feelgood_task
                    except Exception as exc:
                        logger.warning("Feelgood micro-pipeline failed; positive section omitted: %s", exc)
                        positive_summaries = []
            else:
                positive_pool = positive_energy_classification_pool(
                    kept_summaries,
                    main_summaries,
                    hot_topics,
                    focus_storylines,
                    self.cfg,
                )
                if bool(positive_cfg.get("enabled", True)) and positive_pool:
                    logger.info(
                        "Positive energy candidate pool: %d main summaries + %d positive extras",
                        len(main_summaries),
                        max(0, len(positive_pool) - len(main_summaries)),
                    )
                    positive_classifications = self.summarizer.classify_positive_energy(positive_pool)
                    positive_summaries = select_positive_energy_summaries(
                        positive_pool,
                        positive_classifications,
                        self.cfg,
                    )

            plan = self.editorial_planner.plan(
                kept_summaries,
                local_positive_summaries=positive_summaries,
            )
            hot_topics = plan.hot_topics
            focus_storylines = plan.focus_storylines
            regular_summaries = plan.regular_summaries
            positive_summaries = plan.positive_summaries
            if self._use_feelgood_pipeline():
                logger.info(
                    "Positive energy local pipeline from existing articles: selected=%d use_llm_classifier=false",
                    len(positive_summaries),
                )
            english_cfg = self.cfg.output.get("english", {}) if isinstance(self.cfg.output, dict) else {}
            english_enabled = bool(english_cfg.get("enabled", False))
            if english_enabled:
                self.summarizer.translate_report_content(
                    kept_summaries,
                    hot_topics=hot_topics,
                    focus_storylines=focus_storylines,
                )
            positive_summaries = self.summarizer.normalize_positive_energy_summaries(
                positive_summaries,
                include_english=english_enabled,
            )
            focus_storyline_story_count = sum(
                len(family.get("summaries", []))
                for family in focus_storylines
                if isinstance(family.get("summaries"), list)
            )
            hot_topic_story_count = sum(
                len(family.get("summaries", []))
                for family in hot_topics
                if isinstance(family.get("summaries"), list)
            )
            total_story_count = (
                len(regular_summaries)
                + len(positive_summaries)
                + focus_storyline_story_count
                + hot_topic_story_count
            )
            hot_storyline_keys = {
                str(family.get("macro_topic_key", ""))
                for family in hot_topics
                if isinstance(family.get("macro_topic_key"), str)
            }
            _warn_on_summary_storyline_near_miss(kept_summaries, hot_storyline_keys, "Storyline stage after freshness")
            _log_storyline_stage(
                "Storyline stage: final families after freshness",
                [summary.cluster for summary in kept_summaries],
            )

            logger.info(
                "Story display groups: %d hot topics, %d focus storylines, %d positive stories, %d regular main stories (cap=%d)",
                len(hot_topics),
                len(focus_storylines),
                len(positive_summaries),
                len(regular_summaries),
                self.cfg.clustering.get("max_clusters_per_report", 20),
            )
            logger.info(
                "Render input: %d kept stories after freshness (%d regular main, %d positive, %d focus storyline stories, %d hot topic stories)",
                total_story_count,
                len(regular_summaries),
                len(positive_summaries),
                focus_storyline_story_count,
                hot_topic_story_count,
            )

            html_path = self.renderer.render(
                regular_summaries,
                today,
                hot_topics=hot_topics,
                focus_storylines=focus_storylines,
                positive_summaries=positive_summaries,
                report_subdir=self._staging_subdir if not push_after_render else None,
                update_latest=push_after_render,
            )
            if push_after_render:
                publish_summaries = [
                    summary
                    for family in hot_topics + focus_storylines
                    for summary in family.get("summaries", [])
                    if isinstance(summary, ClusterSummary)
                ]
                publish_summaries.extend(positive_summaries)
                publish_summaries.extend(regular_summaries)
                await self.publisher.publish(
                    publish_summaries,
                    today,
                )
                logger.info(
                    "Report latest promotion: %s",
                    "updated latest symlink" if total_story_count > 0 else "kept dated-only output; latest unchanged",
                )
            else:
                self._write_publish_complete(today, total_story_count)
                logger.info(
                    "Report staged for push: report=%s flag=%s latest=unchanged",
                    html_path,
                    self.publish_complete_flag,
                )

            logger.info(
                "=== %s done: %d clusters (%d stale filtered, %d hotspot tabs), report at %s (duration_s=%.2f) ===",
                phase_name,
                len(summaries),
                stats["stale"],
                len(hot_topics),
                html_path,
                time.perf_counter() - started,
            )

    async def push(self, report_date: date | None = None, attempt: int = 0) -> None:
        """Promote staged report output and send the Telegram digest."""
        started = time.perf_counter()
        today = report_date or date.today()
        staged_dir = self._staging_report_dir(today)

        if not self._is_publish_complete(today):
            logger.warning(
                "Push skipped: staged report not ready for %s (attempt=%d)",
                today.isoformat(),
                attempt,
            )
            if not self._schedule_push_retry(today, attempt):
                logger.error("Push failed: no completed staged report for %s", today.isoformat())
            return

        async with self._pipeline_lock:
            if not self._is_publish_complete(today):
                logger.warning(
                    "Push re-check failed: staged report no longer ready for %s (attempt=%d)",
                    today.isoformat(),
                    attempt,
                )
                if not self._schedule_push_retry(today, attempt):
                    logger.error("Push failed after re-check: staged report missing for %s", today.isoformat())
                return

            logger.info("=== PUSH phase started: report_date=%s attempt=%d ===", today.isoformat(), attempt)
            payload = self._load_staged_render_payload(today)
            total_story_count = int(payload.get("total_cluster_count", 0) or 0)
            data_path = staged_dir / "data.json"
            final_dir = self._promote_staged_report(today)
            self._promote_latest_symlink(today, total_story_count)
            await self.publisher.publish_rendered(final_dir / "data.json", today)
            self._clear_publish_complete()
            logger.info(
                "=== PUSH done: report=%s source=%s total_story_count=%d duration_s=%.2f ===",
                final_dir / "index.html",
                data_path,
                total_story_count,
                time.perf_counter() - started,
            )

    async def replay(self, report_date: date | None = None, dry_run: bool = False) -> None:
        """Reset one report date's article set and rerun publish from that exact set."""
        target_date = report_date or date.today()
        target_date_str = target_date.isoformat()
        logger.info("=== REPLAY started for report_date=%s dry_run=%s ===", target_date_str, dry_run)

        article_ids = get_report_article_ids(target_date_str)
        cluster_count = len(get_clusters_for_date(target_date_str))
        if not article_ids:
            logger.warning("Replay: no clusters found for report_date=%s; nothing to reset", target_date_str)
            return

        logger.info(
            "Replay target: report_date=%s cluster_rows=%d article_ids=%d",
            target_date_str,
            cluster_count,
            len(article_ids),
        )
        if dry_run:
            logger.info("Replay dry-run: no DB changes applied for report_date=%s", target_date_str)
            return

        deleted_clusters = delete_clusters_for_date(target_date_str)
        reset_articles = reset_articles_clustered(article_ids)
        replay_articles = get_articles_by_ids(article_ids)

        logger.info(
            "Replay reset applied: deleted_clusters=%d reset_articles=%d",
            deleted_clusters,
            reset_articles,
        )
        logger.info(
            "Replay publish start: report_date=%s article_count=%d",
            target_date_str,
            len(replay_articles),
        )
        await self.publish(report_date=target_date, articles_override=replay_articles, push_after_render=True)
        logger.info("=== REPLAY done for report_date=%s ===", target_date_str)

    async def run_once(self) -> None:
        """Full pipeline in one go (useful for testing / manual runs)."""
        logger.info("=== RUN_ONCE started ===")
        try:
            logger.info("RUN_ONCE boundary: before collect")
            await self.collect(mode="full")
            logger.info("RUN_ONCE boundary: after collect")
            logger.info("RUN_ONCE boundary: before publish")
            await self.publish(push_after_render=True)
            logger.info("RUN_ONCE boundary: after publish")
            logger.info("=== RUN_ONCE done ===")
        except Exception:
            logger.exception("RUN_ONCE failed")
            raise

    # ─── LONG-RUNNING SCHEDULER ──────────────────────────────────────────────

    def start(self) -> None:
        """Start APScheduler with configured cron times."""
        try:
            asyncio.run(self._run_scheduler())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")

    async def _run_scheduler(self) -> None:
        """Async scheduler loop — runs inside asyncio.run()."""
        tz = self.cfg.schedule.get("timezone", "Asia/Shanghai")
        sched = AsyncIOScheduler(timezone=tz)
        self._apscheduler = sched
        self._cleanup_old_staging()
        full_collect_cron = self.cfg.schedule.get(
            "full_collect_cron",
            self.cfg.schedule.get("collect_cron", "0 */4 * * *"),
        )
        delta_collect_cron = self.cfg.schedule.get("prepublish_collect_cron")
        publish_cron = self.cfg.schedule.get("publish_cron", "30 7 * * *")
        push_cron = self.cfg.schedule.get("push_cron", "0 8 * * *")

        sched.add_job(
            partial(self.collect, mode="full"),
            CronTrigger.from_crontab(full_collect_cron, timezone=tz),
            id="collect_full",
        )
        if delta_collect_cron:
            sched.add_job(
                partial(self.collect, mode="delta"),
                CronTrigger.from_crontab(delta_collect_cron, timezone=tz),
                id="collect_delta",
            )
        sched.add_job(
            partial(self.publish, push_after_render=False),
            CronTrigger.from_crontab(publish_cron, timezone=tz),
            id="publish_stage",
        )
        sched.add_job(
            self.push,
            CronTrigger.from_crontab(push_cron, timezone=tz),
            id="push_daily",
        )

        sched.start()
        logger.info(
            "Scheduler started. full_collect=%s delta_collect=%s publish_stage=%s push=%s tz=%s",
            full_collect_cron,
            delta_collect_cron,
            publish_cron,
            push_cron,
            tz,
        )

        # Block until cancelled (KeyboardInterrupt → asyncio.run cancels all tasks)
        await asyncio.Event().wait()
