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
    insert_cluster_evaluation,
    link_cluster_evaluation,
    mark_articles_clustered,
    reset_articles_clustered,
    seed_calibration_weights,
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
    select_positive_summaries,
    select_report_clusters,
)
from newsprism.service.history import (
    EventClusterValidator,
    FreshnessEvaluator,
    StorylineResolver,
    StorylineStateMachine,
)
from newsprism.service.impact import ImpactAssessor
from newsprism.service.seeker import ActiveSeeker
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
        self.deduplicator = Deduplicator(cfg)
        self.clusterer = LLMClusterer(cfg) if cfg.use_llm_clustering else Clusterer(cfg)
        self.cluster_validator = EventClusterValidator(cfg)
        self.seeker = ActiveSeeker(cfg)
        self.summarizer = Summarizer(cfg)
        self.impact_assessor = ImpactAssessor(cfg)
        seed_calibration_weights(self.impact_assessor.seed_weights)
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
            source_certifications=cfg.certifications,
        )
        self.renderer.day_navigation_cfg = cfg.output.get("day_navigation", {}) if isinstance(cfg.output, dict) else {}

    def _positive_energy_cfg(self) -> dict:
        return self.cfg.output.get("positive_energy", {}) if isinstance(self.cfg.output, dict) else {}

    def _persist_impact_evaluations(self, clusters: list[ArticleCluster], report_date: date) -> None:
        """Persist every candidate's impact scores (training/audit record)."""
        assessments = [c.impact for c in clusters if c.impact is not None]
        ranked = sorted(assessments, key=lambda a: -a.composite)
        rank_by_key = {a.cluster_key: position for position, a in enumerate(ranked, 1)}
        for assessment in assessments:
            with contextlib.suppress(Exception):
                insert_cluster_evaluation(
                    report_date=report_date.isoformat(),
                    cluster_key=assessment.cluster_key,
                    dims=assessment.dims,
                    rationale=assessment.rationale,
                    signal=assessment.signal,
                    composite=assessment.composite,
                    rank=rank_by_key.get(assessment.cluster_key),
                    display_category=assessment.display_category,
                    status=assessment.status,
                    flags=assessment.flags,
                    evaluated_by_llm=assessment.evaluated_by_llm,
                    model=assessment.model,
                    subject_regions=assessment.subject_regions,
                )

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
        """Phase 1: Collect → dedup → persist (selection is deferred to impact)."""
        phase_name = "COLLECT_DELTA" if mode == "delta" else "COLLECT"
        async with self._pipeline_lock:
            started = time.perf_counter()
            logger.info("=== %s phase started ===", phase_name)

            raw_articles = await self.collector.collect_all(mode=mode)
            db_articles = raw_to_articles(raw_articles)

            deduped = self.deduplicator.deduplicate(db_articles)

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

            clusters = self.clusterer.cluster(articles)
            if not clusters:
                logger.warning("No clusters formed — skipping %s", phase_name.lower())
                return
            logger.info("Event cluster stage: %d clusters formed", len(clusters))

            clusters = self.cluster_validator.validate(clusters)
            logger.info("Event cluster validation stage: %d clusters after validation", len(clusters))

            hot_cfg = self.cfg.output.get("hot_topics", {}) if isinstance(self.cfg.output, dict) else {}
            impact_cfg = (self.cfg.editorial_values or {}).get("impact", {})
            # Bound the impact-evaluation window by the local cross-source signal
            # (no LLM, no keywords): the strongest multi-source/multi-region stories
            # plus the strongest single-source ones get LLM-scored.
            candidate_window = max(
                int(impact_cfg.get("candidate_window", 90)),
                self.cfg.clustering.get("max_clusters_per_report", 20),
            )
            candidate_clusters = self.impact_assessor.rank_candidates(clusters, candidate_window)
            logger.info(
                "Impact candidate window: %d of %d clusters retained (signal-ranked)",
                len(candidate_clusters),
                len(clusters),
            )
            for index, cluster in enumerate(candidate_clusters):
                cluster._storyline_candidate_index = index  # type: ignore[attr-defined]

            # Impact evaluation — the selection brain.
            self.impact_assessor.assess_clusters(candidate_clusters)

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

            # Phase 2.5: Actively seek missing perspectives (impact status decides where)
            selected_clusters = self.seeker.enhance_clusters(selected_clusters)
            for cluster in selected_clusters:
                # Seeker may have added articles → refresh the local signal/status.
                self.impact_assessor.recompute_local(cluster)
                for article in cluster.articles:
                    if article.id is not None:
                        continue
                    article.id = insert_article(article)
                    if article.id is None and callable(get_article_id_by_url):
                        article.id = get_article_id_by_url(article.url)

            selected_clusters = [
                cluster
                for cluster in selected_clusters
                if not (cluster.impact and cluster.impact.status == "suppress")
            ]
            self._persist_impact_evaluations(candidate_clusters, today)
            logger.info(
                "Impact selection: %s; %d clusters retained for summarization",
                dict(Counter(c.impact.status for c in selected_clusters if c.impact)),
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
                cs.cluster_db_id = cluster_id
                if cs.cluster.impact is not None:
                    with contextlib.suppress(Exception):
                        link_cluster_evaluation(
                            today.isoformat(),
                            cs.cluster.impact.cluster_key,
                            cluster_id,
                            selected=True,
                        )
                upsert_storyline_state(cluster_id, cs, today.isoformat())
                mark_articles_clustered([a.id for a in cs.cluster.articles if a.id])

                kept_summaries.append(cs)

            logger.info(
                "Freshness results: %d new, %d developing, %d stale (filtered)",
                stats["new"], stats["developing"], stats["stale"],
            )

            base_plan = self.editorial_planner.base_plan(kept_summaries)
            # 今日正能量 is the feelgood dimension of the same impact evaluation.
            positive_summaries = select_positive_summaries(kept_summaries, self.cfg)
            plan = self.editorial_planner.finalize(base_plan, positive_summaries=positive_summaries)
            hot_topics = plan.hot_topics
            regular_summaries = plan.regular_summaries
            positive_summaries = plan.positive_summaries

            english_cfg = self.cfg.output.get("english", {}) if isinstance(self.cfg.output, dict) else {}
            english_enabled = bool(english_cfg.get("enabled", False))
            if english_enabled:
                self.summarizer.translate_report_content(
                    kept_summaries,
                    hot_topics=hot_topics,
                    focus_storylines=[],
                )
            hot_topic_story_count = sum(
                len(family.get("summaries", []))
                for family in hot_topics
                if isinstance(family.get("summaries"), list)
            )
            total_story_count = (
                len(regular_summaries)
                + len(positive_summaries)
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
                "Story display groups: %d hot topics, %d positive stories, %d regular main stories (cap=%d)",
                len(hot_topics),
                len(positive_summaries),
                len(regular_summaries),
                self.cfg.clustering.get("max_clusters_per_report", 20),
            )
            logger.info(
                "Render input: %d kept stories after freshness (%d regular main, %d positive, %d hot topic stories)",
                total_story_count,
                len(regular_summaries),
                len(positive_summaries),
                hot_topic_story_count,
            )

            html_path = self.renderer.render(
                regular_summaries,
                today,
                hot_topics=hot_topics,
                focus_storylines=[],
                positive_summaries=positive_summaries,
                report_subdir=self._staging_subdir if not push_after_render else None,
                update_latest=push_after_render,
            )
            if push_after_render:
                publish_summaries = [
                    summary
                    for family in hot_topics
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

    async def _run_calibration(self) -> None:
        """Weekly: nudge impact weights and refresh the editorial-policy memory."""
        from newsprism.service.calibrate import run_calibration

        try:
            result = await asyncio.to_thread(run_calibration, self.cfg)
            logger.info("Calibration run: %s", result)
        except Exception:
            logger.exception("Calibration run failed")

    async def _run_retention(self) -> None:
        """Weekly: drop unclustered articles past the retention window."""
        from newsprism.repo import delete_old_unclustered_articles

        days = int(self.cfg.evolution.get("retention_days", 30)) if isinstance(self.cfg.evolution, dict) else 30
        try:
            deleted = await asyncio.to_thread(delete_old_unclustered_articles, days)
            logger.info("Retention: deleted %d unclustered articles older than %d days", deleted, days)
        except Exception:
            logger.exception("Retention job failed")

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

        # ─── Evolution loop (feedback → calibration → memory; retention) ──────
        evolution = self.cfg.evolution if isinstance(self.cfg.evolution, dict) else {}
        if evolution.get("calibration", {}).get("enabled", True):
            calibrate_cron = self.cfg.schedule.get("calibrate_cron", "30 3 * * 1")
            sched.add_job(
                self._run_calibration,
                CronTrigger.from_crontab(calibrate_cron, timezone=tz),
                id="calibrate_weekly",
            )
        retention_cron = self.cfg.schedule.get("retention_cron", "0 4 * * 1")
        sched.add_job(
            self._run_retention,
            CronTrigger.from_crontab(retention_cron, timezone=tz),
            id="retention_weekly",
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
