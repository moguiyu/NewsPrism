"""Cross-day story history — freshness, cluster validation, storyline grouping.

Merges the former freshness.py and storyline.py into one keyword-free module.
Everything here answers the same question at different granularities: how does
today's cluster relate to recent coverage?

- FreshnessEvaluator     new / developing / stale vs the recent cluster history
- EventClusterValidator  splits incoherent clusters (embedding + title ngrams)
- StorylineResolver      groups related clusters into storyline families using
                         centroid similarity + the LLM relation classifier
- StorylineStateMachine  lifecycle state + compact persisted timeline

No curated vocabulary: decisions come from embeddings, character-ngram string
overlap, shared history keys, and LLM relation judgments.

Layer: service (imports types, config; never imports runtime)
"""
from __future__ import annotations

import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

import numpy as np

from newsprism.config import Config
from newsprism.service.embeddings import get_model
from newsprism.types import Article, ArticleCluster, Cluster, StorylineEvent

logger = logging.getLogger(__name__)

_DEFAULT_ICON = "globe"
_MAX_PAIR_CANDIDATES = 24
_NEIGHBORS_PER_NODE = 4

# Cluster-validation thresholds (stable across releases; not editorial tuning)
_PAIR_SPLIT_SIMILARITY = 0.50
_PAIR_SPLIT_TITLE_OVERLAP = 0.03
_OUTLIER_AVG_SIMILARITY = 0.58
_OUTLIER_AVG_TITLE_OVERLAP = 0.05


# ─── SHARED TEXT/VECTOR HELPERS ───────────────────────────────────────────────

def _char_ngrams(text: str, n: int = 3) -> set[str]:
    compact = re.sub(r"[^\w一-鿿]+", "", text.lower())
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


def _content_hash(*parts: str) -> str:
    """Stable 8-char hash for storyline keys (Issue #4).

    Per-run counters like ``storyline-1`` and ``single-26`` collide across
    days and get reused by the history matcher for unrelated topics — same key
    meant 8 different storylines over two weeks. A content-derived hash makes
    the key deterministic for the same anchor text and naturally different for
    a different topic.
    """
    canonical = "|".join(sorted(p for p in parts if p))
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:8]


def _short_name(value: str, max_chars: int) -> str:
    compact = re.sub(r"\s+", "", value).strip()
    compact = re.sub(r"^(热点专题[-:：]?|专题[-:：]?)", "", compact).strip()
    compact = compact[:max_chars].strip(" -:：，,、。.；;")
    return compact or "焦点话题"


# ─── FRESHNESS ────────────────────────────────────────────────────────────────

@dataclass
class FreshnessResult:
    """Result of freshness evaluation for a cluster."""
    state: str  # "new" | "developing" | "stale"
    continues_cluster_id: int | None = None
    similarity_score: float = 0.0
    new_sources: list[str] | None = None


class FreshnessEvaluator:
    """Classifies clusters as new, developing, or stale vs recent history."""

    def __init__(self, cfg: Config) -> None:
        self.similarity_threshold = 0.85  # high bar for "same story as before"
        self.window_days = cfg.dedup.get("window_days", 3)
        self._embedding_cache: dict[int, np.ndarray] = {}
        self._text_embedding_cache: dict[str, np.ndarray] = {}

    def evaluate(
        self,
        cluster: ArticleCluster,
        summary: str,
        historical_clusters: list[Cluster],
    ) -> FreshnessResult:
        if not historical_clusters or self.window_days <= 0:
            return FreshnessResult(state="new")

        new_embedding = self._get_text_embedding(summary)
        best_match: Cluster | None = None
        best_similarity = 0.0
        for hist in historical_clusters:
            if not hist.summary:
                continue
            similarity = float(np.dot(new_embedding, self._get_cached_embedding(hist)))
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = hist

        if best_similarity < self.similarity_threshold or best_match is None:
            return FreshnessResult(state="new", similarity_score=best_similarity)

        new_sources = list(set(cluster.sources) - set(best_match.perspectives.keys()))
        if new_sources:
            return FreshnessResult(
                state="developing",
                continues_cluster_id=best_match.id,
                similarity_score=best_similarity,
                new_sources=new_sources,
            )
        return FreshnessResult(
            state="stale",
            continues_cluster_id=best_match.id,
            similarity_score=best_similarity,
        )

    def classify_all(
        self,
        cluster_summaries: list[tuple[ArticleCluster, str]],
        historical_clusters: list[Cluster],
    ) -> list[tuple[ArticleCluster, str, FreshnessResult]]:
        return [
            (cluster, summary, self.evaluate(cluster, summary, historical_clusters))
            for cluster, summary in cluster_summaries
        ]

    def score_text_to_historical_cluster(self, text: str, historical_cluster: Cluster) -> float:
        if not historical_cluster.summary:
            return 0.0
        new_embedding = self._get_text_embedding(text)
        return float(np.dot(new_embedding, self._get_cached_embedding(historical_cluster)))

    def _compute_embedding(self, text: str) -> np.ndarray:
        return get_model().encode([text], normalize_embeddings=True, show_progress_bar=False)[0]

    def _get_cached_embedding(self, cluster: Cluster) -> np.ndarray:
        if cluster.id in self._embedding_cache:
            return self._embedding_cache[cluster.id]
        embedding = self._compute_embedding(cluster.summary)
        if cluster.id is not None:
            self._embedding_cache[cluster.id] = embedding
        return embedding

    def _get_text_embedding(self, text: str) -> np.ndarray:
        cached = self._text_embedding_cache.get(text)
        if cached is not None:
            return cached
        embedding = self._compute_embedding(text)
        self._text_embedding_cache[text] = embedding
        return embedding


# ─── EVENT CLUSTER VALIDATION ─────────────────────────────────────────────────

class EventClusterValidator:
    """Splits clusters whose members do not cohere (embedding + title ngrams)."""

    def __init__(self, cfg: Config) -> None:
        hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
        self.enabled = hot_cfg.get("enabled", False)

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
            left, right = articles
            similarity = _cosine(
                np.array(left.embedding, dtype=float) if left.embedding is not None else None,
                np.array(right.embedding, dtype=float) if right.embedding is not None else None,
            )
            if (
                similarity < _PAIR_SPLIT_SIMILARITY
                and _title_overlap(left.title, right.title) < _PAIR_SPLIT_TITLE_OVERLAP
            ):
                return [
                    ArticleCluster(topic_category=left.title[:60], articles=[left]),
                    ArticleCluster(topic_category=right.title[:60], articles=[right]),
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
            if avg_sim < _OUTLIER_AVG_SIMILARITY and avg_overlap < _OUTLIER_AVG_TITLE_OVERLAP:
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
            validated.append(ArticleCluster(topic_category=cluster.topic_category, articles=retained))
        validated.extend(
            ArticleCluster(topic_category=article.title[:60], articles=[article])
            for article in outliers
        )
        return validated


# ─── STORYLINE LIFECYCLE ──────────────────────────────────────────────────────

class StorylineStateMachine:
    """Assign lifecycle state and a compact timeline for storyline clusters."""

    STATES = {"emerging", "developing", "stabilized", "archived"}

    def resolve_state(
        self,
        cluster: ArticleCluster,
        historical_clusters: list[Cluster],
    ) -> str:
        related_history = self._related_history(cluster, historical_clusters)
        if not related_history:
            return "emerging"
        impact = cluster.impact
        regions = {
            article.origin_region for article in cluster.articles if article.origin_region
        }
        if impact is not None and impact.composite >= 0.55 and (len(cluster.sources) >= 3 or len(regions) >= 2):
            return "stabilized"
        return "developing"

    def apply(
        self,
        clusters: list[ArticleCluster],
        historical_clusters: list[Cluster],
        current_date: date,
    ) -> list[ArticleCluster]:
        for cluster in clusters:
            state = self.resolve_state(cluster, historical_clusters)
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
                quality_score=(cluster.impact.composite if cluster.impact else 0.0),
                event_type="current",
            )
        )
        events.sort(key=lambda event: event.event_date)
        return events[-5:]

    def _related_history(self, cluster: ArticleCluster, historical_clusters: list[Cluster]) -> list[Cluster]:
        key = cluster.storyline_key or cluster.macro_topic_key
        if not key:
            return []
        related = [historical for historical in historical_clusters if historical.storyline_key == key]
        related.sort(key=lambda historical: (historical.report_date, historical.id or 0), reverse=True)
        return related


# ─── STORYLINE RESOLUTION ─────────────────────────────────────────────────────

class StorylineResolver:
    """Group related event clusters into storyline families.

    Pair candidates come from centroid cosine + title-ngram overlap + shared
    history; the LLM relation classifier accepts/rejects each pair; union-find
    over accepted edges forms families. Roles derive from edge relations.
    """

    def __init__(self, cfg: Config, summarizer, similarity_fn) -> None:
        self.cfg = cfg
        self.summarizer = summarizer
        self.similarity_fn = similarity_fn
        hot_cfg = cfg.output.get("hot_topics", {}) if isinstance(cfg.output, dict) else {}
        self.max_name_chars = int(hot_cfg.get("tab_name_max_chars", 10))
        self.edge_confidence_threshold = float(hot_cfg.get("edge_confidence_threshold", 0.56))
        self.candidate_similarity = float(hot_cfg.get("admission_similarity", 0.62))
        self.history_similarity_threshold = float(hot_cfg.get("history_similarity_threshold", 0.48))
        # A storyline family must be internally coherent: the mean pairwise
        # centroid cosine across *all* its members must clear this bar. Union-find
        # over accepted edges otherwise chains unrelated clusters transitively
        # (A~B, B~C, … each an edge, but A and Z share nothing).
        self.coherence_min_similarity = float(hot_cfg.get("storyline_coherence_min", 0.60))
        # Relaxed coherence bar for components whose members are glued by
        # same_conflict_different_event edges (different daily incidents of the
        # same ongoing conflict have low centroid similarity by design).
        # Issue #2 rec #3.
        self.conflict_coherence_min_similarity = float(
            hot_cfg.get("storyline_conflict_coherence_min", 0.40)
        )
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
        relations = self._classify_pairs(pair_candidates)
        accepted_edges = [
            edge
            for edge in relations
            if edge["relation"] != "not_related"
            and float(edge["confidence"]) >= self.edge_confidence_threshold
        ]
        components = self._build_components(len(clusters), accepted_edges)
        components = self._enforce_coherence(components, profiles, accepted_edges)
        logger.info(
            "Storyline resolver: clusters=%d history_matches=%d pairs=%d accepted_edges=%d families=%d",
            len(clusters),
            len(history_matches),
            len(pair_candidates),
            len(accepted_edges),
            sum(1 for members in components.values() if len(members) > 1),
        )
        self._assign_storylines(clusters, profiles, components, accepted_edges, history_matches)
        self._split_incoherent_families(clusters, profiles, history_matches, accepted_edges)
        return clusters

    # ── profile / history ────────────────────────────────────────────────────

    def _build_profile(self, cluster: ArticleCluster, index: int) -> dict[str, object]:
        lead_title = cluster.articles[0].title if cluster.articles else cluster.topic_category
        return {
            "index": index,
            "cluster": cluster,
            "text": _cluster_text(cluster),
            "lead_title": lead_title,
            "title_ngrams": _char_ngrams(lead_title),
            "centroid": _cluster_centroid(cluster),
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
                if not historical.storyline_key:
                    continue
                similarity = self.similarity_fn(str(profile["text"]), historical)
                if similarity < self.history_similarity_threshold:
                    continue
                recency_bonus = 0.0
                try:
                    days_old = max((current_date - date.fromisoformat(historical.report_date)).days, 0)
                    recency_bonus = max(0.0, (5 - days_old) / 5.0) * 0.08
                except ValueError:
                    pass
                score = similarity + recency_bonus
                if score <= best_score:
                    continue
                best_score = score
                best_match = {
                    "storyline_key": historical.storyline_key,
                    "storyline_name": historical.storyline_name,
                    "storyline_role": historical.storyline_role,
                    "score": score,
                    "cluster_id": historical.id,
                }
            if best_match is not None:
                matches[int(profile["index"])] = best_match
        return matches

    # ── pair candidates / relations ──────────────────────────────────────────

    def _build_pair_candidates(
        self,
        profiles: list[dict[str, object]],
        history_matches: dict[int, dict[str, object]],
    ) -> list[dict[str, object]]:
        pair_candidates: list[dict[str, object]] = []
        seen: set[tuple[int, int]] = set()
        for idx, left in enumerate(profiles):
            scored_neighbors: list[tuple[float, dict[str, object]]] = []
            for right in profiles[idx + 1 :]:
                pair_key = (int(left["index"]), int(right["index"]))
                if pair_key in seen:
                    continue
                similarity = _cosine(left["centroid"], right["centroid"])
                title_overlap = _title_overlap(str(left["lead_title"]), str(right["lead_title"]))
                left_hist = history_matches.get(int(left["index"]), {})
                right_hist = history_matches.get(int(right["index"]), {})
                shared_history = bool(
                    left_hist.get("storyline_key")
                    and left_hist.get("storyline_key") == right_hist.get("storyline_key")
                )
                score = 0.0
                if similarity >= self.candidate_similarity:
                    score += 2.0 + similarity
                if title_overlap >= 0.06:
                    score += 1.0 + title_overlap
                if shared_history:
                    score += 1.5
                if score <= 0:
                    continue
                seen.add(pair_key)
                scored_neighbors.append(
                    (
                        score,
                        {
                            "left_index": pair_key[0],
                            "right_index": pair_key[1],
                            "left_cluster": left["cluster"],
                            "right_cluster": right["cluster"],
                            "left_history": left_hist,
                            "right_history": right_hist,
                            "similarity": similarity,
                            "title_overlap": title_overlap,
                            "signal_overlap": 1 if shared_history else 0,
                        },
                    )
                )
            scored_neighbors.sort(key=lambda item: item[0], reverse=True)
            for score, context in scored_neighbors[:_NEIGHBORS_PER_NODE]:
                pair_candidates.append({"score": score, **context})

        pair_candidates.sort(key=lambda item: float(item["score"]), reverse=True)
        return pair_candidates[:_MAX_PAIR_CANDIDATES]

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
        shared_history = bool(candidate.get("signal_overlap", 0))
        if similarity >= 0.80 and (title_overlap >= 0.06 or shared_history):
            relation = "same_core_storyline"
            confidence = min(0.92, 0.60 + similarity * 0.28)
        elif similarity >= 0.66 and (title_overlap >= 0.06 or shared_history):
            relation = "same_direct_spillover_storyline"
            confidence = min(0.85, 0.5 + similarity * 0.22 + title_overlap * 0.15)
        elif (
            similarity >= 0.50
            and shared_history
            and self._shares_conflict_keyword(candidate)
        ):
            # Fallback path (LLM unavailable): glue same-conflict different-event
            # pairs into the family as spillover. Issue #2 rec #3.
            relation = "same_conflict_different_event"
            confidence = 0.55
        else:
            relation = "not_related"
            confidence = max(0.15, 0.45 - similarity * 0.2)
        return {
            "left_index": int(candidate["left_index"]),
            "right_index": int(candidate["right_index"]),
            "relation": relation,
            "confidence": confidence,
        }

    # Conflict keyword pairs used by the heuristic fallback only (the LLM prompt
    # does the real work; this is a safety net when the LLM is unavailable).
    # Narrow whitelist — adding broad themes here would re-introduce the
    # over-merging bug that motivated the "precision-first" instruction.
    _CONFLICT_KEYWORD_PAIRS: tuple[tuple[str, str], ...] = (
        ("russia", "ukraine"), ("ru", "ua"), ("ukraine", "ru"),
        ("iran", "us"), ("us", "iran"), ("iran", "israel"),
        ("israel", "palest"), ("israel", "gaza"), ("israel", "hamas"),
        ("houthi",), ("netanyahu",),
    )

    def _shares_conflict_keyword(self, candidate: dict[str, object]) -> bool:
        """True if either cluster's text mentions a known ongoing-conflict keyword."""
        text = ""
        for side in ("left_cluster", "right_cluster"):
            cluster = candidate.get(side)
            if cluster is not None:
                # _cluster_text is the module-level helper used by StorylineResolver.
                text += " " + str(getattr(cluster, "topic_category", "") or "")
                articles = getattr(cluster, "articles", []) or []
                for article in articles[:3]:
                    text += " " + (getattr(article, "title", "") or "")
        text_lower = text.lower()
        return any(all(part in text_lower for part in pair) for pair in self._CONFLICT_KEYWORD_PAIRS)

    def _build_components(self, node_count: int, edges: list[dict[str, object]]) -> dict[int, list[int]]:
        parents = list(range(node_count))

        def find(idx: int) -> int:
            while parents[idx] != idx:
                parents[idx] = parents[parents[idx]]
                idx = parents[idx]
            return idx

        def union(left: int, right: int) -> None:
            left_root, right_root = find(left), find(right)
            if left_root != right_root:
                parents[right_root] = left_root

        for edge in edges:
            union(int(edge["left_index"]), int(edge["right_index"]))

        components: dict[int, list[int]] = defaultdict(list)
        for idx in range(node_count):
            components[find(idx)].append(idx)
        return components

    def _enforce_coherence(
        self,
        components: dict[int, list[int]],
        profiles: list[dict[str, object]],
        accepted_edges: list[dict[str, object]],
    ) -> dict[int, list[int]]:
        """Split transitively-chained components into internally-coherent families.

        Each multi-node component is re-grown from its medoid, admitting a member
        only while the family's mean pairwise centroid cosine stays above the
        coherence bar. Members that don't fit are ejected and recursively
        re-grouped, so a genuine second storyline survives while unrelated
        clusters fall out to singletons.

        Components whose members are glued by ``same_conflict_different_event``
        edges (Issue #2 rec #3) use a relaxed bar — different daily incidents of
        the same ongoing conflict have low centroid similarity by design, but
        the LLM has already judged them as belonging to the same storyline.
        """
        centroids = {int(p["index"]): p["centroid"] for p in profiles}
        conflict_pairs: set[tuple[int, int]] = set()
        for edge in accepted_edges:
            if edge.get("relation") == "same_conflict_different_event":
                li, ri = int(edge["left_index"]), int(edge["right_index"])
                conflict_pairs.add((li, ri))
                conflict_pairs.add((ri, li))

        def _is_conflict_component(members: list[int]) -> bool:
            return any(
                (a, b) in conflict_pairs
                for a in members
                for b in members
                if a != b
            )

        refined: dict[int, list[int]] = {}
        for members in components.values():
            if len(members) < 3:
                refined[members[0]] = members
                continue
            if _is_conflict_component(members):
                # Conflict-glued components are kept as-is — the LLM has judged
                # these as same_conflict_different_event, and different daily
                # incidents of an ongoing conflict legitimately have low or
                # zero centroid similarity (different locations, casualties,
                # branches). Coherence-splitting them would defeat the point.
                refined[members[0]] = members
                continue
            for sub in self._coherent_split(members, centroids):
                refined[sub[0]] = sub
        return refined

    def _coherent_split(
        self,
        nodes: list[int],
        centroids: dict[int, np.ndarray | None],
        bar: float | None = None,
    ) -> list[list[int]]:
        if bar is None:
            bar = self.coherence_min_similarity
        if len(nodes) <= 1:
            return [list(nodes)]
        medoid = max(nodes, key=lambda n: sum(_cosine(centroids[n], centroids[m]) for m in nodes if m != n))
        core = [medoid]
        remaining = [n for n in nodes if n != medoid]
        while remaining:
            best = max(remaining, key=lambda n: self._mean_pairwise(core + [n], centroids))
            if self._mean_pairwise(core + [best], centroids) < bar:
                break
            core.append(best)
            remaining.remove(best)
        result = [sorted(core)]
        if remaining:
            result.extend(self._coherent_split(remaining, centroids, bar))
        return result

    def _split_incoherent_families(
        self,
        clusters: list[ArticleCluster],
        profiles: list[dict[str, object]],
        history_matches: dict[int, dict[str, object]],
        accepted_edges: list[dict[str, object]],
    ) -> None:
        """Final authority on family coherence, over the *assigned* storyline keys.

        History reuse can glue the same stale storyline_key onto many unrelated
        singletons (a bloated historical storyline acts as a magnet), bypassing
        the component-level pass. Here every same-key group is re-checked: the
        coherent sub-family that genuinely continues the story (strongest history
        match to this key, else the largest coherent sub) keeps the storyline;
        every other member detaches to a standalone story.

        Components glued by ``same_conflict_different_event`` edges are exempt
        (Issue #2 rec #3): different daily incidents of an ongoing conflict
        have legitimately low centroid similarity, and the LLM has already
        judged them as a single storyline.
        """
        centroids = {int(p["index"]): p["centroid"] for p in profiles}
        conflict_pairs: set[tuple[int, int]] = set()
        for edge in accepted_edges:
            if edge.get("relation") == "same_conflict_different_event":
                li, ri = int(edge["left_index"]), int(edge["right_index"])
                conflict_pairs.add((li, ri))
                conflict_pairs.add((ri, li))

        def _is_conflict_group(members: list[int]) -> bool:
            return any(
                (a, b) in conflict_pairs
                for a in members
                for b in members
                if a != b
            )

        groups: dict[str, list[int]] = defaultdict(list)
        for idx, cluster in enumerate(clusters):
            if cluster.storyline_key:
                groups[str(cluster.storyline_key)].append(idx)

        for key, members in groups.items():
            if len(members) < 3:
                continue
            if _is_conflict_group(members):
                continue  # conflict-glued family — keep as-is
            subs = self._coherent_split(members, centroids)
            if len(subs) == 1:
                continue

            def _history_score(idx: int, key: str = key) -> float:
                match = history_matches.get(idx)
                return float(match["score"]) if match and match.get("storyline_key") == key else -1.0

            anchor = max(members, key=_history_score)
            primary = next((sub for sub in subs if anchor in sub), max(subs, key=len))
            if len(primary) < 2:
                primary = max(subs, key=len)
            keep = set(primary) if len(primary) >= 2 else set()
            for idx in members:
                if idx not in keep:
                    self._apply_singleton(clusters[idx], profiles[idx], None)

    @staticmethod
    def _mean_pairwise(nodes: list[int], centroids: dict[int, np.ndarray | None]) -> float:
        if len(nodes) < 2:
            return 1.0
        sims = [
            _cosine(centroids[a], centroids[b])
            for i, a in enumerate(nodes)
            for b in nodes[i + 1 :]
        ]
        return sum(sims) / len(sims)

    # ── assignment ───────────────────────────────────────────────────────────

    def _assign_storylines(
        self,
        clusters: list[ArticleCluster],
        profiles: list[dict[str, object]],
        components: dict[int, list[int]],
        edges: list[dict[str, object]],
        history_matches: dict[int, dict[str, object]],
    ) -> None:
        edge_map: dict[int, list[dict[str, object]]] = defaultdict(list)
        for edge in edges:
            edge_map[int(edge["left_index"])].append(edge)
            edge_map[int(edge["right_index"])].append(edge)

        storyline_counter = 1
        for component_nodes in components.values():
            if len(component_nodes) == 1:
                idx = component_nodes[0]
                self._apply_singleton(clusters[idx], profiles[idx], history_matches.get(idx))
                continue

            roles = {
                idx: (
                    "core"
                    if any(edge["relation"] == "same_core_storyline" for edge in edge_map.get(idx, []))
                    else "spillover"
                )
                for idx in component_nodes
            }
            core_nodes = sorted(idx for idx, role in roles.items() if role == "core")
            if not core_nodes:
                # A pure-spillover component still needs a core anchor: pick the
                # highest-composite node (ties broken by index) as the core event.
                anchor = max(
                    component_nodes,
                    key=lambda idx: (
                        clusters[idx].impact.composite if clusters[idx].impact else 0.0,
                        -idx,
                    ),
                )
                roles[anchor] = "core"
                core_nodes = [anchor]

            history_keys = {
                match["storyline_key"]
                for idx in component_nodes
                if (match := history_matches.get(idx)) and match.get("storyline_key")
            }
            reuse_key = history_keys.pop() if len(history_keys) == 1 else None
            if reuse_key:
                storyline_key = str(reuse_key)
                reuse_match = next(
                    (
                        match
                        for idx in component_nodes
                        if (match := history_matches.get(idx)) and match.get("storyline_key") == reuse_key
                    ),
                    None,
                )
                storyline_name = _short_name(
                    str((reuse_match or {}).get("storyline_name") or reuse_key), self.max_name_chars
                )
            else:
                storyline_name = self._build_storyline_name([clusters[idx] for idx in core_nodes])
                # Content-derived key (Issue #4): same anchor set → same key
                # across days; different topic → different hash. Replaces the
                # per-run ``storyline-{N}`` counter that collided across days.
                anchor_titles = [
                    str(profiles[idx]["lead_title"])
                    for idx in core_nodes[:3]
                ]
                storyline_key = f"{_slugify(storyline_name)}-{_content_hash(*anchor_titles)}"
                storyline_counter += 1

            component_edges = [
                edge
                for edge in edges
                if int(edge["left_index"]) in component_nodes and int(edge["right_index"]) in component_nodes
            ]
            confidence = (
                sum(float(edge["confidence"]) for edge in component_edges) / len(component_edges)
                if component_edges
                else 0.6
            )
            icon_key = self._pick_icon_key([clusters[idx] for idx in component_nodes])
            anchor_labels = [
                _short_name(str(profiles[idx]["lead_title"]), self.max_name_chars)
                for idx in core_nodes[:3]
            ]
            for idx in sorted(component_nodes):
                self._apply_storyline(
                    clusters[idx],
                    storyline_key=storyline_key,
                    storyline_name=storyline_name,
                    storyline_role=roles[idx],
                    storyline_confidence=confidence,
                    icon_key=icon_key,
                    membership_status=roles[idx],
                    anchor_labels=anchor_labels,
                )

    def _build_storyline_name(self, anchor_clusters: list[ArticleCluster]) -> str:
        name_storyline = getattr(self.summarizer, "name_storyline", None)
        if callable(name_storyline) and len(anchor_clusters) > 1:
            generated = name_storyline(anchor_clusters)
            if generated:
                return _short_name(generated, self.max_name_chars)
        lead = anchor_clusters[0]
        candidate = lead.impact.short_topic_name if lead.impact and lead.impact.short_topic_name else None
        if candidate:
            return _short_name(candidate, self.max_name_chars)
        if lead.articles:
            return _short_name(lead.articles[0].title, self.max_name_chars)
        return _short_name(lead.topic_category, self.max_name_chars)

    def _pick_icon_key(self, clusters: list[ArticleCluster]) -> str:
        counts: dict[str, int] = defaultdict(int)
        for cluster in clusters:
            icon = cluster.impact.topic_icon_key if cluster.impact else None
            if icon:
                counts[icon] += 1
        if counts:
            best = max(counts, key=lambda key: counts[key])
            if best in self.icon_allowlist:
                return best
        return self.icon_allowlist[0] if self.icon_allowlist else _DEFAULT_ICON

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
        # Compatibility with the renderer/report contract
        cluster.macro_topic_key = storyline_key
        cluster.macro_topic_name = storyline_name
        cluster.macro_topic_icon_key = icon_key

    def _apply_singleton(
        self,
        cluster: ArticleCluster,
        profile: dict[str, object],
        history_match: dict[str, object] | None,
    ) -> None:
        if history_match and history_match.get("storyline_key"):
            storyline_key = str(history_match["storyline_key"])
            storyline_name = _short_name(
                str(history_match.get("storyline_name") or storyline_key), self.max_name_chars
            )
            confidence = float(history_match.get("score", 0.0))
        else:
            # Content-derived key (Issue #4): per-run ``single-{N}`` collided
            # across days (single-8 meant 4 different topics in one week).
            lead_title = str(profile.get("lead_title") or "")
            storyline_key = f"single-{_content_hash(lead_title)}"
            storyline_name = _short_name(str(profile["lead_title"]), self.max_name_chars)
            confidence = 0.0
        self._apply_storyline(
            cluster,
            storyline_key=storyline_key,
            storyline_name=storyline_name,
            storyline_role="none",
            storyline_confidence=confidence,
            icon_key=self._pick_icon_key([cluster]),
            membership_status="none",
            anchor_labels=[],
        )
