"""Shared data types for the NewsPrism pipeline.

Dependency rule: types.py imports NOTHING from the project.
All other layers (config, repo, service, runtime) import from here.

Layer order (strictest dependency rule):
  types → config → repo → service → runtime
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


# ─── COLLECTION ───────────────────────────────────────────────────────────────

@dataclass
class RawArticle:
    """Article as returned by a collector before DB persistence."""
    url: str
    title: str
    source_name: str
    published_at: datetime
    content: str


# ─── STORAGE ──────────────────────────────────────────────────────────────────

@dataclass
class Article:
    """Article stored in SQLite; enriched progressively through the pipeline."""
    url: str
    title: str
    source_name: str
    published_at: datetime
    content: str
    topics: list[str] = field(default_factory=list)
    embedding: list[float] | None = None
    id: int | None = None
    clustered: bool = False
    # Active Seeker metadata
    is_searched: bool = False           # True if dynamically searched for missing perspective
    search_region: str | None = None    # ISO alpha-2 code of searched region (e.g., "jp", "cn")
    source_kind: str = "news"           # "news" | "official_web" | "official_social"
    platform: str | None = None         # "x" | "youtube" | None
    account_id: str | None = None       # platform-specific account/channel identifier
    is_official_source: bool = False    # True for curated official fallback sources
    origin_region: str | None = None    # editorial/source region represented by the article
    searched_provider: str | None = None  # Provider stage that produced this searched article


@dataclass
class SearchRequestEvent:
    """One outbound search/social API request emitted by Active Seeker."""
    provider: str
    request_type: str
    target_region: str | None = None
    query: str | None = None
    account_id: str | None = None
    http_status: int | None = None
    result_count: int | None = None
    accepted_count: int | None = None
    rejection_reason: str | None = None
    rejection_count: int | None = None
    duration_ms: int | None = None
    estimated_cost_usd: float | None = None
    created_at: datetime | None = None


@dataclass
class Cluster:
    """Persisted cluster record (SQLite). Separate from in-memory ArticleCluster."""
    topic_category: str
    article_ids: list[int]
    summary: str
    perspectives: dict[str, str]   # source_name → perspective text
    report_date: str               # YYYY-MM-DD
    id: int | None = None
    # Freshness tracking for cross-day deduplication
    freshness_state: str = "new"   # "new" | "developing" | "stale"
    continues_cluster_id: int | None = None  # Links to previous day's cluster if developing/stale
    storyline_key: str | None = None
    storyline_name: str | None = None
    storyline_role: str = "none"   # "core" | "spillover" | "none"
    storyline_confidence: float = 0.0
    storyline_state: str = "emerging"
    quality_status: str = "unknown"
    quality_score: float = 0.0


# ─── QUALITY ─────────────────────────────────────────────────────────────────

@dataclass
class Claim:
    """A factual claim extracted from an event cluster."""
    text: str
    claim_type: str = "event"       # event|number|quote|causal|forecast|correction|context
    importance: float = 0.5
    source_names: list[str] = field(default_factory=list)
    claim_id: str | None = None


@dataclass
class Evidence:
    """A source-level evidence judgment for a claim."""
    claim_id: str
    source_name: str
    stance: str = "uncovered"       # supports|refutes|uncovered
    excerpt: str = ""
    confidence: float = 0.0


@dataclass
class QualityDecision:
    """Gate result used to decide whether and how a cluster can proceed."""
    status: str = "publishable"     # publishable|needs_review|suppress|seek_more_evidence
    reason: str = ""
    needs_more_evidence: bool = False
    summary_constraints: list[str] = field(default_factory=list)


@dataclass
class ClusterQualityReport:
    """Quality assessment attached to a cluster and persisted with the report."""
    cluster_key: str
    claims: list[Claim] = field(default_factory=list)
    evidence: list[Evidence] = field(default_factory=list)
    fact_coverage: float = 0.0
    source_diversity: float = 0.0
    reliability_score: float = 0.0
    bias_risk: float = 0.0
    overall_score: float = 0.0
    status: str = "publishable"
    flags: list[str] = field(default_factory=list)
    contested_claims: list[str] = field(default_factory=list)
    confirmed_claims: list[str] = field(default_factory=list)
    evidence_summary: str = ""
    decision: QualityDecision = field(default_factory=QualityDecision)
    created_at: datetime | None = None


@dataclass
class StorylineEvent:
    """A persisted event in a storyline timeline."""
    storyline_key: str
    event_date: str
    title: str
    state: str = "emerging"
    summary: str = ""
    cluster_id: int | None = None
    quality_score: float = 0.0
    event_type: str = "update"


# ─── PROCESSING ───────────────────────────────────────────────────────────────

@dataclass
class ArticleCluster:
    """In-memory cluster produced by the Clusterer service."""
    topic_category: str
    articles: list[Article]
    sources: list[str] = field(default_factory=list)
    is_hot_topic: bool = False
    organic_unique_regions: int = 0
    organic_unique_sources: int = 0
    macro_topic_key: str | None = None
    macro_topic_name: str | None = None
    macro_topic_icon_key: str | None = None
    macro_topic_member_count: int = 0
    storyline_key: str | None = None
    storyline_name: str | None = None
    storyline_role: str = "none"
    storyline_confidence: float = 0.0
    storyline_state: str = "emerging"
    storyline_timeline: list[StorylineEvent] = field(default_factory=list)
    storyline_membership_status: str = "none"  # "core" | "spillover" | "excluded_to_main" | "none"
    storyline_anchor_labels: list[str] = field(default_factory=list)
    quality_report: ClusterQualityReport | None = None
    quality_decision: QualityDecision | None = None

    def __post_init__(self) -> None:
        self.sources = list(dict.fromkeys(a.source_name for a in self.articles))

    @property
    def is_multi_source(self) -> bool:
        return len(self.sources) >= 2


@dataclass
class ClusterSummary:
    """Output of the Summarizer service — cluster + LLM-generated text."""
    cluster: ArticleCluster
    summary: str                    # full formatted text (headline + factual body)
    perspectives: dict[str, str] = field(default_factory=dict)   # legacy shim: source_name → perspective text
    summary_en: str | None = None
    grouped_perspectives: list["PerspectiveGroup"] = field(default_factory=list)
    grouped_perspectives_en: list["PerspectiveGroup"] = field(default_factory=list)
    short_topic_name: str | None = None
    short_topic_name_en: str | None = None
    topic_icon_key: str | None = None
    # Freshness metadata (set after freshness evaluation)
    freshness_state: str = "new"    # "new" | "developing" | "stale"
    continues_cluster_id: int | None = None
    is_hot_topic: bool = False
    organic_unique_regions: int = 0
    organic_unique_sources: int = 0
    macro_topic_key: str | None = None
    macro_topic_name: str | None = None
    macro_topic_name_en: str | None = None
    macro_topic_icon_key: str | None = None
    macro_topic_member_count: int = 0
    storyline_key: str | None = None
    storyline_name: str | None = None
    storyline_name_en: str | None = None
    storyline_role: str = "none"
    storyline_confidence: float = 0.0
    storyline_state: str = "emerging"
    storyline_timeline: list[StorylineEvent] = field(default_factory=list)
    storyline_membership_status: str = "none"
    storyline_anchor_labels: list[str] = field(default_factory=list)
    quality_report: ClusterQualityReport | None = None
    quality_status: str = "unknown"
    quality_score: float = 0.0
    quality_flags: list[str] = field(default_factory=list)
    confirmed_claims: list[str] = field(default_factory=list)
    contested_claims: list[str] = field(default_factory=list)
    evidence_summary: str = ""


@dataclass
class PerspectiveGroup:
    """One distinct perspective shared by one or more sources."""
    sources: list[str]
    perspective: str


# ─── CONVERSION ───────────────────────────────────────────────────────────────

def raw_to_article(raw: RawArticle) -> Article:
    """Convert a collected RawArticle into a DB-ready Article."""
    return Article(
        url=raw.url,
        title=raw.title,
        source_name=raw.source_name,
        published_at=raw.published_at,
        content=raw.content,
    )


def raw_to_articles(raws: list[RawArticle]) -> list[Article]:
    return [raw_to_article(r) for r in raws]
