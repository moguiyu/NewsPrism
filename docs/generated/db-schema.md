# Database Schema

**Auto-generated reference.** Do not edit — reflects `newsprism/repo/db.py`.

Database: SQLite at `data/newsprism.db`

---

## Table: articles

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PRIMARY KEY | Auto-increment |
| url | TEXT UNIQUE NOT NULL | Canonical article URL |
| title | TEXT NOT NULL | Article title |
| source_name | TEXT NOT NULL | Source name (e.g. "IT之家") |
| published_at | TEXT NOT NULL | ISO 8601 datetime |
| content | TEXT | Article body (may be truncated) |
| topics | TEXT | JSON array of topic category strings |
| embedding | TEXT | JSON-encoded float array (nullable); stored as text for SQLite portability |
| clustered | INTEGER | 0/1 flag; set to 1 after clustering |
| is_searched | INTEGER NOT NULL DEFAULT 0 | 0/1 flag; set to 1 if article was dynamically searched by Active Seeker |
| search_region | TEXT | ISO 3166-1 alpha-2 code (e.g., "jp", "cn") if is_searched=1; NULL otherwise |
| source_kind | TEXT NOT NULL DEFAULT 'news' | `news`, `official_web`, or `official_social` |
| platform | TEXT | Search/social platform, e.g. `x` or `youtube` |
| account_id | TEXT | Platform-native account or channel identifier for searched/official sources |
| is_official_source | INTEGER NOT NULL DEFAULT 0 | 0/1 flag; true for curated official fallback sources |
| origin_region | TEXT | Region represented by the source/result |
| searched_provider | TEXT | Provider stage that produced a searched article, e.g. `tavily_search` |
| created_at | TEXT | Insert timestamp |

## Table: clusters

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PRIMARY KEY | Auto-increment |
| topic_category | TEXT NOT NULL | Primary topic category |
| article_ids | TEXT NOT NULL | JSON array of article IDs |
| summary | TEXT NOT NULL | AI-generated cluster summary (Chinese) |
| perspectives | TEXT | JSON dict: source_name → perspective snippet |
| report_date | TEXT NOT NULL | YYYY-MM-DD |
| published_telegram | INTEGER | 0/1 flag; set to 1 after Telegram push |
| published_html | INTEGER | 0/1 flag; set to 1 after HTML report generated |
| freshness_state | TEXT NOT NULL DEFAULT 'new' | "new", "developing", or "stale" — freshness classification for cross-day dedup |
| continues_cluster_id | INTEGER | Nullable — links to previous day's cluster if developing/stale |
| storyline_key | TEXT | Stable cross-day storyline identifier for hotspot/storyline continuity |
| storyline_name | TEXT | Human-readable storyline label used for hotspot tabs |
| storyline_role | TEXT NOT NULL DEFAULT 'none' | `core`, `spillover`, or `none` |
| storyline_confidence | REAL NOT NULL DEFAULT 0.0 | Resolver confidence for storyline assignment |
| storyline_state | TEXT NOT NULL DEFAULT 'emerging' | Lifecycle state: `emerging`, `developing`, `turning_point`, `correction`, `stabilized`, or `archived` |
| quality_status | TEXT NOT NULL DEFAULT 'unknown' | Quality gate result: `publishable`, `needs_review`, `seek_more_evidence`, `suppress`, or `unknown` |
| quality_score | REAL NOT NULL DEFAULT 0.0 | Cluster quality score from 0.0 to 1.0 |
| created_at | TEXT | Insert timestamp |

## Table: cluster_quality_reports

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PRIMARY KEY | Auto-increment |
| cluster_id | INTEGER NOT NULL | Associated `clusters.id`; unique per cluster |
| status | TEXT NOT NULL | Gate status: `publishable`, `needs_review`, `seek_more_evidence`, or `suppress` |
| quality_score | REAL NOT NULL | Overall weighted quality score |
| fact_coverage | REAL NOT NULL | Share of extracted claims with supporting evidence |
| source_diversity | REAL NOT NULL | Diversity score across sources and represented regions |
| reliability_score | REAL NOT NULL | Average source reliability score |
| bias_risk | REAL NOT NULL | Risk score from high-risk topic, single-source, official-only, or single-region signals |
| flags | TEXT NOT NULL DEFAULT '[]' | JSON array of quality flags |
| confirmed_claims | TEXT NOT NULL DEFAULT '[]' | JSON array of claims with support |
| contested_claims | TEXT NOT NULL DEFAULT '[]' | JSON array of contested claims |
| evidence_summary | TEXT NOT NULL DEFAULT '' | Human-readable evidence summary |
| decision_status | TEXT NOT NULL DEFAULT 'publishable' | Final gate decision |
| decision_reason | TEXT NOT NULL DEFAULT '' | Reason for the gate decision |
| summary_constraints | TEXT NOT NULL DEFAULT '[]' | JSON array of summarizer constraints derived from the decision |
| created_at | TEXT | Insert timestamp or quality report timestamp |

## Table: cluster_claims

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PRIMARY KEY | Auto-increment |
| cluster_id | INTEGER NOT NULL | Associated `clusters.id` |
| claim_uid | TEXT NOT NULL | Stable claim identifier within the cluster |
| text | TEXT NOT NULL | Extracted claim text |
| claim_type | TEXT NOT NULL | `event`, `number`, `quote`, `causal`, `forecast`, `correction`, or `context` |
| importance | REAL NOT NULL | Claim importance score |
| source_names | TEXT NOT NULL DEFAULT '[]' | JSON array of source names associated with the claim |

## Table: claim_evidence

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PRIMARY KEY | Auto-increment |
| cluster_id | INTEGER NOT NULL | Associated `clusters.id` |
| claim_uid | TEXT NOT NULL | Claim identifier from `cluster_claims` |
| source_name | TEXT NOT NULL | Source used as evidence |
| stance | TEXT NOT NULL | `supports`, `refutes`, or `uncovered` |
| excerpt | TEXT NOT NULL DEFAULT '' | Supporting excerpt when available |
| confidence | REAL NOT NULL | Evidence confidence score |

## Table: storylines

| Column | Type | Notes |
|---|---|---|
| storyline_key | TEXT PRIMARY KEY | Stable storyline identifier |
| storyline_name | TEXT | Human-readable storyline label |
| storyline_state | TEXT NOT NULL DEFAULT 'emerging' | Latest lifecycle state |
| last_report_date | TEXT NOT NULL | Most recent report date for this storyline |
| quality_score | REAL NOT NULL DEFAULT 0.0 | Latest cluster quality score |
| updated_at | TEXT | Update timestamp |

## Table: storyline_events

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PRIMARY KEY | Auto-increment |
| storyline_key | TEXT NOT NULL | Associated storyline identifier |
| cluster_id | INTEGER | Associated cluster when available |
| event_date | TEXT NOT NULL | YYYY-MM-DD |
| title | TEXT NOT NULL | Timeline event title |
| storyline_state | TEXT NOT NULL DEFAULT 'emerging' | Lifecycle state at this event |
| summary | TEXT NOT NULL DEFAULT '' | Event summary |
| quality_score | REAL NOT NULL DEFAULT 0.0 | Event quality score |
| event_type | TEXT NOT NULL DEFAULT 'update' | `history`, `current`, or future event type |
| created_at | TEXT | Insert timestamp |

## Table: search_request_events

| Column | Type | Notes |
|---|---|---|
| id | INTEGER PRIMARY KEY | Auto-increment |
| provider | TEXT NOT NULL | Provider family, e.g. `brightdata_serp`, `tavily_search`, `official_web`, or `x` |
| request_type | TEXT NOT NULL | Request type within the provider, e.g. `search`, `recent_search`, `user_timeline` |
| target_region | TEXT | ISO 3166-1 alpha-2 region sought by Active Seeker |
| query | TEXT | Search query string when applicable |
| account_id | TEXT | Provider account identifier for social/timeline calls |
| http_status | INTEGER | Response status code when available |
| result_count | INTEGER | Raw result count returned by the provider call |
| accepted_count | INTEGER | Accepted result count when known |
| duration_ms | INTEGER | Request latency in milliseconds |
| estimated_cost_usd | REAL | Estimated request cost from config-driven billing metadata |
| created_at | TEXT | Insert timestamp |

## Indexes

- `articles.url` — UNIQUE index (dedup guard)
- `articles.clustered` — for fast unclustered article queries
- `clusters.report_date` — for fast daily report queries
- `cluster_quality_reports.cluster_id` — for cluster quality lookup
- `cluster_claims.cluster_id` — for cluster claim lookup
- `claim_evidence.cluster_id` — for cluster evidence lookup
- `storyline_events.(storyline_key, event_date)` — for timeline lookup
- `search_request_events.created_at` — for time-window cost and volume analysis
- `search_request_events.(provider, request_type)` — for provider-level cost aggregation
