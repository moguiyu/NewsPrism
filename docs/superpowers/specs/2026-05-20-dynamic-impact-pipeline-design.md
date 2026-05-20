# Dynamic Impact Pipeline — Design Spec
**Date:** 2026-05-20  
**Branch:** `feature/dynamic-impact` (worktree)  
**Status:** Approved for implementation planning

---

## Context

NewsPrism currently selects and categorises news using a static `config/keywords.txt` (428 lines, 27 categories). Articles from portal-tier sources with no keyword match are dropped; all others are tagged with keyword-derived topic labels used for clustering and rendering.

This design replaces that static system with a dynamic, LLM-driven impact pipeline. The goal is to surface news that is **most impactful to humankind** — across geopolitical, economic, scientific, and humanitarian dimensions — from the most significant nations, companies, domains, and technologies at any given moment, without a hardcoded topic list.

---

## What Changes vs. What Stays

### Removed
- `config/keywords.txt` — static keyword file
- `newsprism/service/filter.py` — `TopicTagger` class, portal-tier keyword gate
- `topic_category` populated by keywords in the collection pipeline

### Added
- `newsprism/service/impact.py` — `ImpactJudge`: LLM-based impact scoring per cluster
- `newsprism/service/discovery.py` — `SourceDiscoverer`: weekly LLM-driven RSS feed discovery
- `scripts/evaluate_pipeline.py` — A/B evaluation harness comparing two clustering approaches
- `config/sources.yaml` — approved dynamic sources (git-tracked, separate from config.yaml seed)
- `docker-compose.eval.yml` — parallel eval deployment on port 8081
- `scripts/deploy-eval.sh` — deploys worktree to `/vol1/1000/Docker/newsprism-eval/` on fnOS

### Unchanged
- `service/collector.py` — collection logic and 6-path fetch cascade
- `service/dedup.py` — semantic deduplication
- `service/clusterer.py` — mpnet cosine-similarity clustering (used as Option B pre-pass)
- `service/quality.py` — claim diversity and source reliability gate
- `service/summarizer.py` — LLM batch summarisation
- `runtime/renderer.py` — report rendering (reads `impact_label` instead of `topic_category`)
- `runtime/scheduler.py` — orchestration (modified to insert `ImpactJudge` step, remove `TopicTagger`)

---

## Architecture

### Collection Phase (unchanged flow, keyword gate removed)

```
40+ Sources (seed) + sources.yaml (dynamic approved)
    → Collector (6-path cascade)
    → Deduplicator (mpnet semantic)
    → SQLite DB
```

Portal-tier articles are no longer dropped by keyword matching. All articles reach the DB. Impact scoring at cluster level is the noise filter.

### Publishing Phase (new ImpactJudge step)

```
DB Load (last 24 h)
    → Clusterer (mpnet cosine-similarity, no topic field required)
    → ImpactJudge  ← NEW: LLM scores 4 dimensions, drops low-impact clusters
    → QualityGate (unchanged)
    → Summarizer (unchanged, reads impact_label for cluster heading)
    → Renderer → HTML Report
```

### Weekly Source Discovery (new background job)

```
Gap analysis (LLM reads approved source list + last week's cluster_impact rows)
    → Tavily search for candidate sites per gap
    → LLM extracts RSS feed URL from candidate homepages
    → Feed validation (live RSS, ≥5 recent articles, language detected)
    → pending_sources table
    → CLI review: [a]pprove / [r]eject / [s]kip
    → config/sources.yaml (approved feeds, picked up by collector)
```

---

## Component: ImpactJudge (`service/impact.py`)

**Input:** `list[ArticleCluster]` (from Clusterer)  
**Output:** same list, each cluster annotated; clusters below threshold dropped

**Single LLM call per publish run** (batch pattern, same as Summarizer).  
Prompt gives the LLM all cluster summaries (sources + headlines) and asks it to:
1. Score each cluster on 4 dimensions (0–10 each), normalised to 0.0–1.0 composite
2. Assign a human-readable `impact_label` (e.g. `"Gaza Ceasefire Talks"`, not `"Geopolitics"`)
3. Write one `impact_rationale` sentence
4. List `affected_countries` (ISO-3166 alpha-2 codes)
5. List `affected_domains` from controlled vocabulary in `config.yaml → impact.domain_tags`

**Pydantic output model:**
```python
class ClusterImpact(BaseModel):
    index: int
    impact_score: float           # 0.0–1.0 composite
    geopolitical: float
    economic: float
    scientific: float
    humanitarian: float
    impact_label: str
    impact_rationale: str
    affected_countries: list[str] # e.g. ["US", "CN"]
    affected_domains: list[str]   # e.g. ["semiconductors", "trade-war"]

class ImpactBatchResponse(BaseModel):
    clusters: list[ClusterImpact]
```

**Threshold & cap:**
- `impact.min_score` (default `0.35`) — clusters below this are dropped
- `impact.max_clusters` (default `20`) — top N by score are kept

**Model config:** reads `IMPACT_LLM_MODEL` / `IMPACT_LLM_BASE_URL` / `IMPACT_LLM_API_KEY` from env; falls back to main `LITELLM_*` vars. All calls via LiteLLM.

**Rollback flag (Phase 1 only):** `impact.enabled: false` in `config.yaml` bypasses `ImpactJudge` entirely and falls back to the keyword-derived `topic_category` field still populated by `TopicTagger` during the eval phase. This flag has no meaningful fallback after Phase 2 cutover (once `filter.py` is deleted). Post-cutover rollback is via `git revert` + redeploy.

---

## Component: SourceDiscoverer (`service/discovery.py`)

**Schedule:** Sundays 06:00 Warsaw via APScheduler. Also: `python -m newsprism discover-sources`

**Phase 1 — Gap analysis:**  
LLM receives current approved source list (names, languages, country codes) + last week's `cluster_impact` aggregated by `affected_countries` and `affected_domains`. Returns coverage gaps with priority (e.g. `{"gap": "Sub-Saharan Africa economics", "priority": "high"}`).

**Phase 2 — Feed search & validation:**  
For each gap: Tavily search (`TAVILY_API_KEY` in `.env`) finds candidate news sites → Tavily fetches each homepage content → LLM reads the fetched content and extracts the RSS/Atom feed URL → validate (live feed, ≥5 articles in last 7 days, language detected). Validated candidates written to `pending_sources` with status `pending`.

**Approval CLI:** `python -m newsprism review-sources`  
Lists pending feeds with gap rationale and sample headlines. Prompts `[a]pprove / [r]eject / [s]kip`. Approved feeds written to `config/sources.yaml`.

**Source stores:**
- `config/config.yaml` — permanent seed (current 40+ sources, never auto-modified)
- `config/sources.yaml` — dynamic approved sources (git-tracked, auto-updated by CLI)
- Collector merges both at runtime

---

## Evaluation Harness (`scripts/evaluate_pipeline.py`)

**Run on demand:** `python scripts/evaluate_pipeline.py [--date YYYY-MM-DD]`

**Steps:**
1. Load articles from DB (last 24 h, same window as publisher)
2. **Option A** — single large-context LLM call: all article headlines + leads → LLM groups events, scores impact, labels topics simultaneously (`EVAL_LLM_MODEL` env var, defaults to `IMPACT_LLM_MODEL`)
3. **Option B** — mpnet clustering → `ImpactJudge` (same as production path)
4. **Judge LLM call** — receives both ranked lists; evaluates global representativeness, coverage of significant events, what each approach uniquely surfaced or buried; returns verdict with reasoning
5. Write `output/eval-YYYY-MM-DD.html` — side-by-side ranked lists, diff panel, judge verdict, call cost metadata

---

## Data Model Changes (`repo/db.py`)

### New table: `cluster_impact`
```sql
CREATE TABLE cluster_impact (
    cluster_id          TEXT PRIMARY KEY,
    run_date            TEXT NOT NULL,
    impact_score        REAL NOT NULL,
    geopolitical        REAL,
    economic            REAL,
    scientific          REAL,
    humanitarian        REAL,
    impact_label        TEXT NOT NULL,
    impact_rationale    TEXT,
    affected_countries  TEXT,   -- JSON array: ["US", "CN", "TW"]
    affected_domains    TEXT,   -- JSON array: ["semiconductors", "trade-war"]
    pipeline            TEXT DEFAULT 'b',  -- 'a' or 'b' for eval runs
    created_at          TEXT DEFAULT (datetime('now'))
);
```

### New table: `pending_sources`
```sql
CREATE TABLE pending_sources (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    feed_url         TEXT NOT NULL UNIQUE,
    site_name        TEXT,
    language         TEXT,
    country_code     TEXT,   -- ISO-3166 alpha-2
    affected_domains TEXT,   -- JSON array of domain tags
    gap_rationale    TEXT,
    sample_headlines TEXT,   -- JSON array
    status           TEXT DEFAULT 'pending',  -- pending | approved | rejected
    discovered_at    TEXT DEFAULT (datetime('now')),
    reviewed_at      TEXT
);
```

### Existing tables
- `articles.topics` — kept as nullable during eval phase; removed in final cutover migration
- No other existing tables modified

### New config file: `config/sources.yaml`
```yaml
sources:
  - name: "Example News"
    feed_url: "https://example.com/rss"
    language: "en"
    tier: "editorial"
    country_code: "NG"
    approved_at: "2026-05-25"
```

### New `config.yaml` section
```yaml
impact:
  enabled: true
  min_score: 0.35
  max_clusters: 20
  domain_tags:
    - AI
    - semiconductors
    - energy
    - space
    - biotech
    - climate-tech
    - trade
    - markets
    - supply-chain
    - finance
    - conflict
    - diplomacy
    - sanctions
    - elections
    - disaster
    - migration
    - public-health
    - rights
    - trade-war
```

### New `.env` vars
```
# Model IDs filled in .env on fnOS — intentionally not hardcoded here
IMPACT_LLM_MODEL=openai/<model-id>     # large-context model for ImpactJudge (e.g. deepseek-v4-flash when available)
IMPACT_LLM_BASE_URL=https://api.deepseek.com/v1
IMPACT_LLM_API_KEY=<key>               # falls back to LITELLM_API_KEY if unset
EVAL_LLM_MODEL=openai/<model-id>       # Option A single-pass model (defaults to IMPACT_LLM_MODEL if unset)
TAVILY_API_KEY=<key>                   # for source discovery web search
```

---

## Migration Path

### Phase 1 — Parallel eval deployment (weeks 1–3+)

1. Develop on `feature/dynamic-impact` branch via git worktree at `../17_NewsPrism-eval`
2. `docker-compose.eval.yml` defines `newsprism-eval` (scheduler) + `newsprism-eval-web` (port 8081)
3. Deploy to `/vol1/1000/Docker/newsprism-eval/` via `scripts/deploy-eval.sh`
4. Eval container runs new pipeline independently; production on port 8080 untouched
5. Run `evaluate_pipeline.py` periodically against eval DB to compare A vs B
6. Tune `impact.min_score`, `impact.max_clusters`, domain tags, prompt wording in worktree
7. Run `discover-sources` and `review-sources` to validate the source discovery flow

### Phase 2 — Cutover (after evaluation confirms quality)

1. Merge `feature/dynamic-impact` → `main`
2. DB migration on production SQLite: add `cluster_impact`, `pending_sources` tables
3. Update production `docker-compose.dev.yml` to match eval compose
4. `bash scripts/deploy.sh --build`
5. Tear down `newsprism-eval` container; remove `/vol1/1000/Docker/newsprism-eval/`
6. Remove `articles.topics` column in follow-up migration once stable
7. Delete `config/keywords.txt` and `newsprism/service/filter.py`

### Rollback
- **Before cutover:** zero risk — production never changed
- **After cutover:** `git revert` the merge commit + `bash scripts/deploy.sh --build`. The `impact.enabled: false` flag has no meaningful fallback post-cutover since `filter.py` is deleted by then.

---

## Files Created / Modified Summary

| Action | Path |
|--------|------|
| CREATE | `newsprism/service/impact.py` |
| CREATE | `newsprism/service/discovery.py` |
| CREATE | `scripts/evaluate_pipeline.py` |
| CREATE | `scripts/deploy-eval.sh` |
| CREATE | `docker-compose.eval.yml` |
| CREATE | `config/sources.yaml` |
| MODIFY | `newsprism/repo/db.py` — add 2 new tables + migration |
| MODIFY | `newsprism/runtime/scheduler.py` — insert ImpactJudge step, remove TopicTagger |
| MODIFY | `newsprism/runtime/renderer.py` — read `impact_label` instead of `topic_category` |
| MODIFY | `newsprism/config.py` — load `impact` config section + `sources.yaml` |
| MODIFY | `config/config.yaml` — add `impact` section |
| DELETE | `config/keywords.txt` *(Phase 2 only)* |
| DELETE | `newsprism/service/filter.py` *(Phase 2 only)* |

---

## Verification

### Phase 1 eval
```bash
# Deploy eval container
bash scripts/deploy-eval.sh

# Trigger one-off collect + publish on eval container
ssh -i ~/.ssh/fnoskey -p 123 aiagent@192.168.10.5 \
  "cd /vol1/1000/Docker/newsprism-eval && \
   docker exec newsprism-eval python -m newsprism collect && \
   docker exec newsprism-eval python -m newsprism publish"

# Check eval report
open http://192.168.10.5:8081

# Run A/B evaluation harness
docker exec newsprism-eval python scripts/evaluate_pipeline.py
# inspect output/eval-YYYY-MM-DD.html
```

### Unit tests (before cutover)
```bash
.venv/bin/pytest tests/ -v
```
