# Changelog

## v0.5.3 - 2026-07-22

### Added

- **Tavily key rotation** — the Active Seeker now reads `TAVILY_API_KEYS` (a
  comma-separated list) and rotates to the next key on HTTP 401/403 within the
  same call. The active key is pinned for subsequent calls, and a single
  warning fires only when all keys are exhausted. Fixes a 37-day incident
  (2026-06-15 → 2026-07-21) where one invalid key silently returned 0 results
  for every regional-perspective search.
- **Inline missing-perspective placeholders** — when a region is targeted by
  the seeker but the search fails (auth, no results, or all rejected), a flat
  `⚠️` placeholder is rendered in the cluster's source list with the country
  flag, a short bilingual failure label, and the failure detail in a tooltip.
  Never silent, never counts toward `is_multi_source`.
- **`same_conflict_different_event` storyline relation** — a fourth storyline
  relation added to the LLM classifier so that distinct daily incidents of the
  same ongoing conflict (Russia-Ukraine, Iran-US, Israel-Palestine, China-US
  trade) glue into one storyline family with role `spillover`. Components glued
  by this edge bypass the strict 0.60 coherence gate (different daily incidents
  have low centroid similarity by design); a `storyline_conflict_coherence_min`
  config knob exposes the relaxed bar.
- **Content-derived storyline keys** — `storyline-{slug}-{hash8}` and
  `single-{hash8}` replace the per-run counter keys (`storyline-1`, `single-26`)
  that collided across days. Same anchor set → same key across runs; different
  topic → different hash.
- **Storyline name coherence on history reuse** — when a historical
  `storyline_key` is reused for a topic that has drifted (≥3 days old), the
  name is regenerated from today's anchors while the key is kept for
  cross-day continuity. Adjacent history (≤2 days) keeps the name
  unconditionally.
- **`ownership_suppressed` article column** — the per-article ownership-gate
  decision is now persisted (was in-memory only), so the portal/audit can show
  which articles were state-media-suppressed, not just the aggregate
  cluster-level verdict. Race-safe migration for concurrent container startup.
- **Clickable storyline title** — the focus-map title is now a button that
  enters the storyline tab on click (flat hover underline, no card/lift).
- **Shared-storyline tag** — when 2+ main-lane clusters share a storyline key
  (e.g. a family that didn't reach the tab cap), a flat uppercase tag is
  rendered above each card so the connection is visible.
- `docs/generated/db-schema.md` and `docs/design-docs/decisions.md` added per
  AGENTS.md (schema reference + decision log).

### Changed

- **Ownership gate no longer hard-suppresses small high-impact clusters** —
  the `ownership_suppressed_all` rule now requires ≥
  `gate_suppress_min_cluster_size` (default 4) articles AND composite <
  `review_floor` (0.34). Smaller or high-impact clusters demote to
  `needs_review` with flag `ownership_all_blocked_review` so a human editor
  sees the event (was: UK PM resignation at composite 0.587 vanished entirely).
- **Hot topic tab admission lowered to ≥2 members** — `min_items_per_topic`
  lowered from 5 → 3; every storyline family with ≥2 members claims a tab,
  capped at `max_topic_tabs: 3` per day (was: most families spilled to the
  main lane as standalone cards).
- **Within-family members preserved** — `resolve_display_duplicates` now
  skips pairs that belong to the same storyline family. The resolver grouped
  them intentionally; collapsing them (e.g. 3 UK-PM articles → 1) defeated the
  tab.
- **Seeker freshness gate trusts Tavily's `days` bound** — when
  `published_date` is missing (Tavily returns `None` for most outlets), the
  gate now parses the date from the URL path, or trusts the query's `days:3`
  bound. Previously 100% of fresh results were rejected as stale.
- Seeker acceptance telemetry always records (was empty for 2+ months because
  it only fired on rejections).
- `storyline_history_similarity_threshold` raised 0.48 → 0.55.

### Fixed

- All five root causes from the 2026-07-21 systematic review: missing
  perspective searches, conflict-news spillover into the main lane,
  ownership-gate over-suppression, storyline-key contamination, and the
  article-level observability gap.

## v0.5.2 - 2026-07-04

### Added

- State Media Matrix ownership gate: every source is classified into one of
  seven ownership tiers (from `independent_public` to `state_controlled_block`)
  in `config.yaml`. The impact-evaluation LLM classifies each cluster's
  `target_region` and `is_home_affairs`; a per-article gate then suppresses
  state-controlled / captured outlets from covering **another country's** 内政
  (domestic governance), preserves own-country 内政 as the official perspective,
  and applies a configurable weight penalty to constrained / low-evidence
  sources. Independent and public-service media retain full standing on any
  country's affairs. All failure paths safe-degrade to no block.
- 内政 is calibrated to domestic governance only — natural disasters, casualties,
  diplomacy, trade, and war are explicitly excluded (negative prompt examples
  added after a Venezuela-earthquake over-block).
- Ownership gate verdicts are visible in the admin portal 单日审查 view as a
  内政 column (禁 / 审 / 放 / —) with target region and blocked sources in the
  tooltip, backed by a new `gate` JSON column on `cluster_evaluations`.
- Read-only audit script (`python -m newsprism.runtime.audit_ownership`) that
  reports cross-border 内政 coverage by source, target region, and ownership tier.

### Changed

- All 51 sources stamped with `ownership` + `ownership_detail` corroboration
  notes; `config/editorial-values.yaml` gains an `ownership.weight_multipliers`
  key for the constrained / low-evidence tiers.

## v0.5.1 - 2026-06-21

### Added

- Admin quality portal (`newsprism portal`): local-only FastAPI app to inspect
  selection quality (day inspector, category×dimension / subject-country×category
  / source×subject matrices, trends, source review) and capture structured
  feedback (verdict, per-dimension corrections, wrong-category, promote) that
  feeds weekly calibration. Adds a `subject_regions` field to the impact
  evaluation and a `feedback_corrections` table. Runs loopback-only via the
  `newsprism-portal` compose profile, reached over an SSH tunnel.
- Portal now reachable at `https://admin.grayzhang.com` via Cloudflare Tunnel
  + Cloudflare Access (Email OTP). New `PORTAL_REQUIRE_CF_ACCESS` env var
  (default `true`) adds a defensive header check in the portal. SQLite and the
  collection pipeline are unchanged.
- Metabase analytics platform (`newsprism-metabase`): self-hosted Metabase
  reading `data/newsprism.db` read-only, at `metabase.grayzhang.com` via
  Cloudflare Tunnel + Access. Dashboards bootstrapped via REST API. No data
  layer changes.

### Changed

- Public report lanes are now limited to main, hot topics, and `今日好消息` /
  Good News. Compatibility `focus_storylines` inputs no longer render public
  sections, count toward public totals, or publish to Telegram.
- Public categories are normalized to six reader-facing buckets: World,
  Business, Technology, Science & Health, Society, and Culture & Sports, while
  legacy Chinese category values continue to normalize correctly.
- Chinese report and Telegram category labels now render locale-specific names
  while preserving the stable English category keys in JSON/filtering.
- Hot-topic display names now reject source-language or stale historical labels;
  the Russia-Ukraine refinery drift case is repaired to the broader
  `俄乌军事升级` / `Russia-Ukraine military escalation` label.
- Small non-hot storyline groups now return to the main lane and obey the same
  impact ranking and normalized category diversity caps as other main stories.

### Removed

- Public `focus_storyline` report surface: HTML sections, JSON payload, CSS/JS,
  runtime counters, and Telegram publish wiring.
- Confusing reader-facing labels including `Core storyline`, `Direct spillover`,
  routine `Evidence checked`, and routine multi-source confirmation previews.

## v0.5.0 - 2026-06-14

Selection rebuilt from keyword matching to LLM multi-dimensional impact
evaluation, with a self-evolving feedback loop. The keyword paradigm is fully
removed.

### Added

- `service/impact.py`: one batched LLM call scores every candidate cluster on
  six 0–10 dimensions (scope, severity, novelty, actor_influence,
  decision_relevance, feelgood) plus a locally-computed cross-source `signal`.
  A calibrated composite drives selection, status, the `今日正能量` lane, the
  display category, and seeker targeting. LLM failure degrades to a
  deterministic signal-only ranking.
- Self-evolution loop (`service/calibrate.py`, `runtime/feedback.py`): editor
  👍/👎 via Telegram inline buttons (polled hourly) or `newsprism feedback`
  → weekly bounded weight nudges → an `editorial_policy` memo distilled by LLM
  and injected into subsequent impact prompts. CLI: `newsprism calibrate
  run|show|reset`, `newsprism feedback add|list|poll`.
- New SQLite tables: `cluster_evaluations` (per-pick scores + rationale audit
  trail), `editorial_feedback`, `calibration_weights`, `calibration_log`,
  `editorial_policy`. Weekly retention prune of unclustered articles.
- `service/history.py`: merged freshness + keyword-free storyline grouping
  (union-find over LLM relation edges); `service/embeddings.py` single shared
  sentence-transformers loader.

### Changed

- Selection is calibrated impact, not coverage breadth. `今日正能量` is the
  feelgood dimension of the same evaluation. Display categories come from the
  LLM, not a keyword→section map.
- Seeker slimmed (~2100 → ~430 lines): Tavily-only, 12 major regions,
  impact-status-triggered. LLM calls per publish dropped from ~60–100 to ~8–12.
- Sources: AP News re-enabled via the live RSSHub; Reuters stays disabled.

### Removed

- The entire keyword paradigm: `keywords.txt`, `feelgood_keywords.yaml`,
  `filter.py`, `feelgood_scorer.py`, the claim/evidence half of `quality.py`,
  storyline signal-keyword tables, the event-signature entity engines, and
  `clustering.topic_equivalence`. Net `newsprism/` ~13.2k → ~9.4k lines.

### Fixed

- Storyline families no longer chain unrelated events. A coherence pass gates
  every multi-node family on mean pairwise centroid cosine
  (`output.hot_topics.storyline_coherence_min`, default 0.60), and a final
  pass over assigned storyline keys detaches members glued on by a stale
  historical key. A 14-member "中东局势升级" family (mean cosine 0.272) now
  resolves to its genuine 2-member core.
- The `今日正能量` lane is exclusive: a story claimed for it is removed from the
  main/family lanes, so it can no longer be suppressed out of every lane by
  self-collision in display dedup.
- Display dedup similarity lowered 0.80 → 0.75 so cross-language coverage of the
  same event (which sits below same-language pairs under multilingual mpnet)
  merges — e.g. a zh/en NBA-final pair at cosine 0.778.

## v0.4.1 - 2026-06-12

Editorial planning and deployment maintenance patch.

### Fixed

- Small focus storylines now require event-coherent members instead of folding unrelated stories by shared storyline key alone.
- Iran-war energy and inflation spillover stories are absorbed into the existing focus storyline instead of occupying the main feed separately.
- `今日正能量` now blocks product roundups, accessories buying guides, and AI model/API launch stories in both local scoring and final filtering.

### Changed

- fnOS source-build deployment now prunes old dangling Docker images after a successful rebuild.

## v0.4.0 - 2026-05-19

LLM-driven clustering and batch summarisation release.

### Added

- `LLMClusterer`: groups articles by real-world event identity via a single LLM call, with automatic fallback to the embedding-based clusterer on failure or sparse output. Controlled by `clustering.use_llm_clustering` in `config.yaml` (default: `true`).
- `Summarizer.summarize_all_batch` / `_batch_summarize`: builds a single multi-cluster prompt for all clusters, parses a `BatchSummaryResponse`, and falls back to per-cluster calls for any missing index or on total failure.

### Fixed

- LLM clusterer now strips ` ``` `-fenced JSON before parsing — prevents `json.loads` failures when models wrap their response in a markdown code block.
- Removed dynamic `label` attribute assignment on `ArticleCluster` (not declared on the dataclass); label is now logged at DEBUG level only.

## v0.3.3 - 2026-05-18

Footer date navigation fix and small security hardening.

### Fixed

- Footer "N days ago" links no longer render as inert disabled spans when a past report exists. `_build_day_links` was probing the staging subdir for sibling reports, but past reports live in the production output dir; the availability check now targets the final output dir directly.

### Changed

- Past-day footer links use stable `/p/N/` aliases (`/p/1/`, `/p/2/`, …) backed by rotating symlinks under `output/p/`, instead of `../YYYY-MM-DD/` paths. Rendered HTML no longer exposes the date-keyed on-disk layout.
- Today's redundant footer self-link is dropped; the day selector lists past days only.
- Direct `/YYYY-MM-DD/` URLs remain reachable for back-compat with existing bookmarks and audits.

## v0.3.2 - 2026-05-17

Ukraine coverage and small report polish release.

### Added

- Two Ukrainian news sources to restore Russia/Ukraine editorial balance and reduce dependence on the X/Twitter `active_search` fallback for `ua`:
  - **Kyiv Independent** (en, editorial, weight 0.9) — independent voice, included in `delta_source_names` for the morning pre-publish run.
  - **Ukrinform** (uk, editorial, weight 0.85) — official Ukrainian state news agency, native Ukrainian. Activates the `official_independent_contrast` editorial rule for Ukraine-origin stories.
- Public-project note in the report footer.

### Fixed

- DeepSeek URL host check no longer trips on hosts that contain the substring twice.

### Changed

- Report header controls simplified.

## v0.3.1 - 2026-05-09

### Fixed

- Positive-highlight language no longer drifts when a model response changes perspective grouping; source grouping stays stable and only the affected perspective falls back.

## v0.3.0 - 2026-05-08

Positive lane recovery and local scoring release.

### Added

- Local `FeelgoodScorer` for selecting `今日正能量` stories from existing collected `Article` records without additional LLM calls.
- `config/feelgood_keywords.yaml` with cheerful themes, entity boosts, narrative patterns, and strict blockers for hard-news topics.
- Positive lane metadata in rendered data, including local score, category, source, Chinese reason, and English reason.
- Renderer coverage so English mode remains available when positive summaries provide English fields.
- Tests for positive scoring, source provenance boundaries, bilingual positive tags, scheduler integration, and translation drift handling.

### Changed

- `Scheduler.publish()` now selects positive stories from the same article window used by the main pipeline, without marking those articles clustered or changing main clustering input.
- Default positive lane behavior no longer initializes or calls a dedicated feelgood collector and no longer calls `Summarizer.classify_positive_energy()`.
- English translation now preserves the report when a model response changes perspective grouping; source grouping is kept stable and only the affected perspective text falls back.
- Deployment docs now describe the existing-sources-only positive lane and zero-token local scoring controls.

### Removed

- Runtime use of independent feelgood RSS/scrape sources for `今日正能量`.

### Validation

- Full local test suite: 140 passed.
- fnOS publish smoke run completed for 2026-05-08 with `cluster_count=15`, `positive_story_count=5`, and `english_available=true`.
- Server Telegram environment repaired and validated with `fnosnews_bot` without sending a duplicate report.

## v0.2.0 - 2026-05-06

Major editorial trust and report design release.

### Added

- Cluster quality assessment before summarization, including claim extraction, evidence matching, source reliability scoring, source diversity scoring, and bias risk flags.
- Configurable editorial quality thresholds and reliability weights in `config/editorial-values.yaml`.
- SQLite persistence for quality reports, claims, claim evidence, storylines, and storyline events.
- Storyline lifecycle state and compact timelines for developing stories.
- Renderer payload fields for quality status, quality score, quality flags, confirmed/contested claims, evidence summaries, storyline state, and storyline timelines.
- WIRED-inspired report UI using flat editorial rules, square controls, provenance labels, and paper-like typography.
- Report dark mode with `system`, `light`, and `dark` choices persisted in `localStorage`.
- Audit counters for persisted and rendered quality/storyline signals.

### Changed

- Report rendering now favors editorial whitespace, hairline rules, and source provenance over rounded feed cards.
- The scheduler runs quality precheck before summarization and suppresses clusters below configured quality thresholds.
- Summaries receive quality constraints so single-source, contested, or official-only claims stay attributed.
- Public docs now describe the quality gate, storyline lifecycle, WIRED report surface, and updated SQLite schema.

### Validation

- `tests/test_renderer.py tests/test_scheduler_hot_topics.py tests/test_quality.py`: 52 passed.
- Full test suite: 129 passed.
- fnOS deployment rebuilt successfully and services are running.
