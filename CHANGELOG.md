# Changelog

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
