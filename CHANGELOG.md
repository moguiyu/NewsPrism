# Changelog

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
