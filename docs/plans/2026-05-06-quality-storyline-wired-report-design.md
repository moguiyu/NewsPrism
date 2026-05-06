# Quality, Storyline, and WIRED Report Update

Date: 2026-05-06
Release: v0.2.0

## Summary

NewsPrism now treats trust signals and presentation as first-class report data. The pipeline assesses cluster quality before summarization, persists claim/evidence and storyline lifecycle metadata, and renders the daily report in a WIRED-inspired editorial system with light, dark, and system theme modes.

## Scope

- Add configurable editorial values in `config/editorial-values.yaml`.
- Add cluster quality assessment in `newsprism/service/quality.py`.
- Add storyline lifecycle state and compact timelines in `newsprism/service/storyline.py`.
- Persist quality reports, claims, evidence, storylines, and storyline events in SQLite.
- Export quality and storyline fields through `newsprism/runtime/renderer.py`.
- Restyle `templates/report-template.html` to match `docs/design-docs/DESIGN.md`.
- Add a persisted report theme selector: `system`, `light`, `dark`.
- Update audit output to include quality and storyline health counters.

## Behavior

Quality precheck runs before summarization:

1. Extract factual claims from titles and article leads.
2. Match article evidence for each claim.
3. Score fact coverage, source diversity, source reliability, and bias risk.
4. Gate the cluster as `publishable`, `needs_review`, `seek_more_evidence`, or `suppress`.
5. Remove suppressed clusters before summarization.
6. Pass quality constraints into the summarizer.
7. Persist the report, claims, evidence, and storyline state after publishing.

Storyline state is assigned from current cluster signals plus recent history:

- `emerging`: no related history yet
- `developing`: related history exists
- `turning_point`: contested, escalatory, or breakthrough signals
- `correction`: correction, denial, clarification, or retraction signals
- `stabilized`: high-quality related story with no quality flags
- `archived`: reserved for future retention policy

## Report Design

The report remains a static HTML artifact. It keeps existing behavior:

- bilingual toggle
- day selector
- category tabs
- hot-topic tabs
- positive highlights
- source links and provenance labels
- expandable perspective panels
- PWA metadata and service worker behavior

The visual system now follows the local WIRED design reference:

- paper-white light mode and inverted newsprint dark mode
- near-black or white ink, muted metadata, gray rules
- one link accent: `#057dbc`
- square controls and hard borders
- hairline dividers and whitespace instead of card surfaces
- no gradients, glows, blur, shadows, rounded story cards, or hover lift

## Data Contract Notes

No source article contract changed. Rendered report payloads now include additional optional fields:

- `quality_status`
- `quality_score`
- `quality_flags`
- `confirmed_claims`
- `contested_claims`
- `evidence_summary`
- `storyline_state`
- `storyline_timeline`

Existing readers should tolerate these as additive fields.

## Test Plan

```bash
.venv/bin/pytest tests/test_renderer.py tests/test_scheduler_hot_topics.py tests/test_quality.py
.venv/bin/pytest
```

Manual report checks:

- desktop light and dark
- mobile light and dark around 390px width
- system preference and manual theme override
- hot-topic activation
- language toggle
- perspective expansion
- visible and clickable source provenance
- no horizontal overflow
- no visual regression back to glass/card styling

## Deployment Plan

1. Exclude local generated artifacts: screenshots, `.venv/`, `.git/`, `data/`, `output/`, caches.
2. Sync source, config, templates, and docs to `/vol1/1000/Docker/newsprism/`.
3. Rebuild/restart the Compose stack on fnOS.
4. Verify container health and recent logs.
