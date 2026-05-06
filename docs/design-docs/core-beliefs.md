# Core Beliefs

Foundational decisions that shape every other design choice in NewsPrism.

NewsPrism is an independent implementation. It is informed in part by the problem framing popularized by [TrendRadar](https://github.com/sansan0/TrendRadar), while its architecture, publishing flow, and codebase are its own.

---

## 1. Topic-first, not source-first

The primary unit of value is a *story*, not a feed.
The same event covered from five angles is more useful than five unrelated articles.

**Consequence**: clustering is the core algorithm. Everything else feeds it.

---

## 2. Multi-perspective is the product

A single-source summary is a commodity. The editorial value comes from:
- Chinese media angle vs. US media angle vs. EU regulatory angle
- Official framing vs. independent framing

**Consequence**: we collect sources for *diversity of perspective*, not just coverage breadth.
Weight ≠ quality; weight reflects how much we trust the source's perspective to be distinct.

---

## 3. Source tiers replace keyword gatekeeping

Three tiers:
- **editorial** — world news outlets; all articles pass, tagged "World News"
- **tech** — tech-focused outlets; all articles pass, tagged "Tech-General"
- **portal** — mixed-content portals; keyword filter still gates

Rationale: keywords miss emerging terms. Editorial/tech sources self-select relevant content
by their editorial mandate; we trust that mandate.

---

## 4. Agent legibility over human convenience

File structure, module boundaries, and documentation are optimised for a coding agent
reading the repo cold, not for a human navigating a familiar codebase.

**Consequence**: every architectural rule must be written down (see `ARCHITECTURE.md`).
If it is not in the repo, the agent cannot see it.

---

## 5. Layered imports — no shortcuts

The import graph is a strict DAG:

```
types → config → repo → service → runtime
```

No layer may import from a layer above it. No circular imports.
This is mechanically enforceable and eliminates a class of bugs.

---

## 6. newsnow as a Chinese-source proxy

Direct scraping of Chinese portals is fragile (anti-bot, GB2312, JS SPA).
We use the self-hosted [newsnow](https://github.com/ourongxing/newsnow) service
as a stable JSON proxy for 7 Chinese sources.

Fetch chain per source: `newsnow → RSS → rss_fallback → site API → HTML scrape`

**Consequence**: operational dependency on newsnow Docker service. Its failure degrades
Chinese-source coverage gracefully but does not break the pipeline.

---

## 7. Trust is data, not decoration

Source provenance, claim support, and editorial risk are part of the report contract.
They should be computed before summarization, persisted with the cluster, and rendered
where readers can inspect them.

**Consequence**: quality assessment can suppress weak clusters, request more evidence
for high-risk single-source stories, and constrain summaries to supported claims.

---

## 8. Storylines have lifecycle state

A developing story is not just a repeated topic. It can emerge, continue, turn, correct,
or stabilize across days.

**Consequence**: storyline state and timeline events are stored in SQLite and exported
to reports instead of being inferred only from same-day hot-topic grouping.

---

## 9. Reports should feel editorial, not operational

NewsPrism is read like a digest, not operated like a dashboard. Visual hierarchy should
come from typography, rules, spacing, and source labels rather than rounded cards,
decorative surfaces, shadows, or gradients.

**Consequence**: `templates/report-template.html` follows the WIRED-inspired reference
in `docs/design-docs/DESIGN.md`, including flat light and dark newsprint modes.
