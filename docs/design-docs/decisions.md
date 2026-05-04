# Decision Log

Lightweight record of key product and technical decisions. Most recent first.
For design rationale and principles, see [core-beliefs.md](core-beliefs.md).

| Date | Decision | Rationale | Supersedes |
|---|---|---|---|
| 2026-05-04 | Evaluate positive-energy extras beyond the regular main-feed cap | A strict positive lane should not lose cheerful candidates simply because the regular hard-news feed already filled 20 slots; extras are classified separately and render only if accepted | Positive candidates clipped by the main-lane cap |
| 2026-05-03 | Add an early positive-energy rescue lane before portal keyword filtering | Let clearly cheerful stories survive the normal hard-news filter, while a stricter post-summary LLM gate prevents weak or merely neutral stories from entering `今日正能量` | Post-summary-only positive highlights |
| 2026-04-06 | Move maintainer-only deployment and assistant runbooks out of the public repo | Keep the public repository focused on local development and contribution | — |
| 2026-04-22 | Consolidate report rendering onto one default template | Eliminate duplicated template maintenance and focus all front-end evolution on a single report surface | Three HTML report templates |
| 2026-02-22 | Expand keywords from 17 → 31 topic categories, 7 broad groups | Fill empty report categories (财经, 社会民生, 科学健康); improve cluster tagging coverage | — |
| 2026-02-22 | Add 7th report category: 🔭 科学健康 | Health/science content was unclassified; natural extension of the 6-category model | 6-category model |
| 2026-02-22 | Add JP×3, KR×3, RU×3 sources (9 total) | Expand geographic diversity; multilingual mpnet already supports ja/ko/ru | — |
| 2026-02-22 | Replace 今日头条 with 中国新闻网 | 头条 trending URLs are JS-rendered aggregators, not scrapable articles; 中国新闻网 has clean RSS | 今日头条 |
| 2026-02-22 | Enable Reuters + AP News via RSSHub | Direct RSS feeds were unavailable; RSSHub keeps the ingest path file-configurable | rss_url: null |
| 2026-02-22 | Proxy 6 CN sources through self-hosted newsnow | Several CN sources block direct scraping; a local proxy keeps the collector simple | Direct fetch |
| 2026-02-22 | Schedule timezone configurable via env var | Environment overrides are safer than hardcoded scheduler assumptions | Hardcoded UTC |
| 2026-02-21 | Source tiers replace keyword gatekeeping for editorial/tech | Per core-beliefs.md §3: editorial coverage quality > topic filtering | Keyword gate for all |
| 2026-02-21 | SQLite as persistence layer | Single-writer workload, simple local setup, and easy bind-mounting | — |
| 2026-02-21 | LiteLLM + DeepSeek as summarizer backend | Cost-effective; LiteLLM allows model swap without code changes | — |
| 2026-02-21 | sentence-transformers multilingual-mpnet for clustering | Covers all 8 source languages in one model; no per-language pipelines | — |
