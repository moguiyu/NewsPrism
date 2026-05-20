# Dynamic Impact Pipeline — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add LLM-based impact scoring (`ImpactJudge`) to the publish pipeline, deploy it as a parallel eval container on fnOS at port 8081, and provide an A/B evaluation harness comparing single-pass LLM clustering (Option A) against semantic-pre-cluster + ImpactJudge (Option B).

**Architecture:** Phase 1 keeps the existing `collect()` pipeline (TopicTagger + portal gate) unchanged. `ImpactJudge` is inserted into `publish()` after clustering and before the quality gate — it scores clusters on 4 dimensions, drops low-impact ones, and annotates survivors with a dynamic `impact_label`. A standalone `evaluate_pipeline.py` script runs both approaches against real DB data and calls a judge LLM to compare them. Everything runs in a git worktree (`feature/dynamic-impact`) deployed to a separate Docker container — production is never touched.

**Tech Stack:** Python 3.11, LiteLLM, Pydantic v2, SQLite, APScheduler, Docker Compose, Tavily (source discovery, Phase 2)

**Spec:** `docs/superpowers/specs/2026-05-20-dynamic-impact-pipeline-design.md`

**Out of scope (follow-up plan):** `SourceDiscoverer`, `review-sources` CLI, Phase 2 cutover (removing `filter.py`/`keywords.txt`).

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| CREATE | `newsprism/service/impact.py` | `ImpactJudge` — LLM cluster scoring |
| CREATE | `tests/test_impact.py` | Unit tests for ImpactJudge |
| CREATE | `scripts/evaluate_pipeline.py` | A/B evaluation harness |
| CREATE | `docker-compose.eval.yml` | Parallel eval Docker services |
| CREATE | `scripts/deploy-eval.sh` | Rsync + rebuild eval container on fnOS |
| CREATE | `config/sources.yaml` | Empty approved-sources file (template) |
| MODIFY | `newsprism/types.py` | Add impact fields to `ArticleCluster` |
| MODIFY | `newsprism/config.py` | Add `impact` config + impact LLM env vars |
| MODIFY | `newsprism/repo/db.py` | Add `cluster_impact` + `pending_sources` tables + `insert_cluster_impact()` |
| MODIFY | `newsprism/runtime/scheduler.py` | Insert `ImpactJudge` into `publish()` |
| MODIFY | `config/config.yaml` | Add `impact:` section |

---

## Task 1: Git Worktree Setup

**Files:** none — git operations only

- [ ] **Step 1: Create the feature branch from main**

```bash
git checkout main
git checkout -b feature/dynamic-impact
```

Expected: `Switched to a new branch 'feature/dynamic-impact'`

- [ ] **Step 2: Create the worktree**

```bash
git worktree add ../17_NewsPrism-eval feature/dynamic-impact
```

Expected: `Preparing worktree (checking out 'feature/dynamic-impact')`

- [ ] **Step 3: Verify worktree**

```bash
git worktree list
```

Expected output includes both `/Users/xiaodong/Code/17_NewsPrism` (main) and `/Users/xiaodong/Code/17_NewsPrism-eval` (feature/dynamic-impact).

- [ ] **Step 4: All remaining tasks run inside the worktree**

```bash
cd /Users/xiaodong/Code/17_NewsPrism-eval
```

Every file path and command from here uses `17_NewsPrism-eval` as the working directory.

---

## Task 2: DB Schema Migration

**Files:**
- Modify: `newsprism/repo/db.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_repo_db.py  — add to existing file, after existing imports
import json, tempfile
from pathlib import Path

def test_cluster_impact_table_created(tmp_path):
    db = tmp_path / "test.db"
    from newsprism.repo.db import init_db
    init_db(db)
    import sqlite3
    with sqlite3.connect(db) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "cluster_impact" in tables
    assert "pending_sources" in tables

def test_insert_and_get_cluster_impact(tmp_path):
    db = tmp_path / "test.db"
    from newsprism.repo.db import init_db, insert_cluster_impact, get_cluster_impact
    init_db(db)
    insert_cluster_impact(
        cluster_id="cluster-abc",
        run_date="2026-05-20",
        impact_score=0.72,
        geopolitical=0.8,
        economic=0.5,
        scientific=0.3,
        humanitarian=0.9,
        impact_label="Gaza Ceasefire Talks",
        impact_rationale="Major diplomatic development affecting millions.",
        affected_countries=["IL", "PS", "US"],
        affected_domains=["diplomacy", "conflict"],
        pipeline="b",
        db_path=db,
    )
    row = get_cluster_impact("cluster-abc", db_path=db)
    assert row is not None
    assert row["impact_score"] == 0.72
    assert row["impact_label"] == "Gaza Ceasefire Talks"
    assert json.loads(row["affected_countries"]) == ["IL", "PS", "US"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
.venv/bin/pytest tests/test_repo_db.py::test_cluster_impact_table_created tests/test_repo_db.py::test_insert_and_get_cluster_impact -v
```

Expected: FAIL — `ImportError: cannot import name 'insert_cluster_impact'`

- [ ] **Step 3: Add tables to `init_db()` in `newsprism/repo/db.py`**

Inside `init_db()`, append to the `executescript` string (before the closing `"""`):

```python
            CREATE TABLE IF NOT EXISTS cluster_impact (
                cluster_id          TEXT PRIMARY KEY,
                run_date            TEXT NOT NULL,
                impact_score        REAL NOT NULL,
                geopolitical        REAL,
                economic            REAL,
                scientific          REAL,
                humanitarian        REAL,
                impact_label        TEXT NOT NULL,
                impact_rationale    TEXT,
                affected_countries  TEXT,
                affected_domains    TEXT,
                pipeline            TEXT NOT NULL DEFAULT 'b',
                created_at          TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS pending_sources (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_url         TEXT NOT NULL UNIQUE,
                site_name        TEXT,
                language         TEXT,
                country_code     TEXT,
                affected_domains TEXT,
                gap_rationale    TEXT,
                sample_headlines TEXT,
                status           TEXT NOT NULL DEFAULT 'pending',
                discovered_at    TEXT NOT NULL DEFAULT (datetime('now')),
                reviewed_at      TEXT
            );
```

Also add migration guards after the existing migration block (after line ~200 in `db.py`):

```python
        # Migration: cluster_impact and pending_sources tables
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {r[0] for r in cursor.fetchall()}
        if "cluster_impact" not in existing_tables:
            conn.execute("""
                CREATE TABLE cluster_impact (
                    cluster_id TEXT PRIMARY KEY, run_date TEXT NOT NULL,
                    impact_score REAL NOT NULL, geopolitical REAL, economic REAL,
                    scientific REAL, humanitarian REAL, impact_label TEXT NOT NULL,
                    impact_rationale TEXT, affected_countries TEXT, affected_domains TEXT,
                    pipeline TEXT NOT NULL DEFAULT 'b',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """)
        if "pending_sources" not in existing_tables:
            conn.execute("""
                CREATE TABLE pending_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, feed_url TEXT NOT NULL UNIQUE,
                    site_name TEXT, language TEXT, country_code TEXT, affected_domains TEXT,
                    gap_rationale TEXT, sample_headlines TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    discovered_at TEXT NOT NULL DEFAULT (datetime('now')), reviewed_at TEXT
                )
            """)
```

- [ ] **Step 4: Add `insert_cluster_impact()` and `get_cluster_impact()` to `db.py`**

Add after the existing `insert_cluster` function:

```python
def insert_cluster_impact(
    *,
    cluster_id: str,
    run_date: str,
    impact_score: float,
    geopolitical: float,
    economic: float,
    scientific: float,
    humanitarian: float,
    impact_label: str,
    impact_rationale: str,
    affected_countries: list[str],
    affected_domains: list[str],
    pipeline: str = "b",
    db_path: Path = DB_PATH,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO cluster_impact
                (cluster_id, run_date, impact_score, geopolitical, economic, scientific,
                 humanitarian, impact_label, impact_rationale, affected_countries,
                 affected_domains, pipeline)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                cluster_id, run_date, impact_score, geopolitical, economic, scientific,
                humanitarian, impact_label, impact_rationale,
                json.dumps(affected_countries, ensure_ascii=False),
                json.dumps(affected_domains, ensure_ascii=False),
                pipeline,
            ),
        )


def get_cluster_impact(cluster_id: str, db_path: Path = DB_PATH) -> dict | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM cluster_impact WHERE cluster_id = ?", (cluster_id,)
        ).fetchone()
    return dict(row) if row else None
```

Make sure `import json` is already at the top of `db.py` (it is — line 8).

- [ ] **Step 5: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_repo_db.py::test_cluster_impact_table_created tests/test_repo_db.py::test_insert_and_get_cluster_impact -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add newsprism/repo/db.py tests/test_repo_db.py
git commit -m "feat: add cluster_impact and pending_sources tables + repo functions"
```

---

## Task 3: Config & Types Updates

**Files:**
- Modify: `newsprism/config.py`
- Modify: `newsprism/types.py`
- Modify: `config/config.yaml`
- Create: `config/sources.yaml`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_config_impact.py  (new file)
import os
import tempfile
from pathlib import Path


def test_config_loads_impact_section(tmp_path):
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text("""
sources: []
filter:
  keywords_file: config/keywords.txt
schedule: {}
collection: {}
clustering: {}
dedup: {}
summarizer: {}
output: {}
active_search: {}
impact:
  enabled: true
  min_score: 0.35
  max_clusters: 20
  domain_tags: [AI, trade, conflict]
""")
    # keywords.txt must exist for _parse_keywords to not crash
    kw = Path("config/keywords.txt")
    from newsprism.config import load_config
    cfg = load_config(str(config_yaml))
    assert cfg.impact["enabled"] is True
    assert cfg.impact["min_score"] == 0.35
    assert "AI" in cfg.impact["domain_tags"]


def test_config_loads_impact_llm_env(monkeypatch, tmp_path):
    monkeypatch.setenv("IMPACT_LLM_MODEL", "openai/deepseek-v4-flash")
    monkeypatch.setenv("IMPACT_LLM_BASE_URL", "https://api.deepseek.com/v1")
    monkeypatch.setenv("IMPACT_LLM_API_KEY", "sk-test-impact")
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text("""
sources: []
filter:
  keywords_file: config/keywords.txt
schedule: {}
collection: {}
clustering: {}
dedup: {}
summarizer: {}
output: {}
active_search: {}
impact:
  enabled: true
  min_score: 0.35
  max_clusters: 20
  domain_tags: []
""")
    from newsprism.config import load_config
    cfg = load_config(str(config_yaml))
    assert cfg.impact_llm_model == "openai/deepseek-v4-flash"
    assert cfg.impact_llm_api_key == "sk-test-impact"


def test_article_cluster_has_impact_fields():
    from newsprism.types import ArticleCluster, Article
    from datetime import datetime, timezone
    a = Article(
        url="https://example.com/1",
        title="Test",
        source_name="Reuters",
        published_at=datetime.now(timezone.utc),
        content="content",
        topics=[],
        language="en",
    )
    cluster = ArticleCluster(topic_category="test", articles=[a])
    assert cluster.impact_score is None
    assert cluster.impact_label is None
    assert cluster.affected_countries == []
    assert cluster.affected_domains == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_config_impact.py -v
```

Expected: FAIL — `Config has no attribute 'impact'`

- [ ] **Step 3: Update `Config` dataclass in `newsprism/config.py`**

Add these fields to the `Config` dataclass (after `feelgood_keywords`):

```python
    impact: dict[str, Any] = field(default_factory=dict)

    # Impact LLM — falls back to main LITELLM_* vars if unset
    impact_llm_model: str = field(
        default_factory=lambda: os.environ.get("IMPACT_LLM_MODEL") or os.environ.get("LITELLM_MODEL", "deepseek/deepseek-chat")
    )
    impact_llm_base_url: str = field(
        default_factory=lambda: os.environ.get("IMPACT_LLM_BASE_URL") or os.environ.get("LITELLM_BASE_URL", "https://api.deepseek.com")
    )
    impact_llm_api_key: str = field(
        default_factory=lambda: os.environ.get("IMPACT_LLM_API_KEY") or os.environ.get("LITELLM_API_KEY", "")
    )
```

- [ ] **Step 4: Update `load_config()` to populate `impact` and load `sources.yaml`**

In `load_config()`, add after `editorial_values` loading:

```python
    # Load dynamic approved sources from config/sources.yaml (if it exists)
    sources_yaml_path = config_root / "config" / "sources.yaml"
    if sources_yaml_path.exists():
        extra = yaml.safe_load(sources_yaml_path.read_text(encoding="utf-8")) or {}
        for s in extra.get("sources", []):
            if not s.get("enabled", True):
                continue
            sources.append(SourceConfig(
                name=s["name"],
                name_en=s.get("name_en", s["name"]),
                url=s.get("url", s["feed_url"]),
                rss_url=s.get("feed_url"),
                type="rss",
                weight=float(s.get("weight", 1.0)),
                language=s.get("language", "en"),
                region=s.get("country_code", ""),
                tier=s.get("tier", "editorial"),
                enabled=True,
            ))
```

And in the `Config(...)` constructor call at the bottom of `load_config()`, add:

```python
        impact=raw.get("impact", {}),
```

- [ ] **Step 5: Add impact fields to `ArticleCluster` in `newsprism/types.py`**

In the `ArticleCluster` dataclass, add after `quality_decision`:

```python
    # Impact scoring (set by ImpactJudge in publish pipeline)
    impact_score: float | None = None
    impact_label: str | None = None
    impact_rationale: str | None = None
    affected_countries: list[str] = field(default_factory=list)
    affected_domains: list[str] = field(default_factory=list)
    impact_dimensions: dict[str, float] = field(default_factory=dict)
```

- [ ] **Step 6: Add `impact` section to `config/config.yaml`**

Append to the end of `config/config.yaml`:

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

- [ ] **Step 7: Create `config/sources.yaml`**

```yaml
# Dynamic approved sources — managed by `python -m newsprism review-sources`
# Add entries here after running source discovery.
sources: []
```

- [ ] **Step 8: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_config_impact.py -v
```

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add newsprism/config.py newsprism/types.py config/config.yaml config/sources.yaml tests/test_config_impact.py
git commit -m "feat: add impact config section, LLM env vars, and ArticleCluster impact fields"
```

---

## Task 4: ImpactJudge Service

**Files:**
- Create: `newsprism/service/impact.py`
- Create: `tests/test_impact.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_impact.py
from __future__ import annotations
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from newsprism.config import Config, SourceConfig
from newsprism.types import Article, ArticleCluster


def _make_config(min_score=0.35, max_clusters=20, enabled=True) -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={},
        active_search={},
        impact={
            "enabled": enabled,
            "min_score": min_score,
            "max_clusters": max_clusters,
            "domain_tags": ["AI", "trade", "conflict", "diplomacy"],
        },
        impact_llm_model="openai/deepseek-chat",
        impact_llm_base_url="https://api.deepseek.com/v1",
        impact_llm_api_key="sk-test",
    )


def _make_cluster(topic: str, sources: list[str], score_hint: float = 0.8) -> ArticleCluster:
    articles = [
        Article(
            url=f"https://example.com/{i}",
            title=f"Headline about {topic} from {src}",
            source_name=src,
            published_at=datetime.now(timezone.utc),
            content=f"Detailed content about {topic}.",
            topics=[],
            language="en",
        )
        for i, src in enumerate(sources)
    ]
    return ArticleCluster(topic_category=topic, articles=articles)


MOCK_LLM_RESPONSE = """{
  "clusters": [
    {
      "index": 0,
      "impact_score": 0.82,
      "geopolitical": 0.9,
      "economic": 0.7,
      "scientific": 0.2,
      "humanitarian": 0.8,
      "impact_label": "US-China Trade War Escalation",
      "impact_rationale": "New tariffs affecting $500B in trade with global supply chain implications.",
      "affected_countries": ["US", "CN"],
      "affected_domains": ["trade", "supply-chain"]
    },
    {
      "index": 1,
      "impact_score": 0.21,
      "geopolitical": 0.1,
      "economic": 0.1,
      "scientific": 0.5,
      "humanitarian": 0.1,
      "impact_label": "Local Sports Result",
      "impact_rationale": "Regional sports event with limited global significance.",
      "affected_countries": ["GB"],
      "affected_domains": []
    }
  ]
}"""


def test_impact_judge_scores_and_filters_clusters():
    from newsprism.service.impact import ImpactJudge

    cfg = _make_config(min_score=0.35, max_clusters=20)
    judge = ImpactJudge(cfg)

    clusters = [
        _make_cluster("US-China trade", ["Reuters", "AP", "新华社"]),
        _make_cluster("Local sports", ["BBC Sport"]),
    ]

    mock_response = MagicMock()
    mock_response.choices[0].message.content = MOCK_LLM_RESPONSE

    with patch("litellm.completion", return_value=mock_response):
        result = judge.score(clusters)

    assert len(result) == 1  # second cluster dropped (score 0.21 < min_score 0.35)
    assert result[0].impact_score == 0.82
    assert result[0].impact_label == "US-China Trade War Escalation"
    assert result[0].affected_countries == ["US", "CN"]
    assert result[0].affected_domains == ["trade", "supply-chain"]
    assert result[0].impact_dimensions == {
        "geopolitical": 0.9, "economic": 0.7, "scientific": 0.2, "humanitarian": 0.8
    }


def test_impact_judge_disabled_returns_all_clusters():
    from newsprism.service.impact import ImpactJudge

    cfg = _make_config(enabled=False)
    judge = ImpactJudge(cfg)

    clusters = [_make_cluster("topic", ["Reuters"])]
    with patch("litellm.completion") as mock_llm:
        result = judge.score(clusters)
    mock_llm.assert_not_called()
    assert result == clusters


def test_impact_judge_max_clusters_cap():
    from newsprism.service.impact import ImpactJudge

    cfg = _make_config(min_score=0.0, max_clusters=1)
    judge = ImpactJudge(cfg)

    clusters = [
        _make_cluster("High impact", ["Reuters", "AP"]),
        _make_cluster("Also high impact", ["BBC", "CNN"]),
    ]

    response_json = """{
      "clusters": [
        {"index": 0, "impact_score": 0.9, "geopolitical": 0.9, "economic": 0.5,
         "scientific": 0.3, "humanitarian": 0.7, "impact_label": "Story A",
         "impact_rationale": "Major event.", "affected_countries": ["US"], "affected_domains": ["diplomacy"]},
        {"index": 1, "impact_score": 0.75, "geopolitical": 0.7, "economic": 0.4,
         "scientific": 0.2, "humanitarian": 0.6, "impact_label": "Story B",
         "impact_rationale": "Notable event.", "affected_countries": ["GB"], "affected_domains": ["trade"]}
      ]
    }"""

    mock_response = MagicMock()
    mock_response.choices[0].message.content = response_json

    with patch("litellm.completion", return_value=mock_response):
        result = judge.score(clusters)

    assert len(result) == 1  # capped at max_clusters=1
    assert result[0].impact_label == "Story A"  # highest score first


def test_impact_judge_empty_clusters():
    from newsprism.service.impact import ImpactJudge
    cfg = _make_config()
    judge = ImpactJudge(cfg)
    with patch("litellm.completion") as mock_llm:
        result = judge.score([])
    mock_llm.assert_not_called()
    assert result == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_impact.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'newsprism.service.impact'`

- [ ] **Step 3: Create `newsprism/service/impact.py`**

```python
"""LLM-based cluster impact scoring — replaces static keyword-based topic selection.

For each publish run, ImpactJudge makes a single LLM call that receives all
candidate clusters and returns impact scores across 4 dimensions. Clusters below
the configured threshold are dropped; survivors are annotated with a dynamic
impact_label that replaces the keyword-derived topic_category in the report.

Layer: service (imports types, config; never imports repo or runtime)
"""
from __future__ import annotations

import json
import logging

import litellm
from pydantic import BaseModel, Field

from newsprism.config import Config
from newsprism.service.llm_compat import completion_compat_kwargs
from newsprism.types import ArticleCluster

logger = logging.getLogger(__name__)

litellm.set_verbose = False


class ClusterImpact(BaseModel):
    index: int
    impact_score: float = Field(ge=0.0, le=1.0)
    geopolitical: float = Field(ge=0.0, le=1.0)
    economic: float = Field(ge=0.0, le=1.0)
    scientific: float = Field(ge=0.0, le=1.0)
    humanitarian: float = Field(ge=0.0, le=1.0)
    impact_label: str
    impact_rationale: str
    affected_countries: list[str] = Field(default_factory=list)
    affected_domains: list[str] = Field(default_factory=list)


class ImpactBatchResponse(BaseModel):
    clusters: list[ClusterImpact]


class ImpactJudge:
    """Scores a batch of ArticleClusters for global impact using a single LLM call."""

    def __init__(self, cfg: Config) -> None:
        self.enabled = cfg.impact.get("enabled", True)
        self.min_score = float(cfg.impact.get("min_score", 0.35))
        self.max_clusters = int(cfg.impact.get("max_clusters", 20))
        self.domain_tags = cfg.impact.get("domain_tags", [])
        self.domain_tags_str = ", ".join(self.domain_tags)

        self.model = cfg.impact_llm_model
        self.base_url = cfg.impact_llm_base_url
        self.api_key = cfg.impact_llm_api_key
        self.compat_kwargs = completion_compat_kwargs(self.model, self.base_url)

    def score(self, clusters: list[ArticleCluster]) -> list[ArticleCluster]:
        """Score clusters, drop low-impact ones, annotate survivors. Returns sorted by score desc."""
        if not self.enabled or not clusters:
            return clusters

        prompt = self._build_prompt(clusters)
        try:
            response = litellm.completion(
                model=self.model,
                api_key=self.api_key,
                api_base=self.base_url,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=len(clusters) * 300 + 500,
                response_format={"type": "json_object"},
                **self.compat_kwargs,
            )
            content = response.choices[0].message.content or ""
            batch = ImpactBatchResponse.model_validate_json(content)
        except Exception:
            logger.exception("ImpactJudge LLM call failed — returning all clusters unscored")
            return clusters

        scored_map: dict[int, ClusterImpact] = {c.index: c for c in batch.clusters}
        annotated: list[ArticleCluster] = []

        for i, cluster in enumerate(clusters):
            impact = scored_map.get(i)
            if impact is None:
                logger.warning("ImpactJudge missing score for cluster index %d — skipping", i)
                continue
            if impact.impact_score < self.min_score:
                logger.debug(
                    "Dropping cluster '%s' (impact_score=%.2f < min_score=%.2f)",
                    cluster.topic_category, impact.impact_score, self.min_score,
                )
                continue
            cluster.impact_score = impact.impact_score
            cluster.impact_label = impact.impact_label
            cluster.impact_rationale = impact.impact_rationale
            cluster.affected_countries = impact.affected_countries
            cluster.affected_domains = impact.affected_domains
            cluster.impact_dimensions = {
                "geopolitical": impact.geopolitical,
                "economic": impact.economic,
                "scientific": impact.scientific,
                "humanitarian": impact.humanitarian,
            }
            annotated.append(cluster)

        annotated.sort(key=lambda c: c.impact_score or 0.0, reverse=True)
        result = annotated[: self.max_clusters]

        logger.info(
            "ImpactJudge: %d/%d clusters retained (min_score=%.2f, max=%d)",
            len(result), len(clusters), self.min_score, self.max_clusters,
        )
        return result

    def _build_prompt(self, clusters: list[ArticleCluster]) -> str:
        blocks: list[str] = []
        for i, cluster in enumerate(clusters):
            headlines = "\n".join(
                f"  [{a.source_name}] {a.title}" for a in cluster.articles[:6]
            )
            blocks.append(f"=== Cluster {i} | Sources: {', '.join(cluster.sources[:5])} ===\n{headlines}")

        clusters_text = "\n\n".join(blocks)
        domain_list = self.domain_tags_str or "AI, trade, conflict, diplomacy, disaster, public-health"

        return (
            f"You are a global news editor. Evaluate the impact of each news cluster on humankind.\n\n"
            f"Score each cluster across 4 dimensions (0.0–1.0 each):\n"
            f"- geopolitical: impact on nations, governments, international relations\n"
            f"- economic: impact on markets, trade, employment, supply chains\n"
            f"- scientific: impact on technology, science, innovation, public health\n"
            f"- humanitarian: impact on human welfare, rights, disasters, migration\n\n"
            f"Compute impact_score as the weighted average: "
            f"geopolitical×0.3 + economic×0.25 + scientific×0.2 + humanitarian×0.25\n\n"
            f"Also provide:\n"
            f"- impact_label: a specific, human-readable event name (e.g. 'Gaza Ceasefire Talks', not 'Geopolitics')\n"
            f"- impact_rationale: one sentence explaining global significance\n"
            f"- affected_countries: ISO-3166 alpha-2 codes of most affected countries\n"
            f"- affected_domains: choose from: {domain_list}\n\n"
            f"Output JSON only:\n"
            f'{{\"clusters\": [{{\"index\": 0, \"impact_score\": 0.75, \"geopolitical\": 0.8, '
            f'\"economic\": 0.6, \"scientific\": 0.3, \"humanitarian\": 0.7, '
            f'\"impact_label\": \"...\", \"impact_rationale\": \"...\", '
            f'\"affected_countries\": [\"US\", \"CN\"], \"affected_domains\": [\"trade\"]}}, ...]}}\n\n'
            f"--- Clusters ---\n\n{clusters_text}"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_impact.py -v
```

Expected: all 4 PASS

- [ ] **Step 5: Commit**

```bash
git add newsprism/service/impact.py tests/test_impact.py
git commit -m "feat: add ImpactJudge service with LLM cluster scoring"
```

---

## Task 5: Wire ImpactJudge into Scheduler

**Files:**
- Modify: `newsprism/runtime/scheduler.py`

- [ ] **Step 1: Write the failing integration test**

```python
# tests/test_scheduler_impact.py  (new file)
from __future__ import annotations
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from newsprism.config import Config, SourceConfig
from newsprism.types import Article, ArticleCluster


def _make_config_with_impact() -> Config:
    return Config(
        raw={},
        sources=[],
        topics={},
        schedule={},
        collection={},
        filter={},
        clustering={},
        dedup={},
        summarizer={},
        output={},
        active_search={},
        impact={"enabled": True, "min_score": 0.0, "max_clusters": 20, "domain_tags": ["AI"]},
        impact_llm_model="openai/deepseek-chat",
        impact_llm_base_url="https://api.deepseek.com",
        impact_llm_api_key="sk-test",
    )


def test_scheduler_instantiates_impact_judge():
    """Scheduler.__init__ must create self.impact_judge when impact.enabled is true."""
    from newsprism.runtime.scheduler import NewsPrismScheduler
    cfg = _make_config_with_impact()
    with patch("newsprism.runtime.scheduler.Collector"), \
         patch("newsprism.runtime.scheduler.Deduplicator"), \
         patch("newsprism.runtime.scheduler.Clusterer"), \
         patch("newsprism.runtime.scheduler.ImpactJudge") as MockJudge, \
         patch("newsprism.runtime.scheduler.TopicTagger"), \
         patch("newsprism.runtime.scheduler.TelegramPublisher"):
        scheduler = NewsPrismScheduler(cfg)
    MockJudge.assert_called_once_with(cfg)
    assert hasattr(scheduler, "impact_judge")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
.venv/bin/pytest tests/test_scheduler_impact.py -v
```

Expected: FAIL — `ImportError: cannot import name 'ImpactJudge'` (not yet imported in scheduler)

- [ ] **Step 3: Add import and instantiation to `scheduler.py`**

In `newsprism/runtime/scheduler.py`, add the import near the other service imports (around line 51):

```python
from newsprism.service.impact import ImpactJudge
```

In `NewsPrismScheduler.__init__()`, after `self.tagger = TopicTagger(cfg)` (line ~1497), add:

```python
        self.impact_judge = ImpactJudge(cfg)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
.venv/bin/pytest tests/test_scheduler_impact.py -v
```

Expected: PASS

- [ ] **Step 5: Insert ImpactJudge into the `publish()` pipeline**

In `scheduler.py`, find the publish pipeline section that reads:

```python
            candidate_clusters = clusters[:candidate_window]
```

After this line, add the ImpactJudge call:

```python
            # Impact scoring: LLM scores all candidates, drops low-impact clusters
            candidate_clusters = self.impact_judge.score(candidate_clusters)
            if not candidate_clusters:
                logger.warning("ImpactJudge filtered all clusters — skipping publish")
                return
            logger.info("ImpactJudge: %d clusters retained for enrichment", len(candidate_clusters))
```

- [ ] **Step 6: Store impact scores to DB after summarization**

In `scheduler.py`, find where summaries are stored to DB (after `freshness_results` loop, where `insert_cluster` is called). Add impact persistence inside the same loop, after the existing `insert_cluster` call:

```python
                    # Persist impact scores
                    if cs.cluster.impact_score is not None:
                        from newsprism.repo.db import insert_cluster_impact
                        import uuid
                        cluster_id = f"{today.isoformat()}-{uuid.uuid4().hex[:8]}"
                        insert_cluster_impact(
                            cluster_id=cluster_id,
                            run_date=today.isoformat(),
                            impact_score=cs.cluster.impact_score,
                            geopolitical=cs.cluster.impact_dimensions.get("geopolitical", 0.0),
                            economic=cs.cluster.impact_dimensions.get("economic", 0.0),
                            scientific=cs.cluster.impact_dimensions.get("scientific", 0.0),
                            humanitarian=cs.cluster.impact_dimensions.get("humanitarian", 0.0),
                            impact_label=cs.cluster.impact_label or cs.cluster.topic_category,
                            impact_rationale=cs.cluster.impact_rationale or "",
                            affected_countries=cs.cluster.affected_countries,
                            affected_domains=cs.cluster.affected_domains,
                            pipeline="b",
                        )
```

- [ ] **Step 7: Update renderer to use `impact_label` when available**

In `newsprism/runtime/renderer.py`, find where `topic_category` is used as the cluster heading. Search for it:

```bash
grep -n "topic_category" /Users/xiaodong/Code/17_NewsPrism-eval/newsprism/runtime/renderer.py | head -20
```

For each place where a cluster heading is rendered from `topic_category`, update to prefer `impact_label`:

```python
# Pattern to find and replace:
# cluster.topic_category  →  cluster.impact_label or cluster.topic_category
# cs.cluster.topic_category  →  cs.cluster.impact_label or cs.cluster.topic_category
```

Example — if you find a line like:
```python
"topic": cluster.topic_category,
```
Change it to:
```python
"topic": cluster.impact_label or cluster.topic_category,
```

Apply this to all occurrences in `renderer.py` where the topic label is set for display.

- [ ] **Step 8: Run the full test suite**

```bash
.venv/bin/pytest tests/ -v --tb=short
```

Expected: all tests PASS (test count may increase). Fix any failures before committing.

- [ ] **Step 9: Commit**

```bash
git add newsprism/runtime/scheduler.py newsprism/runtime/renderer.py tests/test_scheduler_impact.py
git commit -m "feat: wire ImpactJudge into publish pipeline; persist impact scores to DB"
```

---

## Task 6: Evaluation Harness

**Files:**
- Create: `scripts/evaluate_pipeline.py`

- [ ] **Step 1: Create the evaluation harness**

```python
#!/usr/bin/env python3
"""A/B evaluation harness — compares Option A (single-pass LLM) vs Option B (mpnet + ImpactJudge).

Usage:
    python scripts/evaluate_pipeline.py [--date YYYY-MM-DD] [--db PATH]

Output:
    output/eval-YYYY-MM-DD.html  — side-by-side ranked lists + judge LLM verdict
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import litellm
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger("evaluate")


def load_articles(eval_date: date, db_path: Path):
    from newsprism.repo.db import init_db
    from newsprism.repo.db import DB_PATH
    db = db_path or DB_PATH
    init_db(db)
    import sqlite3
    cutoff = (datetime.combine(eval_date, datetime.min.time()) - timedelta(hours=24)).isoformat()
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM articles WHERE published_at >= ? ORDER BY published_at DESC LIMIT 500",
            (cutoff,)
        ).fetchall()
    from newsprism.types import Article
    articles = []
    for r in rows:
        try:
            articles.append(Article(
                url=r["url"], title=r["title"], source_name=r["source_name"],
                published_at=datetime.fromisoformat(r["published_at"]),
                content=r["content"] or "",
                topics=json.loads(r["topics"] or "[]"),
                language="",
            ))
        except Exception:
            pass
    return articles


def run_option_b(articles, cfg) -> list[dict]:
    """Option B: mpnet semantic clustering → ImpactJudge."""
    from newsprism.service.clusterer import Clusterer
    from newsprism.service.impact import ImpactJudge
    clusterer = Clusterer(cfg)
    judge = ImpactJudge(cfg)
    clusters = clusterer.cluster(articles)
    scored = judge.score(clusters)
    return [
        {
            "label": c.impact_label or c.topic_category,
            "score": c.impact_score or 0.0,
            "rationale": c.impact_rationale or "",
            "sources": c.sources[:5],
            "countries": c.affected_countries,
            "domains": c.affected_domains,
            "headlines": [a.title for a in c.articles[:3]],
            "pipeline": "B",
        }
        for c in scored
    ]


def run_option_a(articles, cfg) -> list[dict]:
    """Option A: single large-context LLM call — groups + scores all articles at once."""
    eval_model = os.environ.get("EVAL_LLM_MODEL") or cfg.impact_llm_model
    eval_base_url = os.environ.get("EVAL_LLM_BASE_URL") or cfg.impact_llm_base_url
    eval_api_key = os.environ.get("EVAL_LLM_API_KEY") or cfg.impact_llm_api_key

    from newsprism.service.llm_compat import completion_compat_kwargs
    compat = completion_compat_kwargs(eval_model, eval_base_url)

    # Format all articles as a compact list for the large-context LLM
    lines = []
    for i, a in enumerate(articles[:300]):  # safety cap
        lines.append(f"[{i}] [{a.source_name}] {a.title}")
    articles_text = "\n".join(lines)

    prompt = (
        "You are a global news editor with access to all articles collected in the last 24 hours.\n"
        "Group them into distinct news events, then score each event for global impact.\n\n"
        "Scoring dimensions (0.0–1.0):\n"
        "- geopolitical: nations, governments, international relations\n"
        "- economic: markets, trade, employment, supply chains\n"
        "- scientific: technology, innovation, public health\n"
        "- humanitarian: human welfare, rights, disasters, migration\n"
        "impact_score = geopolitical×0.3 + economic×0.25 + scientific×0.2 + humanitarian×0.25\n\n"
        "Output JSON:\n"
        "{\"clusters\": [{\"impact_label\": \"...\", \"impact_score\": 0.8, "
        "\"geopolitical\": 0.9, \"economic\": 0.7, \"scientific\": 0.2, \"humanitarian\": 0.8, "
        "\"impact_rationale\": \"...\", \"affected_countries\": [\"US\", \"CN\"], "
        "\"affected_domains\": [\"trade\"], \"article_indices\": [0, 5, 12], "
        "\"sources\": [\"Reuters\", \"AP\"]}, ...]}\n\n"
        "--- Articles ---\n\n" + articles_text
    )

    try:
        response = litellm.completion(
            model=eval_model,
            api_key=eval_api_key,
            api_base=eval_base_url,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=8000,
            response_format={"type": "json_object"},
            **compat,
        )
        content = response.choices[0].message.content or "{}"
        data = json.loads(content)
        result = []
        for c in data.get("clusters", [])[:20]:
            result.append({
                "label": c.get("impact_label", "Unknown"),
                "score": float(c.get("impact_score", 0.0)),
                "rationale": c.get("impact_rationale", ""),
                "sources": c.get("sources", [])[:5],
                "countries": c.get("affected_countries", []),
                "domains": c.get("affected_domains", []),
                "headlines": [articles[i].title for i in c.get("article_indices", [])[:3] if i < len(articles)],
                "pipeline": "A",
            })
        result.sort(key=lambda x: x["score"], reverse=True)
        return result
    except Exception:
        logger.exception("Option A LLM call failed")
        return []


def judge_comparison(option_a: list[dict], option_b: list[dict], cfg) -> str:
    """Ask a judge LLM which pipeline produced the better impact ranking."""
    a_text = "\n".join(f"{i+1}. [{c['score']:.2f}] {c['label']} — {c['rationale']}" for i, c in enumerate(option_a[:10]))
    b_text = "\n".join(f"{i+1}. [{c['score']:.2f}] {c['label']} — {c['rationale']}" for i, c in enumerate(option_b[:10]))

    prompt = (
        "You are evaluating two news pipeline approaches. Both processed the same 24-hour article pool.\n\n"
        f"--- Option A (single-pass LLM grouping) top 10 ---\n{a_text}\n\n"
        f"--- Option B (semantic clustering + ImpactJudge) top 10 ---\n{b_text}\n\n"
        "Evaluate:\n"
        "1. Which ranking better represents globally significant events?\n"
        "2. Which surfaces stories most readers should know about vs. niche noise?\n"
        "3. What important events did each approach miss that the other caught?\n"
        "4. Overall verdict: A, B, or tie — with reasoning.\n\n"
        "Be specific and critical. Mention event names."
    )

    try:
        response = litellm.completion(
            model=cfg.impact_llm_model,
            api_key=cfg.impact_llm_api_key,
            api_base=cfg.impact_llm_base_url,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1000,
        )
        return response.choices[0].message.content or "Judge call returned empty."
    except Exception:
        logger.exception("Judge LLM call failed")
        return "Judge call failed — check logs."


def render_html(eval_date: date, option_a: list[dict], option_b: list[dict], verdict: str) -> str:
    def cluster_rows(clusters: list[dict]) -> str:
        rows = []
        for c in clusters:
            headlines = "<br>".join(f"• {h}" for h in c["headlines"])
            countries = ", ".join(c["countries"][:5]) or "—"
            domains = ", ".join(c["domains"][:4]) or "—"
            score_pct = int(c["score"] * 100)
            rows.append(f"""
            <tr>
              <td><strong>{c['label']}</strong><br><small>{c['rationale']}</small></td>
              <td><div class="score-bar"><div class="score-fill" style="width:{score_pct}%"></div></div>
                  <small>{c['score']:.2f}</small></td>
              <td><small>{', '.join(c['sources'][:3])}</small></td>
              <td><small>{countries}</small></td>
              <td><small>{domains}</small></td>
              <td><small>{headlines}</small></td>
            </tr>""")
        return "\n".join(rows)

    # Diff: what A has that B doesn't and vice versa
    a_labels = {c["label"] for c in option_a}
    b_labels = {c["label"] for c in option_b}
    only_in_a = [c for c in option_a if c["label"] not in b_labels]
    only_in_b = [c for c in option_b if c["label"] not in a_labels]

    def diff_list(clusters):
        return "".join(f"<li>[{c['score']:.2f}] <strong>{c['label']}</strong> — {c['rationale']}</li>" for c in clusters)

    verdict_html = verdict.replace("\n", "<br>")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NewsPrism Eval {eval_date}</title>
<style>
  body {{ font-family: -apple-system, sans-serif; background: #f8f7f4; color: #111; padding: 32px; }}
  h1 {{ border-bottom: 3px solid #111; padding-bottom: 12px; }}
  h2 {{ font-size: 1rem; text-transform: uppercase; letter-spacing: 1px; color: #555; margin-top: 32px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin: 24px 0; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
  th {{ background: #111; color: #fff; text-align: left; padding: 8px 10px; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #ddd; vertical-align: top; }}
  tr:nth-child(even) td {{ background: #f2f0ec; }}
  .score-bar {{ background: #ddd; height: 6px; border-radius: 3px; margin-bottom: 2px; }}
  .score-fill {{ background: #057dbc; height: 6px; border-radius: 3px; }}
  .verdict {{ background: #fff; border-left: 4px solid #057dbc; padding: 16px 20px; font-size: 0.9rem; line-height: 1.7; }}
  .diff {{ background: #fff; border: 1px solid #ddd; padding: 16px 20px; }}
  .diff ul {{ padding-left: 20px; font-size: 0.84rem; }}
  .tag {{ font-size: 0.68rem; font-family: monospace; background: #e8f4fd; color: #057dbc; padding: 1px 6px; border-radius: 2px; }}
</style>
</head>
<body>
<h1>NewsPrism Pipeline A/B Evaluation — {eval_date}</h1>
<p><span class="tag">Option A</span> Single-pass LLM grouping &nbsp;|&nbsp; <span class="tag">Option B</span> mpnet clustering + ImpactJudge</p>

<h2>Side-by-Side Ranking</h2>
<div class="grid">
  <div>
    <h3>Option A — Single-pass LLM ({len(option_a)} clusters)</h3>
    <table>
      <thead><tr><th>Event</th><th>Score</th><th>Sources</th><th>Countries</th><th>Domains</th><th>Headlines</th></tr></thead>
      <tbody>{cluster_rows(option_a)}</tbody>
    </table>
  </div>
  <div>
    <h3>Option B — mpnet + ImpactJudge ({len(option_b)} clusters)</h3>
    <table>
      <thead><tr><th>Event</th><th>Score</th><th>Sources</th><th>Countries</th><th>Domains</th><th>Headlines</th></tr></thead>
      <tbody>{cluster_rows(option_b)}</tbody>
    </table>
  </div>
</div>

<h2>Diff — What Each Approach Uniquely Surfaced</h2>
<div class="grid">
  <div class="diff">
    <strong>Only in Option A ({len(only_in_a)}):</strong>
    <ul>{diff_list(only_in_a) or '<li><em>None</em></li>'}</ul>
  </div>
  <div class="diff">
    <strong>Only in Option B ({len(only_in_b)}):</strong>
    <ul>{diff_list(only_in_b) or '<li><em>None</em></li>'}</ul>
  </div>
</div>

<h2>Judge LLM Verdict</h2>
<div class="verdict">{verdict_html}</div>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(description="NewsPrism A/B pipeline evaluation")
    parser.add_argument("--date", help="Report date YYYY-MM-DD (default: today)")
    parser.add_argument("--db", help="Path to SQLite DB (default: data/newsprism.db)")
    args = parser.parse_args()

    eval_date = date.fromisoformat(args.date) if args.date else date.today()
    db_path = Path(args.db) if args.db else None

    from newsprism.config import load_config
    cfg = load_config()

    logger.info("Loading articles for %s", eval_date)
    articles = load_articles(eval_date, db_path)
    logger.info("Loaded %d articles", len(articles))

    if not articles:
        print("No articles found. Run collect first: docker exec newsprism-eval python -m newsprism collect")
        sys.exit(1)

    logger.info("Running Option B (mpnet + ImpactJudge)...")
    option_b = run_option_b(articles, cfg)
    logger.info("Option B: %d clusters", len(option_b))

    logger.info("Running Option A (single-pass LLM)...")
    option_a = run_option_a(articles, cfg)
    logger.info("Option A: %d clusters", len(option_a))

    logger.info("Calling judge LLM...")
    verdict = judge_comparison(option_a, option_b, cfg)

    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f"eval-{eval_date}.html"
    out_file.write_text(render_html(eval_date, option_a, option_b, verdict), encoding="utf-8")
    logger.info("Evaluation report written to %s", out_file)
    print(f"\nOpen: {out_file.resolve()}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Make it executable and test syntax**

```bash
chmod +x scripts/evaluate_pipeline.py
.venv/bin/python -m py_compile scripts/evaluate_pipeline.py && echo "Syntax OK"
```

Expected: `Syntax OK`

- [ ] **Step 3: Commit**

```bash
git add scripts/evaluate_pipeline.py
git commit -m "feat: add A/B evaluation harness with judge LLM verdict"
```

---

## Task 7: Eval Docker Setup

**Files:**
- Create: `docker-compose.eval.yml`
- Create: `scripts/deploy-eval.sh`

- [ ] **Step 1: Create `docker-compose.eval.yml`**

```yaml
# Parallel eval deployment — port 8081, separate data/output volumes
# Production (port 8080) is never touched by this file.
services:
  newsprism-eval:
    build: .
    container_name: newsprism-eval
    restart: unless-stopped
    env_file: .env
    volumes:
      - ./data-eval:/app/data
      - ./output-eval:/app/output
    depends_on:
      newsnow:
        condition: service_healthy

  newsprism-eval-web:
    image: nginx:alpine
    container_name: newsprism-eval-web
    restart: unless-stopped
    ports:
      - "8081:80"
    volumes:
      - ./output-eval:/usr/share/nginx/html:ro
      - ./nginx.conf:/etc/nginx/conf.d/default.conf:ro
    depends_on:
      - newsprism-eval

  # Reuse the existing newsnow service if already running; otherwise define it here.
  # If newsnow is already up from docker-compose.dev.yml, remove this block and
  # set NEWSNOW_BASE_URL in .env to point at the existing container.
  newsnow:
    image: ghcr.io/ourongxing/newsnow:latest
    container_name: newsnow-eval
    restart: unless-stopped
    ports:
      - "3002:4444"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://127.0.0.1:4444/"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 90s
```

> **Note:** If the existing `newsnow` container is already running on fnOS, remove the `newsnow` service block and set `NEWSNOW_BASE_URL=http://newsnow:4444` in `.env` so the eval container reuses it. The `depends_on` block should reference the existing container name.

- [ ] **Step 2: Create `scripts/deploy-eval.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

REMOTE="aiagent@192.168.10.5"
REMOTE_PORT=123
SSH_KEY="$HOME/.ssh/fnoskey"
REMOTE_DIR="/vol1/1000/Docker/newsprism-eval"
LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== NewsPrism Eval Deploy ==="
echo "Local:  $LOCAL_DIR"
echo "Remote: $REMOTE:$REMOTE_DIR"

# Create remote directory
ssh -i "$SSH_KEY" -p "$REMOTE_PORT" "$REMOTE" "mkdir -p $REMOTE_DIR/data-eval $REMOTE_DIR/output-eval"

# Rsync source — exclude prod data, venv, generated files, and .env
rsync -avz --progress \
  --exclude='.env' \
  --exclude='.env.production' \
  --exclude='data/' \
  --exclude='data-eval/' \
  --exclude='output/' \
  --exclude='output-eval/' \
  --exclude='.venv/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  --exclude='.git/' \
  --exclude='design-samples/' \
  --exclude='node_modules/' \
  -e "ssh -i $SSH_KEY -p $REMOTE_PORT" \
  "$LOCAL_DIR/" "$REMOTE:$REMOTE_DIR/"

echo ""
echo "=== Building and restarting eval container ==="
ssh -i "$SSH_KEY" -p "$REMOTE_PORT" "$REMOTE" "
  cd $REMOTE_DIR &&
  docker compose -f docker-compose.eval.yml build --no-cache newsprism-eval &&
  docker compose -f docker-compose.eval.yml up -d
"

echo ""
echo "=== Eval container status ==="
ssh -i "$SSH_KEY" -p "$REMOTE_PORT" "$REMOTE" \
  "cd $REMOTE_DIR && docker compose -f docker-compose.eval.yml ps"

echo ""
echo "=== Recent logs ==="
ssh -i "$SSH_KEY" -p "$REMOTE_PORT" "$REMOTE" \
  "cd $REMOTE_DIR && docker compose -f docker-compose.eval.yml logs --tail=30 newsprism-eval"

echo ""
echo "Eval report will be at: http://192.168.10.5:8081"
```

- [ ] **Step 3: Make deploy script executable**

```bash
chmod +x scripts/deploy-eval.sh
```

- [ ] **Step 4: Verify `docker-compose.eval.yml` syntax**

```bash
docker compose -f docker-compose.eval.yml config > /dev/null && echo "Compose syntax OK"
```

Expected: `Compose syntax OK`

- [ ] **Step 5: Commit**

```bash
git add docker-compose.eval.yml scripts/deploy-eval.sh
git commit -m "feat: add eval Docker setup and deploy script for port 8081"
```

---

## Task 8: Run Full Test Suite and Deploy

- [ ] **Step 1: Run full test suite**

```bash
.venv/bin/pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected: all tests PASS. Fix any failures before deploying.

- [ ] **Step 2: Deploy eval container to fnOS**

```bash
bash scripts/deploy-eval.sh
```

Expected: rsync completes, Docker build succeeds, containers start.

- [ ] **Step 3: Verify eval container is running**

```bash
ssh -i ~/.ssh/fnoskey -p 123 aiagent@192.168.10.5 \
  "cd /vol1/1000/Docker/newsprism-eval && docker compose -f docker-compose.eval.yml ps"
```

Expected: `newsprism-eval` and `newsprism-eval-web` show `Up`.

- [ ] **Step 4: Trigger first collect on eval container**

```bash
ssh -i ~/.ssh/fnoskey -p 123 aiagent@192.168.10.5 \
  "docker exec newsprism-eval python -m newsprism collect 2>&1 | tail -20"
```

Expected: log lines showing articles collected and saved.

- [ ] **Step 5: Trigger first publish on eval container**

```bash
ssh -i ~/.ssh/fnoskey -p 123 aiagent@192.168.10.5 \
  "docker exec newsprism-eval python -m newsprism publish 2>&1 | tail -40"
```

Expected: log lines including `ImpactJudge: N/M clusters retained` and `Rendering report`.

- [ ] **Step 6: Check the eval report renders**

Open `http://192.168.10.5:8081` in a browser. Confirm clusters show `impact_label` values (specific event names like "Gaza Ceasefire Talks") rather than generic keyword labels like "Geopolitics".

- [ ] **Step 7: Run the A/B evaluation harness**

```bash
ssh -i ~/.ssh/fnoskey -p 123 aiagent@192.168.10.5 \
  "cd /vol1/1000/Docker/newsprism-eval && \
   docker exec newsprism-eval python scripts/evaluate_pipeline.py 2>&1 | tail -10"
```

Then copy the output HTML:
```bash
scp -i ~/.ssh/fnoskey -P 123 \
  aiagent@192.168.10.5:/vol1/1000/Docker/newsprism-eval/output/eval-$(date +%Y-%m-%d).html \
  ./output/
open ./output/eval-$(date +%Y-%m-%d).html
```

Expected: HTML report opens with side-by-side A/B comparison and judge LLM verdict.

- [ ] **Step 8: Commit final state**

```bash
git add -A
git commit -m "chore: phase 1 eval deployment complete — A/B harness running on fnOS :8081"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] `http://192.168.10.5:8080` — production report unchanged (still uses old pipeline)
- [ ] `http://192.168.10.5:8081` — eval report shows impact-labeled clusters
- [ ] `cluster_impact` table populates after each publish run
- [ ] `evaluate_pipeline.py` produces `output/eval-YYYY-MM-DD.html` with judge verdict
- [ ] `git worktree list` shows both main and `feature/dynamic-impact`
- [ ] `.venv/bin/pytest tests/ -v` — all tests pass

---

## Follow-up Plans (not in scope here)

- **`SourceDiscoverer`** — `service/discovery.py`, `review-sources` CLI, `pending_sources` approval flow
- **Phase 2 cutover** — remove `filter.py` + `keywords.txt`, update `collect()` to drop TopicTagger, merge to `main`
