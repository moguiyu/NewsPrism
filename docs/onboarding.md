# Contributor Guide

This guide is the fastest path to a working local NewsPrism environment.

For single-server self-hosting with Docker Compose, see `docs/deploy-docker.md`.

## Requirements

- Python 3.11 or newer
- `pip`
- Optional: Docker / Docker Compose for local `newsnow` and HTML serving

## Setup

```bash
git clone https://github.com/moguiyu/NewsPrism.git
cd NewsPrism

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]

cp .env.example .env
```

Fill in the required LiteLLM variables in `.env`. Telegram and active-search provider tokens are optional for most local work.

## Common Commands

```bash
pytest
python -m build
python -m newsprism collect
python -m newsprism publish
python -m newsprism once
python -m newsprism replay --date 2026-03-14 --dry-run
```

Optional local services:

```bash
docker compose up -d newsnow
docker compose up -d web
```

`newsnow` helps with hard-to-fetch Chinese sources. The `web` service serves generated output from `http://localhost:8080`.

## Project Layout

```text
config/          Source catalog, thresholds, schedules, keywords, style guide, nginx config
docs/            Public design notes, contributor docs, schema reference
newsprism/       Python package
templates/       HTML report templates
tests/           Unit and regression tests
scripts/         Local utility scripts
```

## Contribution Expectations

1. Keep the layer DAG intact: `types -> config -> repo -> service -> runtime`.
2. Define shared dataclasses in `newsprism/types.py`.
3. Put thresholds, schedules, and source configuration in `config/config.yaml`, not inline in code.
4. Update `docs/generated/db-schema.md` when the SQLite schema changes.
5. Keep sources in `config/config.yaml`; do not hardcode source lists in Python.
6. Preserve the no-fabrication rules in `config/style-guide.md`.

## Useful Files

| File | Why it matters |
|---|---|
| `docs/design-docs/core-beliefs.md` | Product philosophy and editorial goals |
| `docs/design-docs/decisions.md` | High-level technical and product decisions |
| `docs/generated/db-schema.md` | Current database schema |
| `config/config.yaml` | Sources, schedules, clustering rules, output template |
| `config/keywords.txt` | Topic taxonomy |

## Optional Git Hook

The repository includes a lightweight pre-commit hook that blocks accidental `.env` commits:

```bash
git config core.hooksPath .githooks
```
