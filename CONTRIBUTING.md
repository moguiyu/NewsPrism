# Contributing to NewsPrism

Thanks for contributing.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
cp .env.example .env
```

Fill in the required LiteLLM variables in `.env` before running end-to-end commands.

## Development Workflow

1. Branch from `main`.
2. Keep changes focused and small enough to review clearly.
3. Add or update tests when behavior changes.
4. Update docs when user-facing behavior, config, or schema expectations change.
5. Open a pull request with a clear summary and verification notes.

## Commands

```bash
pytest
python -m build
python -m newsprism collect
python -m newsprism publish
python -m newsprism once
```

Optional local services:

```bash
docker compose up -d newsnow
docker compose up -d web
```

## Project Rules

- Preserve the layer DAG: `types -> config -> repo -> service -> runtime`.
- Keep shared dataclasses in `newsprism/types.py`.
- Keep thresholds, schedules, and source definitions in `config/config.yaml`.
- Update `docs/generated/db-schema.md` when the database schema changes.
- Add or remove sources only through `config/config.yaml`.
- Preserve the no-fabrication constraints in `docs/product-specs/style-guide.md`.

## Pull Request Checklist

- Tests added or updated when behavior changed
- `pytest` passes locally
- `python -m build` succeeds locally
- Docs updated when needed
- New config, schema, or CLI behavior is reflected in the relevant public docs
