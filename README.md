# NewsPrism

NewsPrism is a multilingual news monitor that collects articles from 38 sources across 11 countries, clusters same-event coverage, and publishes a Chinese digest that highlights how different outlets frame the same story.

```text
Collect articles  ->  Tag + dedup  ->  Cluster events  ->  Summarize angles  ->  Render HTML / publish
```

This public repository supports both self-hosting with Docker and local development from source.

## Highlights

- Multilingual collection across Chinese, English, Japanese, Korean, Russian, Polish, Dutch, and more
- Event-level clustering with same-day and cross-day deduplication
- Perspective-seeking for missing regional angles
- HTML report rendering with a premium story-first layout
- CLI entrypoints for collection, publish, replay, and scheduler runs

## Architecture

```text
newsprism/
├── types.py          Shared dataclasses and typed records
├── config.py         YAML + environment loader
├── repo/             SQLite persistence
├── service/          collect, filter, dedup, cluster, summarize
└── runtime/          schedule, render, publish
```

Layer rule: `types -> config -> repo -> service -> runtime`. Higher layers must not be imported downward.

## Self-Host With Docker

NewsPrism supports a single-server Docker Compose deployment with published GHCR images. The default install path only needs `docker-compose.yml` and `.env`; it does not require a local `Dockerfile` or a full repo checkout.

```bash
# Download docker-compose.yml and .env.example from this repo into an empty directory
cp .env.example .env
# Fill in at least LITELLM_API_KEY, LITELLM_MODEL, LITELLM_BASE_URL, REPORT_BASE_URL

# Start the published images
docker compose up -d

# Optional: trigger the first run immediately instead of waiting for cron
docker compose exec newsprism python -m newsprism once
```

The default stack includes:

- `newsprism`: scheduler and pipeline worker
- `web`: static report server on `http://localhost:8080`
- `newsnow`: optional-but-recommended helper for difficult Chinese sources

The default image-based install uses the config and templates bundled inside the `newsprism` image. If you want editable host-side `config/`, `templates/`, or nginx config files, use the contributor/source-build stack in `docker-compose.dev.yml`.

Full server install, update, backup, and customization guidance lives in `docs/deploy-docker.md`.

## Develop Locally

```bash
git clone https://github.com/moguiyu/NewsPrism.git
cd NewsPrism

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]

cp .env.example .env
# Fill in at least LITELLM_API_KEY, LITELLM_MODEL, and LITELLM_BASE_URL

python -m newsprism collect
python -m newsprism once
```

Optional helper services:

- `docker compose -f docker-compose.dev.yml up -d newsnow` starts a local `newsnow` proxy for harder Chinese sources.
- `docker compose -f docker-compose.dev.yml up -d web` serves generated HTML reports from `http://localhost:8080`.

## Environment Variables

Required for summarization:

| Variable | Purpose |
|---|---|
| `LITELLM_API_KEY` | API key for your OpenAI-compatible LLM provider |
| `LITELLM_MODEL` | LiteLLM model identifier used for story summaries |
| `LITELLM_BASE_URL` | Provider base URL |
| `REPORT_BASE_URL` | Public or local base URL used in rendered report links |

Optional:

| Variable | Purpose |
|---|---|
| `EVALUATOR_MODEL` | Separate model for active-search evaluation |
| `NEWSNOW_BASE_URL` | External `newsnow` endpoint if not using the bundled compose service |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Telegram publishing |
| `TAVILY_API_KEY` / `BRIGHTDATA_API_KEY` | Active search providers |
| `X_BEARER_TOKEN` / `YOUTUBE_API_KEY` | Official social fallback providers |
| `SCHEDULE_TIMEZONE` | Override scheduler timezone without editing YAML |

## Customization

Self-hosters are expected to customize the installation through files, not Python code.

For the default image-based install, those files live inside the container image. Use `docker-compose.dev.yml` from a repo checkout if you want to edit `config/`, `templates/`, or `config/nginx.conf` directly on the host.

| Surface | What you can change |
|---|---|
| `.env` | provider keys, public report URL, scheduler timezone, optional integrations |
| `config/config.yaml` | schedule, source list, clustering thresholds, dedup rules, output template, active-search behavior |
| `config/keywords.txt` | topic groups and keyword filters |
| `config/style-guide.md` | editorial prompt and no-fabrication rules used by the summarizer |
| `templates/report-*.html` | HTML branding and presentation |

Common examples:

- disable sources by setting `enabled: false` under `sources:`
- switch `output.template` from `design-premium` to `design-a`, `design-b`, or `design-c`
- edit `schedule.collect_cron` and `schedule.publish_cron`
- tighten or broaden topic matching in `config/keywords.txt` and `filter.min_topic_score`

## CLI Commands

| Command | Purpose |
|---|---|
| `python -m newsprism collect` | Fetch and store fresh articles |
| `python -m newsprism publish` | Cluster and publish the current report |
| `python -m newsprism once` | Run collection and publish in one pass |
| `python -m newsprism replay --date YYYY-MM-DD` | Rebuild one report date from its saved article set |
| `python -m newsprism run` | Start the long-running scheduler |

## Testing and Packaging

```bash
pytest
python -m build
```

The default test suite is designed to run without private infrastructure or deployment secrets.

## Configuration Surface

Most behavior is file-based:

| File | Purpose |
|---|---|
| `config/config.yaml` | Sources, schedules, thresholds, template selection |
| `config/keywords.txt` | Topic taxonomy and keyword mapping |
| `config/style-guide.md` | Editorial prompt and no-fabrication constraints |
| `docs/generated/db-schema.md` | Current SQLite schema reference |

## Project Docs

| Topic | File |
|---|---|
| Docker self-hosting | `docs/deploy-docker.md` |
| Contributor setup | `docs/onboarding.md` |
| Design philosophy | `docs/design-docs/core-beliefs.md` |
| Key decisions | `docs/design-docs/decisions.md` |
| Database schema | `docs/generated/db-schema.md` |
| Contributing process | `CONTRIBUTING.md` |
| Security reporting | `SECURITY.md` |

## Deployment Notes

- Supported public deployment target: one Linux server with Docker Compose
- Default self-hosting path uses published GHCR images; no local source checkout is required
- Persistence lives in `data/`, `output/`, and the Hugging Face cache volume
- SQLite is intended for single-host use; this repo does not target clustered multi-writer deployments
- For public internet exposure, run a reverse proxy with HTTPS in front of the `web` service and set `REPORT_BASE_URL` accordingly
- Some source coverage, especially difficult Chinese sites, is materially better when `newsnow` is enabled

## License

Released under the MIT License. See `LICENSE`.
