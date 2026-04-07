# Docker Self-Hosting

This guide documents the supported public deployment model for NewsPrism:

- one Linux server
- Docker Engine + Docker Compose plugin
- image-based install with published GHCR images

NewsPrism is designed for a single-host deployment with local persistent volumes. It is not intended for clustered multi-writer operation.

## What Gets Deployed

The default `docker-compose.yml` defines three services:

- `newsprism`: the scheduler and pipeline worker from `ghcr.io/moguiyu/newsprism:latest`
- `web`: nginx serving generated HTML from `output/` from `ghcr.io/moguiyu/newsprism-web:latest`
- `newsnow`: optional but strongly recommended helper service for difficult Chinese sources

Persistent state:

- `data/`: SQLite database
- `output/`: dated reports and `latest/`
- `hf_cache` Docker volume: embedding model downloads

## Server Requirements

- Linux host with Docker Engine and Docker Compose plugin
- enough disk for:
  - SQLite data in `data/`
  - generated reports in `output/`
  - Hugging Face model cache
- outbound internet access for source fetching and your configured LLM/search providers

Recommended for public access:

- a domain name
- a reverse proxy such as Caddy, Nginx, or Traefik
- HTTPS termination in front of the `web` service on port `8080`

## Install

Create an empty deployment directory and download `docker-compose.yml` and `.env.example` from this repository into it.

```bash
cp .env.example .env
```

Edit `.env` and set at least:

- `LITELLM_API_KEY`
- `LITELLM_MODEL`
- `LITELLM_BASE_URL`
- `REPORT_BASE_URL`

For a public deployment behind HTTPS, `REPORT_BASE_URL` should be your final public origin, for example:

```bash
REPORT_BASE_URL=https://news.example.com
```

Start the stack:

```bash
docker compose up -d
docker compose ps
docker compose logs -f newsprism web newsnow
```

If you do not want to wait for the next scheduled publish window, trigger an immediate first run:

```bash
docker compose exec newsprism python -m newsprism once
```

The report server is available at:

- `http://SERVER_IP:8080/` directly
- or your reverse-proxied public domain if you expose it that way

The default image-based install uses the config and templates baked into the application image. If you want editable host-side `config/`, `templates/`, or nginx config files, use the contributor/source-build workflow in `docker-compose.dev.yml`.

## Day-2 Operations

Update to a newer version:

```bash
docker compose pull
docker compose up -d
```

Check service state:

```bash
docker compose ps
docker compose logs -f newsprism web newsnow
```

Trigger one run manually:

```bash
docker compose exec newsprism python -m newsprism once
```

Replay a specific report date:

```bash
docker compose exec newsprism python -m newsprism replay --date 2026-03-14
```

Back up the important state:

- back up `data/`
- back up `output/`

These two paths are sufficient for preserving the database and generated reports for a single-host install.

## Contributor / Source Build

If you want to hack on NewsPrism itself or keep `config/`, `templates/`, and `config/nginx.conf` editable on the host, use the source-build stack from a repo checkout:

```bash
git clone https://github.com/moguiyu/NewsPrism.git
cd NewsPrism

cp .env.example .env
docker compose -f docker-compose.dev.yml up -d --build
```

This contributor stack preserves the original bind mounts for:

- `./config:/app/config`
- `./templates:/app/templates`
- `./config/nginx.conf:/etc/nginx/conf.d/default.conf`

## Safe Customization Surface

Normal self-hosting changes should be done through files, not Python code.

For the default image-based install, these files live inside the image. Use the source-build compose file if you need direct host-side editing.

### `.env`

Use `.env` for deployment- and provider-specific values:

- `LITELLM_API_KEY`, `LITELLM_MODEL`, `LITELLM_BASE_URL`
- `REPORT_BASE_URL`
- `SCHEDULE_TIMEZONE`
- optional Telegram integration
- optional active-search provider keys

### `config/config.yaml`

Use `config/config.yaml` for product behavior:

- `schedule.collect_cron`, `schedule.publish_cron`, `schedule.timezone`
- `collection.*` freshness and request timing
- `filter.min_topic_score`, `filter.max_topics_per_article`
- `clustering.*` thresholds and report size
- `dedup.*` cross-day repetition behavior
- `output.template`
- `output.hot_topics.*`
- `active_search.*`, including `search_profiles` and cost-related knobs
- `sources:` list

### `config/keywords.txt`

Use `config/keywords.txt` to define what the digest cares about.

- Each `# Category Name` line starts a topic group.
- Following non-empty lines are keywords for that group.
- Add local-market company names, political entities, or sector-specific terms here.
- Extend an existing category when the new term is clearly part of that same topic.
- Create a new category only when you want it to behave as a distinct topic family.

### `config/style-guide.md`

Use `config/style-guide.md` for the editorial prompt enforced during summarization.

- adjust tone or summary format requirements
- keep the no-fabrication rules intact
- treat this file as runtime config, not contributor-only docs

### `templates/report-*.html`

Edit the templates if you want to change branding, layout, typography, or card presentation:

- `report-design-a.html`
- `report-design-b.html`
- `report-design-c.html`
- `report-design-premium.html`

Switch the active template in `config/config.yaml` with `output.template`.

## Common Customizations

### 1. Change the schedule

Example: collect every 2 hours and publish at 07:30 local time.

```yaml
schedule:
  collect_cron: "0 */2 * * *"
  publish_cron: "30 7 * * *"
  timezone: "Europe/Warsaw"
```

You can also override timezone with:

```bash
SCHEDULE_TIMEZONE=Asia/Singapore
```

### 2. Narrow the digest to specific themes

If you only want AI, chips, and geopolitics:

- remove unrelated categories from `config/keywords.txt`
- or keep them but raise `filter.min_topic_score`

Example:

```yaml
filter:
  min_topic_score: 0.2
  max_topics_per_article: 3
```

Then add your own high-signal local terms to `config/keywords.txt`, for example:

```text
# AI & LLM
OpenAI
Anthropic
your-local-ai-company

# Chips & Hardware
NVIDIA
ASML
your-local-semiconductor-brand
```

### 3. Disable or target specific sources

Every source entry under `sources:` can be tuned.

Common fields:

- `enabled`
- `weight`
- `tier`
- `region`
- `language`
- `perspective`

Disable one source:

```yaml
- name: Reuters
  enabled: false
```

Run a lighter or region-specific deployment by disabling large sections of the `sources:` list you do not need.

When adding a new source:

- prefer RSS first
- use `type: rss` when possible
- set `tier` intentionally:
  - `editorial`: broad news editorial coverage, bypasses keyword gate
  - `tech`: tech-focused source, also bypasses keyword gate
  - `portal`: mixed-content source, still filtered by keyword matching

### 4. Change the output style

Switch templates in `config/config.yaml`:

```yaml
output:
  template: "design-a"
```

Supported values:

- `design-a`
- `design-b`
- `design-c`
- `design-premium`

### 5. Reduce search cost or external dependencies

If you want a simpler install:

- leave `TAVILY_API_KEY`, `BRIGHTDATA_API_KEY`, `X_BEARER_TOKEN`, and `YOUTUBE_API_KEY` unset
- tune `active_search.max_results_per_region`
- tune the fallback behavior in `active_search.search_profiles`

If you want to disable Telegram:

- leave `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` unset

### 6. Disable `newsnow`

`newsnow` is optional at the Compose level, but coverage for several difficult Chinese sources will degrade without it.

If you disable it:

- remove or comment out the `newsnow` service
- remove the `depends_on` entry from `newsprism`
- set `NEWSNOW_BASE_URL` only if you run `newsnow` somewhere else

This is acceptable for English-only or reduced-source deployments, but not recommended for the full default source catalog.

## Troubleshooting

### `unable to evaluate symlinks in Dockerfile path`

This error happens when a stack UI or NAS tries to run a compose service with `build: .`, but the compose project directory does not actually contain the NewsPrism repository and its `Dockerfile`.

The default `docker-compose.yml` in this repo no longer requires a local Docker build. If you still see this error, you are likely using an older compose file or a custom stack definition. Use the published-image install path:

```bash
docker compose up -d
```

Only use `docker-compose.dev.yml` when you intentionally cloned the full repository and want a source build.

## Limitations

- single-host SQLite deployment only
- not designed for clustered multi-writer use
- some source quality depends on `newsnow`
- search completeness depends on which optional provider keys you configure
- `REPORT_BASE_URL` should reflect the real public URL if you use Telegram or public links
