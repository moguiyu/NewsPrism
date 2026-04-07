# Security Policy

## Supported Versions

Security fixes are currently targeted at the latest code on `main`.

## Reporting a Vulnerability

Do not open a public GitHub issue for security-sensitive reports.

Preferred path:

1. Use GitHub's private vulnerability reporting feature for this repository, if it is enabled.
2. If that feature is not available yet, contact the maintainer privately through GitHub instead of filing a public issue.

Please include:

- A clear description of the issue
- Impact and affected components
- Reproduction steps or a proof of concept
- Any suggested mitigation if you have one

We will acknowledge valid reports as quickly as practical and coordinate on disclosure before publishing a fix.

## Credential Management

NewsPrism requires several API keys to operate. **Never commit `.env` to version control.**

### Required credentials (`.env`)

| Variable | Service | Rotate when |
|---|---|---|
| `LITELLM_API_KEY` | DeepSeek / OpenAI-compatible LLM | Annually or on suspected exposure |
| `TAVILY_API_KEY` | Tavily web search | Annually or on suspected exposure |
| `BRIGHTDATA_API_KEY` | BrightData proxy | Annually or on suspected exposure |
| `X_BEARER_TOKEN` | Twitter/X API | Annually or on suspected exposure |
| `TELEGRAM_BOT_TOKEN` | Telegram delivery | On bot re-creation or suspected exposure |

### Key rotation procedure

1. Generate a new key from the respective provider dashboard
2. Update the `.env` file on your deployment server
3. Restart the containers: `docker compose up -d --force-recreate newsprism`
4. Verify the service is healthy: `docker compose ps`
5. Revoke the old key from the provider dashboard

### If a key is accidentally committed

1. **Immediately revoke** the exposed key from the provider dashboard
2. Generate and deploy a replacement key
3. Remove the key from git history using [BFG Repo-Cleaner](https://rtyley.github.io/bfg-repo-cleaner/) or `git filter-repo`
4. Force-push the cleaned history and notify affected service providers
