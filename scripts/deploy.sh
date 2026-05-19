#!/usr/bin/env bash
# Deploy NewsPrism to fnOS.
# Diffs local .env.production against remote .env before syncing — aborts if
# they diverge and the user does not confirm.
set -euo pipefail

SSH="ssh -i ~/.ssh/fnoskey -p 123 aiagent@192.168.10.5"
REMOTE_PATH="/vol1/1000/Docker/newsprism"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── 1. Env drift check ────────────────────────────────────────────────────────
PRODUCTION_ENV="$ROOT/.env.production"

if [[ ! -f "$PRODUCTION_ENV" ]]; then
  echo "⚠️  .env.production not found — skipping env diff check."
  echo "   Run: scp -i ~/.ssh/fnoskey -P 123 aiagent@192.168.10.5:$REMOTE_PATH/.env .env.production"
else
  REMOTE_ENV=$(mktemp)
  trap 'rm -f "$REMOTE_ENV"' EXIT
  $SSH "cat $REMOTE_PATH/.env" > "$REMOTE_ENV"

  if ! diff -q "$PRODUCTION_ENV" "$REMOTE_ENV" > /dev/null 2>&1; then
    echo "⚠️  fnOS .env differs from local .env.production:"
    echo "────────────────────────────────────────────────"
    diff "$PRODUCTION_ENV" "$REMOTE_ENV" || true
    echo "────────────────────────────────────────────────"
    echo ""
    read -r -p "Continue deploy anyway? [y/N] " answer
    if [[ ! "$answer" =~ ^[Yy]$ ]]; then
      echo "Aborted. Fix the drift first:"
      echo "  • Update .env.production to match what fnOS should have, then:"
      echo "    scp -i ~/.ssh/fnoskey -P 123 .env.production aiagent@192.168.10.5:$REMOTE_PATH/.env"
      exit 1
    fi
  else
    echo "✓ fnOS .env matches .env.production"
  fi
fi

# ── 2. Rsync source ───────────────────────────────────────────────────────────
echo ""
echo "→ Syncing source to fnOS..."
rsync -av \
  -e "ssh -i ~/.ssh/fnoskey -p 123" \
  --exclude .git \
  --exclude .venv \
  --exclude data \
  --exclude output \
  --exclude __pycache__ \
  --exclude .pytest_cache \
  --exclude .worktrees \
  --exclude .claude \
  --exclude .env \
  --exclude .env.production \
  "$ROOT/" aiagent@192.168.10.5:"$REMOTE_PATH/"

# ── 3. Rebuild & restart ──────────────────────────────────────────────────────
echo ""
echo "→ Rebuilding and restarting containers..."
$SSH "cd $REMOTE_PATH && docker compose -f docker-compose.dev.yml up -d --build"

# ── 4. Verify ─────────────────────────────────────────────────────────────────
echo ""
echo "→ Verifying..."
$SSH "cd $REMOTE_PATH && docker compose -f docker-compose.dev.yml ps && docker compose -f docker-compose.dev.yml logs --tail=30 newsprism"
