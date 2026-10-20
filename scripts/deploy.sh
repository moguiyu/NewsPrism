#!/usr/bin/env bash
# Deploy NewsPrism to fnOS (source-build stack: docker-compose.dev.yml).
#
# Safety contract, enforced by tests/test_deploy_script.py:
#   * env drift is reported by scripts/check_env_drift.sh — file hashes and KEY
#     NAMES only, never values (the 2026-09-06 audit found the old script printed
#     the whole diff, real API keys included);
#   * only paths covered by scripts/deploy-excludes.txt are synced, so AGENTS.md,
#     CLAUDE.md, docs/, dist/, .venv, .opencode and .mcp.json (which holds a live
#     API token) never reach the server;
#   * the superseded image is removed BY ID; a host-wide 'docker image prune' is
#     never run, because it also prunes unrelated stacks (metabase/umami/newsnow);
#   * remote files that no longer exist locally are REPORTED, and removed only
#     with --prune (data/ and output/ are protected by the exclusion list).
set -euo pipefail

FNOS_HOST="${FNOS_HOST:-192.168.10.5}"
SSH_PORT=123
SSH_KEY="$HOME/.ssh/fnoskey"
REMOTE_PATH="/vol1/1000/Docker/newsprism"
IMAGE_NAME="newsprism-newsprism"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXCLUDES="$SCRIPT_DIR/deploy-excludes.txt"
DRIFT_CHECK="$SCRIPT_DIR/check_env_drift.sh"
PRODUCTION_ENV="$ROOT/.env.production"

DRY_RUN=0
PRUNE=0

usage() {
  cat <<'USAGE'
usage: bash scripts/deploy.sh [--dry-run] [--prune]

  (no flags)  sync the source tree to fnOS, rebuild the containers, verify them,
              and report remote drift (files on the server no longer in this tree)
  --dry-run   print the commands that would run; change nothing, anywhere
  --prune     also delete those stale remote files

environment:
  FNOS_HOST   fnOS host or Tailscale IP (default 192.168.10.5)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --prune) PRUNE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if [[ ! -f "$EXCLUDES" ]]; then
  echo "error: exclusion list not found: $EXCLUDES" >&2
  exit 2
fi

SSH_TARGET="aiagent@$FNOS_HOST"
RSYNC_SSH="ssh -o ConnectTimeout=10 -i $SSH_KEY -p $SSH_PORT"
RSYNC_ARGS=(-a --exclude-from="$EXCLUDES" -e "$RSYNC_SSH")

plan() { printf 'would run: %s\n' "$*"; }

run_remote() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    plan "ssh $SSH_TARGET $1"
  else
    ssh -o ConnectTimeout=10 -i "$SSH_KEY" -p "$SSH_PORT" "$SSH_TARGET" "$1"
  fi
}

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "DRY RUN — nothing will be written locally or on fnOS"
fi
echo "→ Target: $SSH_TARGET:$REMOTE_PATH"

# ── 1. Env drift: hashes and key names only, never values ─────────────────────
if [[ ! -f "$PRODUCTION_ENV" ]]; then
  echo "⚠️  $PRODUCTION_ENV not found — skipping the env drift check"
elif [[ "$DRY_RUN" -eq 1 ]]; then
  plan "scp $SSH_TARGET:$REMOTE_PATH/.env <tmp> && bash $DRIFT_CHECK $PRODUCTION_ENV <tmp>"
else
  REMOTE_ENV="$(mktemp)"
  trap 'rm -f "$REMOTE_ENV"' EXIT
  chmod 600 "$REMOTE_ENV"
  echo "→ Checking env drift (hashes and key names only; values are never printed)"
  if scp -q -i "$SSH_KEY" -P "$SSH_PORT" "$SSH_TARGET:$REMOTE_PATH/.env" "$REMOTE_ENV"; then
    if ! bash "$DRIFT_CHECK" "$PRODUCTION_ENV" "$REMOTE_ENV"; then
      echo ""
      echo "⚠️  fnOS .env differs from .env.production (names above; values never printed)."
      echo "    Fix it with: scp -i ~/.ssh/fnoskey -P $SSH_PORT .env.production $SSH_TARGET:$REMOTE_PATH/.env"
      read -r -p "Continue the deploy anyway? [y/N] " answer
      if [[ ! "$answer" =~ ^[Yy]$ ]]; then
        echo "Aborted. The host .env was NOT modified."
        exit 1
      fi
    fi
  else
    echo "⚠️  could not fetch the remote .env — skipping the drift check"
  fi
fi

# ── 2. Drift report: files on the server that no longer exist here ────────────
STALE_ARGS=(-n --delete "${RSYNC_ARGS[@]}")
if [[ "$DRY_RUN" -eq 1 ]]; then
  plan "rsync ${STALE_ARGS[*]} $ROOT/ $SSH_TARGET:$REMOTE_PATH/   # report only"
else
  echo "→ Remote drift (files on the server that no longer exist here):"
  STALE_LIST="$(rsync "${STALE_ARGS[@]}" "$ROOT/" "$SSH_TARGET:$REMOTE_PATH/" \
    | awk '$1 == "*deleting" { print $2 }' || true)"
  if [[ -n "$STALE_LIST" ]]; then
    printf '    %s\n' $STALE_LIST
    if [[ "$PRUNE" -eq 1 ]]; then
      echo "    (--prune: these will be deleted by the sync below)"
    else
      echo "    (kept — re-run with --prune to delete them)"
    fi
  else
    echo "    none"
  fi
fi

# ── 3. Sync the source tree ───────────────────────────────────────────────────
if [[ "$PRUNE" -eq 1 ]]; then
  RSYNC_ARGS+=(--delete)
fi
if [[ "$DRY_RUN" -eq 1 ]]; then
  plan "rsync -av ${RSYNC_ARGS[*]} $ROOT/ $SSH_TARGET:$REMOTE_PATH/"
else
  echo "→ Syncing source (exclusions: $(basename "$EXCLUDES"))"
  rsync -av "${RSYNC_ARGS[@]}" "$ROOT/" "$SSH_TARGET:$REMOTE_PATH/"
fi

# ── 4. Rebuild & restart ──────────────────────────────────────────────────────
COMPOSE_UP="cd $REMOTE_PATH && mkdir -p data-beta output-beta && docker compose -f docker-compose.dev.yml up -d --build newsprism newsprism-portal newsprism-beta web"
OLD_IMAGE=""
if [[ "$DRY_RUN" -eq 0 ]]; then
  OLD_IMAGE="$(run_remote "docker images -q $IMAGE_NAME | head -1" || true)"
fi
echo "→ Rebuilding and restarting containers"
run_remote "$COMPOSE_UP"

# ── 5. Remove ONLY the superseded image — never a host-wide prune ─────────────
if [[ "$DRY_RUN" -eq 1 ]]; then
  plan "ssh $SSH_TARGET docker image rm <previous $IMAGE_NAME id>"
else
  NEW_IMAGE="$(run_remote "docker images -q $IMAGE_NAME | head -1" || true)"
  if [[ -n "$OLD_IMAGE" && -n "$NEW_IMAGE" && "$OLD_IMAGE" != "$NEW_IMAGE" ]]; then
    echo "→ Removing superseded image $OLD_IMAGE (this image only)"
    run_remote "docker image rm $OLD_IMAGE" || echo "   (still referenced elsewhere; left in place)"
  else
    echo "→ No superseded image to remove"
  fi
fi

# ── 6. Verify ─────────────────────────────────────────────────────────────────
echo "→ Verifying"
run_remote "cd $REMOTE_PATH && docker compose -f docker-compose.dev.yml ps"
run_remote "cd $REMOTE_PATH && docker compose -f docker-compose.dev.yml logs --tail=30 newsprism"
run_remote "cd $REMOTE_PATH && docker compose -f docker-compose.dev.yml logs --tail=20 newsprism-beta"
