#!/usr/bin/env bash
# Compare two env files WITHOUT ever printing a value.
#
# Reports only: the sha256 of each file, the key NAMES present on one side only,
# and the key NAMES whose values differ. Exit: 0 identical, 1 drift, 2 usage.
#
# Used by scripts/deploy.sh. The 2026-09-06 audit found the deploy script printed
# the full env diff (real API keys and bot tokens) to the terminal; this helper
# replaces that with a names-only report.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: check_env_drift.sh <file-a> <file-b>" >&2
  exit 2
fi
FILE_A="$1"
FILE_B="$2"

for f in "$FILE_A" "$FILE_B"; do
  if [[ ! -f "$f" ]]; then
    echo "usage: check_env_drift.sh <file-a> <file-b>" >&2
    echo "error: not a readable file: $f" >&2
    exit 2
  fi
done

hash_of() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | cut -d' ' -f1
  else
    sha256sum "$1" | cut -d' ' -f1
  fi
}

A_HASH="$(hash_of "$FILE_A")"
B_HASH="$(hash_of "$FILE_B")"
echo "sha256 file-a: $A_HASH"
echo "sha256 file-b: $B_HASH"

if [[ "$A_HASH" == "$B_HASH" ]]; then
  echo "env files are identical"
  exit 0
fi

keys_of() {
  grep -oE '^[[:space:]]*(export[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*' "$1" 2>/dev/null \
    | sed -E 's/^[[:space:]]*(export[[:space:]]+)?//' | sort -u || true
}

# Hash the value rather than printing it, so the comparison never leaks secrets.
value_hash_of() {
  local file="$1" key="$2"
  grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$file" 2>/dev/null \
    | head -1 | hash_of /dev/stdin || true
}

ONLY_A="$(comm -23 <(keys_of "$FILE_A") <(keys_of "$FILE_B"))"
ONLY_B="$(comm -13 <(keys_of "$FILE_A") <(keys_of "$FILE_B"))"

CHANGED=""
while IFS= read -r key; do
  [[ -z "$key" ]] && continue
  if [[ -n "$ONLY_A" ]] && grep -qxF "$key" <<<"$ONLY_A"; then continue; fi
  if [[ -n "$ONLY_B" ]] && grep -qxF "$key" <<<"$ONLY_B"; then continue; fi
  if [[ "$(value_hash_of "$FILE_A" "$key")" != "$(value_hash_of "$FILE_B" "$key")" ]]; then
    CHANGED="$CHANGED$key"$'\n'
  fi
done < <(keys_of "$FILE_A")

echo "env drift detected (values are never printed):"
if [[ -n "$ONLY_A" ]]; then
  echo "  keys only in file-a:"
  sed 's/^/    /' <<<"$ONLY_A"
fi
if [[ -n "$ONLY_B" ]]; then
  echo "  keys only in file-b:"
  sed 's/^/    /' <<<"$ONLY_B"
fi
if [[ -n "$CHANGED" ]]; then
  echo "  keys whose values differ:"
  sed '/^$/d; s/^/    /' <<<"$CHANGED"
fi
exit 1
