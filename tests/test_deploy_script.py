"""Guards for scripts/deploy.sh — safety contract from the 2026-09-06 audit.

The deploy script syncs the working tree to fnOS and rebuilds containers. That
audit found three hazards, each encoded here:

1. env drift was reported by diffing the two env files, which printed secret
   VALUES to the terminal;
2. the rsync exclude list did not cover every local-only artifact, so
   AGENTS.md / CLAUDE.md / docs/ / dist/ / .venv / .opencode / .mcp.json (which
   carries a live API token) were copied to the server;
3. a host-wide image prune removed dangling images for unrelated stacks
   (metabase, umami, newsnow).

This replaces the earlier test that asserted the host-wide prune existed.
"""
from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path("scripts/deploy.sh")
EXCLUDES = Path("scripts/deploy-excludes.txt")
DRIFT = Path("scripts/check_env_drift.sh")


def run(args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, **kwargs)


def _code_lines(path):
    """Executable lines only: comments and blank lines cannot run a command."""
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            out.append(stripped)
    return out


def _exclude_patterns():
    lines = EXCLUDES.read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip() and not line.startswith("#")}


def test_script_is_syntactically_valid():
    assert run(["bash", "-n", str(SCRIPT)]).returncode == 0


def test_help_prints_usage_and_takes_no_action():
    result = run(["bash", str(SCRIPT), "--help"])
    assert result.returncode == 0
    out = result.stdout + result.stderr
    assert "usage" in out.lower()
    for action in ("Syncing source", "Rebuilding", "rsync"):
        assert action not in out


def test_unknown_argument_fails_with_usage():
    result = run(["bash", str(SCRIPT), "--nope"])
    assert result.returncode != 0
    assert "usage" in (result.stdout + result.stderr).lower()


def test_dry_run_prints_planned_mutations_without_executing_them():
    result = run(["bash", str(SCRIPT), "--dry-run"])
    assert result.returncode == 0
    out = result.stdout + result.stderr
    assert "DRY RUN" in out
    assert "would run:" in out
    assert "rsync" in out
    assert "up -d --build" in out


def test_never_runs_a_host_wide_image_prune():
    code = " ".join(_code_lines(SCRIPT))
    assert "image prune" not in code, "a host-wide prune also hits unrelated stacks"
    assert "docker image rm" in code, "the superseded image must be removed by id"


def test_superseded_image_is_removed_after_rebuild_and_before_verification():
    script = SCRIPT.read_text(encoding="utf-8")
    rebuild = "up -d --build"
    removal = "docker image rm"
    verify = "logs --tail"
    assert rebuild in script and removal in script and verify in script
    assert script.index(rebuild) < script.index(removal) < script.rindex(verify)


def test_env_drift_goes_through_the_helper_and_never_diffs_raw_files():
    script = SCRIPT.read_text(encoding="utf-8")
    assert "check_env_drift.sh" in script
    assert 'diff "$PRODUCTION_ENV"' not in script
    # the remote .env may be fetched, but never dumped to stdout
    for line in _code_lines(SCRIPT):
        if "REMOTE_PATH/.env" in line and "scp" not in line:
            assert ">" in line, f"remote .env echoed to stdout: {line}"


def test_script_uses_the_shared_exclude_file():
    assert "--exclude-from" in SCRIPT.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "pattern",
    [
        ".git/", ".venv/", "data/", "output/", "__pycache__/", ".pytest_cache/",
        ".env", ".env.production", ".env.bak*",
        "AGENTS.md", "CLAUDE.md", "docs/", "dist/", ".agents/", ".superpowers/",
        ".remember/", ".mcp.json", ".opencode/", ".openspec/", ".claude/",
        ".vscode/", ".zcode/", ".worktrees/", ".ruff_cache/", ".firecrawl/",
        ".DS_Store", ".playwright-mcp/",
        "/*.png", "/banner-*.png", "/eval-report-*.png",
    ],
)
def test_exclude_list_covers_local_only_artifacts(pattern):
    assert pattern in _exclude_patterns(), f"{pattern} would be shipped to the server"


def test_screenshots_are_excluded_by_anchored_patterns_only():
    """A bare '*.png' would stop the app's own icons from ever being deployed."""
    assert "*.png" not in _exclude_patterns()


@pytest.mark.parametrize(
    "required", ["newsprism/", "config/", "templates/", "pyproject.toml", "README.md"]
)
def test_required_build_inputs_are_not_excluded(required):
    assert required not in _exclude_patterns()


def test_env_drift_helper_never_prints_secret_values(tmp_path):
    local = tmp_path / "local.env"
    remote = tmp_path / "remote.env"
    local.write_text(
        "TELEGRAM_BOT_TOKEN=111:AAA-secret-value\n"
        "SHARED_KEY=only-in-local\n"
        "LITELLM_API_KEY=sk-identical-both-sides\n",
        encoding="utf-8",
    )
    remote.write_text(
        "TELEGRAM_BOT_TOKEN=222:BBB-other-secret\n"
        "SHARED_KEY=only-in-remote\n"
        "LITELLM_API_KEY=sk-identical-both-sides\n",
        encoding="utf-8",
    )

    result = run(["bash", str(DRIFT), str(local), str(remote)])
    out = result.stdout + result.stderr

    assert result.returncode == 1, "drift must be reported as a non-zero exit"
    assert "TELEGRAM_BOT_TOKEN" in out, "a changed key must be named"
    assert "SHARED_KEY" in out, "a differing key must be named"
    for secret in (
        "111:AAA-secret-value",
        "222:BBB-other-secret",
        "sk-identical-both-sides",
        "only-in-local",
        "only-in-remote",
    ):
        assert secret not in out, f"secret value leaked: {secret}"
    assert "LITELLM_API_KEY" not in out, "identical values must not be reported"


def test_env_drift_helper_reports_identical_files(tmp_path):
    env_file = tmp_path / "same.env"
    env_file.write_text("A=1\nB=2\n", encoding="utf-8")
    result = run(["bash", str(DRIFT), str(env_file), str(env_file)])
    assert result.returncode == 0
    assert "identical" in (result.stdout + result.stderr).lower()


def test_env_drift_helper_requires_two_arguments():
    result = run(["bash", str(DRIFT), "only-one-arg"])
    assert result.returncode != 0
    assert "usage" in (result.stdout + result.stderr).lower()


def test_env_drift_helper_is_invoked_with_quoted_paths():
    """Both paths must be quoted: an unquoted path with spaces would silently
    compare the wrong file, or none at all."""
    line = next(l for l in _code_lines(SCRIPT) if l.strip().startswith("if ! bash"))
    assert '"$DRIFT_CHECK"' in line
    assert '"$PRODUCTION_ENV"' in line
    assert '"$REMOTE_ENV"' in line
    assert line.strip().startswith("if ! bash"), line
