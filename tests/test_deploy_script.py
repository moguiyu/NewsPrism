from pathlib import Path


def test_deploy_prunes_unused_images_after_rebuild_before_verification():
    script = Path("scripts/deploy.sh").read_text(encoding="utf-8")

    rebuild = "docker compose -f docker-compose.dev.yml up -d --build"
    prune = "docker image prune -f"
    verify = "docker compose -f docker-compose.dev.yml ps"

    assert rebuild in script
    assert prune in script
    assert verify in script
    assert script.index(rebuild) < script.index(prune) < script.index(verify)
