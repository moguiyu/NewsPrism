"""One-shot migration: legacy zh-only reports get /cn/{date} symlinks and
canonical/og/JSON-LD/neighbor/archive links rewritten to the Chinese edition.

Run inside the newsprism container:
    docker compose -f docker-compose.dev.yml exec -T newsprism python - < scripts/migrate_legacy_to_cn.py
Dual-edition dates (data.json default_language == "en") are skipped.
"""
import json
import re
import shutil
from pathlib import Path

out = Path("output")
cn = out / "cn"
cn.mkdir(exist_ok=True)
base = "https://news.moguiyu.top"
patched = linked = skipped_dual = 0

DATE_HREF = re.compile(r'href="/(\d{4}-\d{2}-\d{2})/"')

for d in sorted(out.iterdir()):
    if not (d.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", d.name)):
        continue
    try:
        dual = json.loads((d / "data.json").read_text(encoding="utf-8")).get("default_language") == "en"
    except Exception:
        dual = False
    if dual:
        skipped_dual += 1
        continue
    link = cn / d.name
    if not link.exists():
        link.symlink_to(f"../{d.name}")
        linked += 1
    f = d / "index.html"
    html = f.read_text(encoding="utf-8")
    orig = html
    # self-referential absolute URLs (canonical / og:url / JSON-LD @id)
    html = html.replace(f"{base}/{d.name}/", f"{base}/cn/{d.name}/")
    # footer neighbour day-links → Chinese edition URLs
    html = DATE_HREF.sub(r'href="/cn/\1/"', html)
    # footer archive link → Chinese archive
    html = html.replace('href="/archive/"', 'href="/cn/archive/"')
    if html != orig:
        f.write_text(html, encoding="utf-8")
        patched += 1

print(f"legacy migrated: linked={linked} patched={patched} dual_skipped={skipped_dual}")
en = out / "en"
if en.exists():
    shutil.rmtree(en)
    print("removed legacy /en dir")
