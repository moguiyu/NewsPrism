from pathlib import Path

from scripts.rewrite_report_base_url import rewrite_report_base_url


def test_report_url_migration_is_dry_run_first_and_preserves_chinese_logo_route(tmp_path):
    """Backfill changes only generated metadata after an explicit apply."""
    report = tmp_path / "cn" / "2026-08-25" / "index.html"
    report.parent.mkdir(parents=True)
    report.write_text(
        """<html lang=\"zh-CN\"><head>
<link rel=\"canonical\" href=\"https://news.moguiyu.top/cn/2026-08-25/\" />
</head><body><a class=\"logo\" href=\"./\" onclick=\"window.location.reload(); return false;\">NewsPrism</a></body></html>""",
        encoding="utf-8",
    )
    sitemap = tmp_path / "sitemap.xml"
    sitemap.write_text("https://news.moguiyu.top/cn/2026-08-25/", encoding="utf-8")
    untouched = tmp_path / "2026-08-25" / "data.json"
    untouched.parent.mkdir(parents=True)
    untouched.write_text('{"canonical":"https://news.moguiyu.top"}', encoding="utf-8")

    dry_run = rewrite_report_base_url(
        tmp_path,
        from_base_url="https://news.moguiyu.top",
        to_base_url="https://news.grayzhang.com",
        apply=False,
    )

    assert dry_run.changed_files == [report, sitemap]
    assert "news.moguiyu.top" in report.read_text(encoding="utf-8")
    assert untouched.read_text(encoding="utf-8") == '{"canonical":"https://news.moguiyu.top"}'

    applied = rewrite_report_base_url(
        tmp_path,
        from_base_url="https://news.moguiyu.top",
        to_base_url="https://news.grayzhang.com",
        apply=True,
    )

    assert applied.changed_files == [report, sitemap]
    migrated_html = report.read_text(encoding="utf-8")
    assert "https://news.grayzhang.com/cn/2026-08-25/" in migrated_html
    assert 'class="logo" href="/cn/"' in migrated_html
    assert "window.location.reload" not in migrated_html
    assert sitemap.read_text(encoding="utf-8") == "https://news.grayzhang.com/cn/2026-08-25/"

    assert rewrite_report_base_url(
        tmp_path,
        from_base_url="https://news.moguiyu.top",
        to_base_url="https://news.grayzhang.com",
        apply=True,
    ).changed_files == []
