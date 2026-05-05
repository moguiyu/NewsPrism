"""Read-only quality audit for source, selection, and rendered report health."""
from __future__ import annotations

import json
import re
import sqlite3
import urllib.parse
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from newsprism.config import load_config


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _date_window(days: int, anchor: str | None) -> tuple[str, str]:
    end = date.fromisoformat(anchor) if anchor else date.today()
    start = end - timedelta(days=max(days - 1, 0))
    return start.isoformat(), end.isoformat()


def _load_rendered_reports(output_dir: Path, start: str, end: str) -> list[tuple[str, dict[str, Any], str]]:
    reports: list[tuple[str, dict[str, Any], str]] = []
    for data_path in sorted(output_dir.glob("*/data.json")):
        report_date = data_path.parent.name
        if report_date == "latest" or report_date < start or report_date > end:
            continue
        html = ""
        html_path = data_path.parent / "index.html"
        if html_path.exists():
            html = html_path.read_text(encoding="utf-8", errors="ignore")
        try:
            reports.append((report_date, json.loads(data_path.read_text(encoding="utf-8")), html))
        except json.JSONDecodeError:
            continue
    return reports


def _is_generic_or_stale_article(article: dict[str, Any]) -> str | None:
    url = str(article.get("url") or "")
    title = str(article.get("title") or "")
    path = urllib.parse.urlparse(url).path.lower()
    if re.search(r"/(author|authors|company|companies|topic|topics|tag|tags|search|archive|archives|word)(/|$)", path):
        return "generic_page"
    if re.search(r"\|\s*Reuters\s*$|latest news|最新ニュース|最新消息", title, re.IGNORECASE):
        return "generic_page"
    if re.search(r"2025|202501", url + title):
        return "stale_or_historical"
    return None


def _configured_source_tiers() -> dict[str, str]:
    try:
        return {source.name: source.tier for source in load_config().sources}
    except Exception:
        return {}


def _source_tier(row: sqlite3.Row | dict[str, Any], configured_tiers: dict[str, str]) -> str:
    source_name = str(row["source_name"] if isinstance(row, sqlite3.Row) else row.get("source_name") or "")
    if source_name in configured_tiers:
        return configured_tiers[source_name]
    source_kind = str(row["source_kind"] if isinstance(row, sqlite3.Row) else row.get("source_kind") or "news")
    if source_kind != "news":
        return source_kind
    is_official = bool(row["is_official_source"] if isinstance(row, sqlite3.Row) else row.get("is_official_source"))
    if is_official:
        return "official"
    is_searched = bool(row["is_searched"] if isinstance(row, sqlite3.Row) else row.get("is_searched"))
    if is_searched:
        return "active_search"
    return "unknown"


def audit(days: int = 10, anchor_date: str | None = None, db_path: str | Path = "data/newsprism.db", output_dir: str | Path = "output") -> dict[str, Any]:
    db = Path(db_path)
    output = Path(output_dir)
    start, end = _date_window(days, anchor_date)
    result: dict[str, Any] = {
        "window": {"start": start, "end": end, "days": days},
        "db": {},
        "rendered_reports": [],
        "issues": Counter(),
    }

    configured_tiers = _configured_source_tiers()
    if db.exists():
        with _connect(db) as conn:
            search_event_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(search_request_events)").fetchall()
            }
            rejection_reason_select = (
                "rejection_reason" if "rejection_reason" in search_event_columns else "NULL AS rejection_reason"
            )
            rejection_count_select = (
                "rejection_count" if "rejection_count" in search_event_columns else "NULL AS rejection_count"
            )
            article_rows = conn.execute(
                """SELECT id, source_name, published_at, origin_region, source_kind,
                          is_official_source, is_searched, searched_provider
                   FROM articles
                   WHERE date(published_at) BETWEEN date(?) AND date(?)""",
                (start, end),
            ).fetchall()
            cluster_rows = conn.execute(
                "SELECT report_date, topic_category, article_ids, summary, perspectives FROM clusters WHERE report_date BETWEEN ? AND ?",
                (start, end),
            ).fetchall()
            search_event_rows = conn.execute(
                f"""SELECT provider, request_type, target_region, result_count, accepted_count,
                          {rejection_reason_select}, {rejection_count_select}, estimated_cost_usd, created_at
                   FROM search_request_events
                   WHERE date(created_at) BETWEEN date(?) AND date(?)""",
                (start, end),
            ).fetchall()
        raw_by_source = Counter(row["source_name"] for row in article_rows)
        raw_by_region = Counter(row["origin_region"] or "unknown" for row in article_rows)
        raw_by_tier = Counter(_source_tier(row, configured_tiers) for row in article_rows)
        searched_by_provider = Counter(row["searched_provider"] or "unknown" for row in article_rows if row["is_searched"])
        articles_by_id = {int(row["id"]): row for row in article_rows}
        selected_by_source: Counter[str] = Counter()
        selected_by_region: Counter[str] = Counter()
        selected_by_tier: Counter[str] = Counter()
        topic_mix = Counter(row["topic_category"] for row in cluster_rows)
        for row in cluster_rows:
            try:
                perspectives = json.loads(row["perspectives"] or "{}")
            except json.JSONDecodeError:
                perspectives = {}
            if len({str(value).strip() for value in perspectives.values() if str(value).strip()}) <= 1:
                result["issues"]["db_one_angle_cluster"] += 1
            try:
                ids = json.loads(row["article_ids"] or "[]")
            except json.JSONDecodeError:
                ids = []
            for article_id in ids:
                article_row = articles_by_id.get(int(article_id))
                if article_row is None:
                    continue
                selected_by_source.update([article_row["source_name"]])
                selected_by_region.update([article_row["origin_region"] or "unknown"])
                selected_by_tier.update([_source_tier(article_row, configured_tiers)])
        search_by_provider_type = Counter(
            f"{row['provider']}:{row['request_type']}" for row in search_event_rows
        )
        search_by_region = Counter(row["target_region"] or "unknown" for row in search_event_rows)
        rejection_reasons: Counter[str] = Counter()
        for row in search_event_rows:
            reason = row["rejection_reason"]
            if reason:
                rejection_reasons[reason] += int(row["rejection_count"] or 1)
        result["db"] = {
            "raw_article_count": len(article_rows),
            "cluster_count": len(cluster_rows),
            "raw_by_source": raw_by_source.most_common(25),
            "raw_by_region": raw_by_region.most_common(25),
            "raw_by_tier": raw_by_tier.most_common(25),
            "topic_mix": topic_mix.most_common(25),
            "selected_by_source": selected_by_source.most_common(25),
            "selected_by_region": selected_by_region.most_common(25),
            "selected_by_tier": selected_by_tier.most_common(25),
            "selected_article_reference_count": sum(selected_by_source.values()),
            "searched_by_provider": searched_by_provider.most_common(25),
            "search_request_count": len(search_event_rows),
            "search_by_provider_type": search_by_provider_type.most_common(25),
            "search_by_region": search_by_region.most_common(25),
            "active_search_rejection_reasons": rejection_reasons.most_common(25),
            "estimated_search_cost_usd": round(
                sum(float(row["estimated_cost_usd"] or 0.0) for row in search_event_rows), 6
            ),
        }

    for report_date, payload, html in _load_rendered_reports(output, start, end):
        clusters = payload.get("clusters") or []
        one_angle_multi = 0
        generic_or_stale = []
        duplicate_actions = Counter()
        topic_mix = Counter()
        for cluster in clusters:
            topic_mix[str(cluster.get("topic") or "")] += 1
            duplicate_actions[str(cluster.get("duplicate_action") or "kept")] += 1
            if cluster.get("is_multi") and int(cluster.get("distinct_perspective_count") or 0) <= 1:
                one_angle_multi += 1
            for article in cluster.get("articles") or []:
                status = article.get("search_acceptance_status")
                reason = article.get("search_acceptance_reason")
                if status == "rejected" and reason:
                    result["issues"][f"rendered_search_rejected_{reason}"] += 1
                issue = _is_generic_or_stale_article(article)
                if issue:
                    generic_or_stale.append(
                        {
                            "cluster_index": cluster.get("index"),
                            "headline": cluster.get("headline"),
                            "source": article.get("source"),
                            "url": article.get("url"),
                            "issue": issue,
                        }
                    )
        markdown_leaks = html.count("**") if html else 0
        result["issues"]["rendered_one_angle_multi_source"] += one_angle_multi
        result["issues"]["rendered_generic_or_stale_article"] += len(generic_or_stale)
        result["issues"]["rendered_markdown_leak"] += markdown_leaks
        result["rendered_reports"].append(
            {
                "date": report_date,
                "cluster_count": len(clusters),
                "one_angle_multi_source": one_angle_multi,
                "one_angle_multi_source_rate": round(one_angle_multi / len(clusters), 4) if clusters else 0.0,
                "generic_or_stale_articles": generic_or_stale,
                "duplicate_actions": dict(duplicate_actions),
                "topic_mix": topic_mix.most_common(25),
                "markdown_leaks": markdown_leaks,
            }
        )

    result["issues"] = dict(result["issues"])
    return result


def format_audit_report(payload: dict[str, Any]) -> str:
    lines = [
        f"NewsPrism quality audit: {payload['window']['start']}..{payload['window']['end']}",
        "",
        f"DB raw articles: {payload.get('db', {}).get('raw_article_count', 0)}",
        f"DB clusters: {payload.get('db', {}).get('cluster_count', 0)}",
        f"Rendered reports: {len(payload.get('rendered_reports', []))}",
        "",
        "Issues:",
    ]
    for key, count in sorted((payload.get("issues") or {}).items()):
        lines.append(f"- {key}: {count}")
    lines.append("")
    lines.append("Rendered report details:")
    for report in payload.get("rendered_reports", []):
        lines.append(
            f"- {report['date']}: clusters={report['cluster_count']}, "
            f"one_angle_multi={report['one_angle_multi_source']}, "
            f"generic_or_stale={len(report['generic_or_stale_articles'])}, "
            f"markdown_leaks={report['markdown_leaks']}"
        )
    return "\n".join(lines)
