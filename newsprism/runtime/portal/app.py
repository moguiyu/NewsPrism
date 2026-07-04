"""FastAPI admin quality portal — local-only, reads/writes the live SQLite.

Layer: runtime (imports repo + service + portal.analytics).
"""
from __future__ import annotations

from datetime import date, timedelta
import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from newsprism.repo.db import (
    DB_PATH, get_calibration_state, get_latest_editorial_policy,
    insert_editorial_feedback, insert_feedback_correction, list_corrections,
    query_evaluations, selected_source_regions,
)
from newsprism.runtime.portal import analytics as A

_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


class VerdictIn(BaseModel):
    cluster_id: int
    verdict: int
    note: str = ""


class CorrectionIn(BaseModel):
    evaluation_id: int
    kind: str                       # dimension | category | promote | demote
    dimension: str | None = None
    suggested_value: float | None = None
    payload: str = ""


def _parse_list(value: str | None) -> list[str]:
    return [v for v in (value or "").split(",") if v] if value else []


def _parse_float(value: str | None) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None


# --- Cloudflare Access defensive gate --------------------------------------
# Threat model: prevent portal exposure on MISCONFIGURATION (Access policy
# disabled, or LAN/loopback access that bypasses Cloudflare), NOT to resist
# header forgery. v1 validates the header is present and JWT-shaped only;
# signature verification is deferred. See design spec
# docs/superpowers/specs/2026-06-17-portal-cloudflare-access-design.md.

# Header name Cloudflare Access injects on tunneled requests.
_CF_ACCESS_HEADER = "cf-access-jwt-assertion"


def _is_cf_access_allowed(headers, require: bool) -> bool:
    """Decide whether a request should pass the Cloudflare Access gate.

    ``headers`` is a mapping (Starlette headers are case-insensitive; this
    function is written to match the lowercase header name Starlette exposes).
    When ``require`` is False, always True (local dev / SSH tunnel mode).
    Otherwise True only if a JWT-shaped (three dot-separated segments) header
    is present.
    """
    if not require:
        return True
    token = headers.get(_CF_ACCESS_HEADER, "")
    if not token:
        return False
    return token.count(".") == 2


def _cf_access_required() -> bool:
    """Read PORTAL_REQUIRE_CF_ACCESS. Secure-by-default: True unless the env
    var is exactly 'false' (case-insensitive). Anything else (unset, 'true',
    garbage) means required. This makes a fresh production container safe even
    if .env omits the var."""
    return os.environ.get("PORTAL_REQUIRE_CF_ACCESS", "true").strip().lower() != "false"


def create_app(db_path: Path = DB_PATH) -> FastAPI:
    app = FastAPI(title="NewsPrism Quality Portal")
    app.state.db_path = db_path
    _TEMPLATES.env.globals["heat_class"] = A.heat_class
    _TEMPLATES.env.globals["gate_badge"] = A.gate_badge
    _TEMPLATES.env.globals["DIMENSIONS"] = A.DIMENSIONS

    require_cf = _cf_access_required()

    @app.middleware("http")
    async def cf_access_gate(request: Request, call_next):
        # /healthz is always reachable (cloudflared liveness probe).
        if request.url.path == "/healthz":
            return await call_next(request)
        if not _is_cf_access_allowed(request.headers, require_cf):
            return PlainTextResponse(
                "Cloudflare Access authentication required.",
                status_code=401,
            )
        return await call_next(request)

    @app.get("/healthz")
    def healthz():
        return PlainTextResponse("ok")

    def _window(req: Request) -> tuple[str, str]:
        q = req.query_params
        today = date.today().isoformat()
        return (q.get("date_from") or today, q.get("date_to") or today)

    def _filtered(req: Request, rows: list[dict]) -> list[dict]:
        q = req.query_params
        return A.filter_rows(
            rows,
            categories=_parse_list(q.get("categories")),
            statuses=_parse_list(q.get("statuses")),
            selection=q.get("selection", "all"),
            composite_min=_parse_float(q.get("composite_min")),
            composite_max=_parse_float(q.get("composite_max")),
            subject_regions=_parse_list(q.get("subject_regions")),
            has_feedback={"1": True, "0": False}.get(q.get("has_feedback")),
        )

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        end = date.today()
        start = end - timedelta(days=7)
        rows = query_evaluations(start.isoformat(), end.isoformat(), db_path=db_path)
        return _TEMPLATES.TemplateResponse(
            request,
            "index.html",
            {"rows": rows, "trend": A.trends(rows),
             "start": start.isoformat(), "end": end.isoformat()},
        )

    @app.get("/day", response_class=HTMLResponse)
    def day(request: Request):
        d = request.query_params.get("date") or date.today().isoformat()
        rows = _filtered(request, query_evaluations(d, d, db_path=db_path))
        return _TEMPLATES.TemplateResponse(
            request, "day.html", {"date": d, "rows": rows},
        )

    @app.get("/matrices", response_class=HTMLResponse)
    def matrices(request: Request):
        df, dt = _window(request)
        rows = _filtered(request, query_evaluations(df, dt, db_path=db_path))
        src = selected_source_regions(df, dt, db_path=db_path)
        return _TEMPLATES.TemplateResponse(
            request,
            "matrices.html",
            {"date_from": df, "date_to": dt,
             "cat_dim": A.matrix_category_dimension(rows),
             "subj_cat": A.matrix_subject_category(rows),
             "src_subj": A.matrix_source_subject(rows, src)},
        )

    @app.get("/trends", response_class=HTMLResponse)
    def trends_page(request: Request):
        df, dt = _window(request)
        rows = query_evaluations(df, dt, db_path=db_path)
        series = A.trends(rows)
        spark = A.sparkline_svg([t["composite_avg"] for t in series])
        return _TEMPLATES.TemplateResponse(
            request, "trends.html",
            {"date_from": df, "date_to": dt, "series": series, "spark": spark},
        )

    @app.get("/calibration", response_class=HTMLResponse)
    def calibration_page(request: Request):
        return _TEMPLATES.TemplateResponse(
            request, "calibration.html",
            {"weights": get_calibration_state(db_path=db_path),
             "policy": get_latest_editorial_policy(db_path=db_path),
             "corrections": list_corrections(days=30, db_path=db_path)},
        )

    @app.get("/sources", response_class=HTMLResponse)
    def sources_page(request: Request):
        df, dt = _window(request)
        rows = query_evaluations(df, dt, db_path=db_path)
        src = selected_source_regions(df, dt, db_path=db_path)
        return _TEMPLATES.TemplateResponse(
            request, "sources.html",
            {"date_from": df, "date_to": dt, "review": A.source_review(rows, src)},
        )

    @app.post("/api/verdict")
    def api_verdict(body: VerdictIn):
        insert_editorial_feedback(body.cluster_id, body.verdict, channel="portal",
                                  note=body.note, db_path=db_path)
        return {"ok": True}

    @app.post("/api/correction")
    def api_correction(body: CorrectionIn):
        insert_feedback_correction(body.evaluation_id, body.kind, dimension=body.dimension,
                                   suggested_value=body.suggested_value, payload=body.payload,
                                   channel="portal", db_path=db_path)
        return {"ok": True}

    return app
