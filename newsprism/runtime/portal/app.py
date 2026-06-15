"""FastAPI admin quality portal — local-only, reads/writes the live SQLite.

Layer: runtime (imports repo + service + portal.analytics).
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from newsprism.repo.db import (
    DB_PATH, query_evaluations, selected_source_regions,
)
from newsprism.runtime.portal import analytics as A

_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _parse_list(value: str | None) -> list[str]:
    return [v for v in (value or "").split(",") if v] if value else []


def create_app(db_path: Path = DB_PATH) -> FastAPI:
    app = FastAPI(title="NewsPrism Quality Portal")
    app.state.db_path = db_path
    _TEMPLATES.env.globals["heat_class"] = A.heat_class
    _TEMPLATES.env.globals["DIMENSIONS"] = A.DIMENSIONS

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
            composite_min=float(q["composite_min"]) if q.get("composite_min") else None,
            composite_max=float(q["composite_max"]) if q.get("composite_max") else None,
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

    return app
