"""Entry point: python -m newsprism [collect|publish|run|once]"""
import argparse
import asyncio
import json
import logging
import sys
from datetime import date


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, stream=sys.stdout)
    # Silence noisy third-party loggers
    for name in ("httpx", "httpcore", "urllib3", "feedparser",
                 "sentence_transformers", "transformers", "huggingface_hub"):
        logging.getLogger(name).setLevel(logging.WARNING)


from newsprism.config import load_config
from newsprism.runtime.scheduler import Scheduler


def _run_async_command(label: str, coro) -> None:
    logger = logging.getLogger(__name__)
    try:
        asyncio.run(coro)
    except Exception:
        logger.exception("%s failed", label)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(prog="newsprism")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("collect", help="Collect and store articles now")
    sub.add_parser("publish", help="Build and publish today's report immediately")
    sub.add_parser("push", help="Push today's staged report now")
    sub.add_parser("once", help="Run full pipeline once (collect + publish)")
    replay = sub.add_parser("replay", help="Replay one report date from the exact article set used in that report")
    replay.add_argument("--date", dest="report_date", help="Target report date in YYYY-MM-DD format (default: today)")
    replay.add_argument("--dry-run", action="store_true", help="Show what would be reset without changing the DB")
    audit_parser = sub.add_parser("audit", help="Audit source, selection, and rendered report quality")
    audit_parser.add_argument("--days", type=int, default=10, help="Number of days to audit (default: 10)")
    audit_parser.add_argument("--date", dest="audit_date", help="Anchor date in YYYY-MM-DD format (default: today)")
    audit_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    audit_parser.add_argument("--db-path", default="data/newsprism.db", help="SQLite DB path")
    audit_parser.add_argument("--output-dir", default="output", help="Rendered output directory")
    sub.add_parser("run", help="Start scheduler (long-running)")

    feedback_parser = sub.add_parser("feedback", help="Record or list editor accept/reject feedback")
    feedback_sub = feedback_parser.add_subparsers(dest="feedback_cmd")
    fb_add = feedback_sub.add_parser("add", help="Record one accept/reject signal")
    fb_add.add_argument("--cluster", dest="fb_cluster", type=int, required=True, help="Cluster id from the report")
    fb_add.add_argument("--verdict", choices=["accept", "reject"], required=True)
    fb_add.add_argument("--note", default="", help="Optional note")
    fb_list = feedback_sub.add_parser("list", help="Show recent feedback")
    fb_list.add_argument("--limit", type=int, default=30)

    calibrate_parser = sub.add_parser("calibrate", help="Tune impact weights and refresh editorial policy memory")
    calibrate_sub = calibrate_parser.add_subparsers(dest="calibrate_cmd")
    calibrate_sub.add_parser("run", help="Run weekly calibration now")
    calibrate_sub.add_parser("show", help="Show current weights and editorial policy")
    calibrate_sub.add_parser("reset", help="Restore all weights to seed values")

    search_review_parser = sub.add_parser(
        "search-review", help="Review Active Search publisher identity candidates"
    )
    search_review_sub = search_review_parser.add_subparsers(dest="search_review_cmd")
    sr_list = search_review_sub.add_parser("list", help="List pending candidates")
    sr_list.add_argument("--limit", type=int, default=50)
    sr_list.add_argument("--db-path", default="data/newsprism.db")
    sr_approve = search_review_sub.add_parser("approve", help="Approve one domain binding")
    sr_approve.add_argument("--id", type=int, required=True)
    sr_approve.add_argument("--db-path", default="data/newsprism.db")
    sr_approve.add_argument(
        "--bindings-file", default="data/search-source-bindings.yaml"
    )
    sr_reject = search_review_sub.add_parser("reject", help="Reject one candidate")
    sr_reject.add_argument("--id", type=int, required=True)
    sr_reject.add_argument("--reason", default="rejected_by_editor")
    sr_reject.add_argument("--db-path", default="data/newsprism.db")

    portal_parser = sub.add_parser("portal", help="Run the local admin quality portal")
    portal_parser.add_argument("--host", default="127.0.0.1")
    portal_parser.add_argument("--port", type=int, default=8081)
    portal_parser.add_argument("--db-path", default="data/newsprism.db")

    args = parser.parse_args()

    _setup_logging(verbose=args.verbose)

    cfg = load_config()
    sched = Scheduler(cfg)
    try:
        target_date = date.fromisoformat(args.report_date) if getattr(args, "report_date", None) else None
    except ValueError:
        parser.error("--date must be in YYYY-MM-DD format")
    try:
        if getattr(args, "audit_date", None):
            date.fromisoformat(args.audit_date)
    except ValueError:
        parser.error("--date must be in YYYY-MM-DD format")

    try:
        if args.cmd == "collect":
            _run_async_command("collect", sched.collect())
        elif args.cmd == "publish":
            _run_async_command("publish", sched.publish(push_after_render=True))
        elif args.cmd == "push":
            _run_async_command("push", sched.push())
        elif args.cmd == "once":
            _run_async_command("once", sched.run_once())
        elif args.cmd == "replay":
            _run_async_command("replay", sched.replay(report_date=target_date, dry_run=args.dry_run))
        elif args.cmd == "audit":
            from newsprism.runtime.audit import audit, format_audit_report

            payload = audit(
                days=args.days,
                anchor_date=args.audit_date,
                db_path=args.db_path,
                output_dir=args.output_dir,
            )
            if args.json:
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                print(format_audit_report(payload))
        elif args.cmd == "feedback":
            from newsprism.runtime.feedback import (
                format_feedback_list,
                record_feedback_cli,
            )

            if args.feedback_cmd == "add":
                row_id = record_feedback_cli(args.fb_cluster, args.verdict, note=args.note)
                print(f"Recorded feedback #{row_id}: cluster={args.fb_cluster} verdict={args.verdict}")
            elif args.feedback_cmd == "list":
                print(format_feedback_list(limit=args.limit))
            else:
                feedback_parser.print_help()
                sys.exit(1)
        elif args.cmd == "calibrate":
            from newsprism.service.calibrate import (
                reset_calibration,
                run_calibration,
                show_calibration,
            )

            if args.calibrate_cmd == "run":
                result = run_calibration(cfg)
                print(json.dumps(result, ensure_ascii=False, indent=2))
            elif args.calibrate_cmd == "show":
                print(show_calibration())
            elif args.calibrate_cmd == "reset":
                print(reset_calibration())
            else:
                calibrate_parser.print_help()
                sys.exit(1)
        elif args.cmd == "search-review":
            from pathlib import Path
            from newsprism.runtime.search_review import (
                approve_review_binding,
                format_pending_reviews,
                reject_review,
            )

            if args.search_review_cmd == "list":
                print(format_pending_reviews(args.limit, Path(args.db_path)))
            elif args.search_review_cmd == "approve":
                result = approve_review_binding(
                    args.id,
                    db_path=Path(args.db_path),
                    bindings_path=Path(args.bindings_file),
                )
                print(json.dumps(result, ensure_ascii=False, indent=2))
            elif args.search_review_cmd == "reject":
                reject_review(args.id, args.reason, db_path=Path(args.db_path))
                print(f"Rejected Active Search candidate #{args.id}")
            else:
                search_review_parser.print_help()
                sys.exit(1)
        elif args.cmd == "portal":
            import uvicorn
            from pathlib import Path
            from newsprism.repo.db import init_db
            from newsprism.runtime.portal.app import create_app

            db_path = Path(args.db_path)
            init_db(db_path)
            uvicorn.run(create_app(db_path=db_path), host=args.host, port=args.port)
        elif args.cmd == "run":
            sched.start()
        else:
            parser.print_help()
            sys.exit(1)
    except Exception:
        logging.getLogger(__name__).exception("newsprism command %s exited with an error", args.cmd or "unknown")
        sys.exit(1)


if __name__ == "__main__":
    main()
