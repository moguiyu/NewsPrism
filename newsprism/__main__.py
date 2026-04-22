"""Entry point: python -m newsprism [collect|publish|run|once]"""
import argparse
import asyncio
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
    sub.add_parser("run", help="Start scheduler (long-running)")
    args = parser.parse_args()

    _setup_logging(verbose=args.verbose)

    cfg = load_config()
    sched = Scheduler(cfg)
    try:
        target_date = date.fromisoformat(args.report_date) if getattr(args, "report_date", None) else None
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
