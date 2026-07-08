"""
Single entrypoint for running one subquery search mode with shared database/folder config.

Examples:
  python subqueries/run_subquery_search.py --search topic
  python subqueries/run_subquery_search.py --search filters
  python subqueries/run_subquery_search.py --search passthrough --database q20260629 --query-folder everything
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from common_config import (
    DEFAULT_QUERY_FOLDER_FILTERS,
    DEFAULT_QUERY_FOLDER_PASSTHROUGH,
    DEFAULT_QUERY_FOLDER_TOPIC,
    resolve_database,
    resolve_query_folder,
)


SCRIPTS = {
    "topic": "subquery_search_by_topic.py",
    "filters": "subquery_search_by_filters.py",
    "passthrough": "subquery_search_passthrough.py",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one subquery search mode with shared config.")
    parser.add_argument("--search", choices=["topic", "filters", "passthrough"], required=True)
    parser.add_argument("--database", default=None, help="Athena/classification database, e.g. q20260629")
    parser.add_argument("--query-folder", default=None, help="Subquery output folder name")
    parser.add_argument(
        "--filter",
        action="append",
        dest="filters",
        help="Only for search=filters. Repeat for multiple filter clauses.",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=None,
        help="Optional min publications threshold override for the selected search script.",
    )
    return parser.parse_args()


def default_folder_for(search: str) -> str:
    if search == "filters":
        return DEFAULT_QUERY_FOLDER_FILTERS
    if search == "passthrough":
        return DEFAULT_QUERY_FOLDER_PASSTHROUGH
    return DEFAULT_QUERY_FOLDER_TOPIC


def main() -> None:
    args = parse_args()

    database = resolve_database(args.database)
    query_folder = resolve_query_folder(args.query_folder, default_folder_for(args.search))

    script = Path(__file__).resolve().parent / SCRIPTS[args.search]
    cmd = [sys.executable, str(script), "--database", database, "--query-folder", query_folder]

    if args.min_size is not None:
        cmd.extend(["--min-size", str(args.min_size)])

    if args.search == "filters" and args.filters:
        for item in args.filters:
            cmd.extend(["--filter", item])

    print(f"[entrypoint] search={args.search}")
    print(f"[entrypoint] database={database}")
    print(f"[entrypoint] query_folder={query_folder}")
    print("[entrypoint] running:", " ".join(cmd))

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
