"""
Single entrypoint for running one subquery search mode with shared snapshot/query/subquery config.

Examples:
    python 06-subquery_reports/run_subquery_search.py --search topic --snapshot 2026-06-26 --query q20260629 --subquery quantum_computing
    python 06-subquery_reports/run_subquery_search.py --search filters --snapshot 2026-06-26 --query q20260629 --subquery filters_ave_py_ge_2022_and_recency_py_ge_0_4
    python 06-subquery_reports/run_subquery_search.py --search passthrough --snapshot 2026-06-26 --query q20260629 --subquery everything
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from common_config import (
    DEFAULT_STAGING,
    DEFAULT_WORKGROUP,
    resolve_paths,
)


SCRIPTS = {
    "topic": "subquery_search_by_topic.py",
    "filters": "subquery_search_by_filters.py",
    "passthrough": "subquery_search_passthrough.py",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one subquery search mode with shared config.")
    parser.add_argument("--search", choices=["topic", "filters", "passthrough"], required=True)
    parser.add_argument("--snapshot", default=None, help="Snapshot token, e.g. 2026-06-26.")
    parser.add_argument("--query", default=None, help="Query token, e.g. q20260629.")
    parser.add_argument("--subquery", default=None, help="Subquery output folder name.")
    parser.add_argument("--query-folder", default=None, help="Deprecated alias for --subquery.")
    parser.add_argument("--staging", default=DEFAULT_STAGING, help="Athena query output S3 path.")
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP, help="Athena workgroup.")
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

def main() -> None:
    args = parse_args()

    paths = resolve_paths(
        snapshot=args.snapshot,
        query=args.query,
        subquery=args.subquery,
        query_folder=args.query_folder,
    )

    script = Path(__file__).resolve().parent / SCRIPTS[args.search]
    cmd = [
        sys.executable,
        str(script),
        "--snapshot",
        paths.snapshot,
        "--query",
        paths.query,
        "--subquery",
        paths.subquery,
        "--staging",
        args.staging,
        "--workgroup",
        args.workgroup,
    ]

    if args.min_size is not None:
        cmd.extend(["--min-size", str(args.min_size)])

    if args.search == "filters" and args.filters:
        for item in args.filters:
            cmd.extend(["--filter", item])

    print(f"[entrypoint] search={args.search}")
    print(f"[entrypoint] snapshot={paths.snapshot}")
    print(f"[entrypoint] query={paths.query}")
    print(f"[entrypoint] database={paths.database}")
    print(f"[entrypoint] subquery={paths.subquery}")
    print(f"[entrypoint] staging={args.staging}")
    print(f"[entrypoint] workgroup={args.workgroup}")
    print("[entrypoint] running:", " ".join(cmd))

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
