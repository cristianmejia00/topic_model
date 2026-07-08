#!/usr/bin/env python3
"""
Run utility for JSONL -> Parquet Glue ETL.

Starts a Glue job run with optional argument overrides and can wait for terminal state.
"""

from __future__ import annotations

import argparse
import time

import boto3
from botocore.exceptions import ClientError


DEFAULT_JOB_NAME = "openalex_jsonl_to_parquet"
DEFAULT_POLL_SECONDS = 30

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}
FAILURE_STATES = {"FAILED", "STOPPED", "TIMEOUT", "ERROR"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start and monitor a Glue job run.")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--input-path", default=None)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--openalex-prefix", default=None)
    parser.add_argument("--substring-len", type=int, default=None)
    parser.add_argument("--write-mode", default=None)
    parser.add_argument("--wait", action="store_true", help="Wait until terminal state.")
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help="Polling interval while waiting for status updates.",
    )
    return parser.parse_args()


def build_runtime_arguments(args: argparse.Namespace) -> dict[str, str]:
    runtime_arguments: dict[str, str] = {}

    if args.input_path:
        runtime_arguments["--INPUT_PATH"] = args.input_path
    if args.output_path:
        runtime_arguments["--OUTPUT_PATH"] = args.output_path
    if args.openalex_prefix:
        runtime_arguments["--OPENALEX_PREFIX"] = args.openalex_prefix
    if args.substring_len is not None:
        runtime_arguments["--SUBSTRING_LEN"] = str(args.substring_len)
    if args.write_mode:
        runtime_arguments["--WRITE_MODE"] = args.write_mode

    return runtime_arguments


def wait_for_completion(glue, job_name: str, run_id: str, poll_seconds: int) -> int:
    last_state = None

    while True:
        run = glue.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
        state = run.get("JobRunState", "UNKNOWN")

        if state != last_state:
            print(f"[status] {job_name} run {run_id}: {state}")
            last_state = state

        if state in TERMINAL_STATES:
            if state in FAILURE_STATES:
                message = run.get("ErrorMessage", "No ErrorMessage returned by Glue.")
                print(f"[failure] {message}")
                return 1

            execution_time = run.get("ExecutionTime")
            dpu_seconds = run.get("DPUSeconds")
            print(f"[success] ExecutionTime={execution_time}s DPUSeconds={dpu_seconds}")
            return 0

        time.sleep(poll_seconds)


def main() -> int:
    args = parse_args()
    glue = boto3.client("glue")

    runtime_arguments = build_runtime_arguments(args)
    start_kwargs = {"JobName": args.job_name}
    if runtime_arguments:
        start_kwargs["Arguments"] = runtime_arguments

    response = glue.start_job_run(**start_kwargs)
    run_id = response["JobRunId"]

    print(f"Started Glue job: {args.job_name}")
    print(f"Run ID: {run_id}")

    if not args.wait:
        print("Run launched asynchronously. Re-run with --wait to monitor completion.")
        return 0

    return wait_for_completion(glue, args.job_name, run_id, args.poll_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ClientError as exc:
        raise RuntimeError(f"AWS API error: {exc}") from exc
