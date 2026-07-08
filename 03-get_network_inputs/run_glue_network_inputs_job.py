#!/usr/bin/env python3
"""
Run utility for Step 03 network-input ETL Glue job.

Reads required SNAPSHOT/QUERY from YAML config and starts a Glue run.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import ClientError


DEFAULT_JOB_NAME = "openalex_get_network_inputs"
DEFAULT_POLL_SECONDS = 30

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}
FAILURE_STATES = {"FAILED", "STOPPED", "TIMEOUT", "ERROR"}
REQUIRED_CONFIG_KEYS = ("SNAPSHOT", "QUERY")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start and monitor Step 03 Glue job run.")
    parser.add_argument("--config", required=True, type=Path, help="Path to step config YAML.")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--wait", action="store_true", help="Wait until terminal state.")
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help="Polling interval while waiting for status updates.",
    )
    return parser.parse_args()


def load_config(config_path: Path) -> dict[str, str]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError("Config must be a YAML object.")

    out: dict[str, str] = {}
    missing: list[str] = []
    for key in REQUIRED_CONFIG_KEYS:
        value = str(raw.get(key, "")).strip()
        if not value:
            missing.append(key)
        out[key] = value

    if missing:
        missing_keys = ", ".join(missing)
        raise RuntimeError(f"Missing required config keys with non-empty values: {missing_keys}")

    return out


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
    config = load_config(args.config)

    snapshot = config["SNAPSHOT"]
    query = config["QUERY"]

    print(f"[config] SNAPSHOT={snapshot}")
    print(f"[config] QUERY={query}")

    glue = boto3.client("glue")
    start_kwargs = {
        "JobName": args.job_name,
        "Arguments": {
            "--SNAPSHOT": snapshot,
            "--QUERY": query,
        },
    }

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
