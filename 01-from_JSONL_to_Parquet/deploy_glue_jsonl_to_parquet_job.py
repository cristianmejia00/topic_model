#!/usr/bin/env python3
"""
Deploy utility for JSONL -> Parquet Glue ETL.

What it does:
1) Uploads the local ETL script to the configured S3 scripts prefix.
2) Creates a Glue job if missing, otherwise updates the existing job.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


DEFAULT_SCRIPT_S3_PREFIX = "s3://aws-glue-assets-702228044494-ap-northeast-1/scripts/01-from-jsonl-to-parquet/"
DEFAULT_JOB_NAME = "openalex_jsonl_to_parquet"
DEFAULT_INPUT_PATH = "s3://openalex-works/snapshot/data/works"
DEFAULT_OUTPUT_PATH = "s3://openalex-results/snapshot_{SNAPSHOT_DATE}"
DEFAULT_OPENALEX_PREFIX = "https://openalex.org/"
DEFAULT_SUBSTRING_LEN = 500
DEFAULT_WRITE_MODE = "overwrite"
DEFAULT_GLUE_VERSION = "5.0"
DEFAULT_WORKER_TYPE = "G.4X"
DEFAULT_NUMBER_OF_WORKERS = 30
DEFAULT_TIMEOUT_MINUTES = 240
DEFAULT_MAX_CONCURRENT_RUNS = 1
DEFAULT_JOB_BOOKMARK_OPTION = "job-bookmark-disable"

ROLE_ENV_VAR = "GLUE_JOB_ROLE"


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected an S3 URI, got: {uri}")
    remainder = uri[5:]
    bucket, sep, key = remainder.partition("/")
    if not bucket or not sep:
        raise ValueError(f"Expected S3 URI with bucket and key, got: {uri}")
    return bucket, key


def normalize_prefix(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"


def parse_args() -> argparse.Namespace:
    repo_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="Deploy JSONL to Parquet Glue job and script.")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME, help="Glue job name.")
    parser.add_argument(
        "--script-local-path",
        type=Path,
        default=repo_dir / "glue_jsonl_to_parquet_etl.py",
        help="Local path to the Glue ETL Python script.",
    )
    parser.add_argument(
        "--script-s3-prefix",
        default=DEFAULT_SCRIPT_S3_PREFIX,
        help="S3 prefix where the Glue script is uploaded.",
    )
    parser.add_argument(
        "--role",
        default=os.getenv(ROLE_ENV_VAR, "").strip() or None,
        help=f"Glue job IAM role name/ARN. Defaults to ${ROLE_ENV_VAR} if set.",
    )

    parser.add_argument("--description", default="OpenAlex works JSONL to parquet conversion job.")
    parser.add_argument("--glue-version", default=DEFAULT_GLUE_VERSION)
    parser.add_argument("--worker-type", default=DEFAULT_WORKER_TYPE)
    parser.add_argument("--number-of-workers", type=int, default=DEFAULT_NUMBER_OF_WORKERS)
    parser.add_argument("--timeout-minutes", type=int, default=DEFAULT_TIMEOUT_MINUTES)
    parser.add_argument("--max-retries", type=int, default=0)
    parser.add_argument("--max-concurrent-runs", type=int, default=DEFAULT_MAX_CONCURRENT_RUNS)
    parser.add_argument("--job-bookmark-option", default=DEFAULT_JOB_BOOKMARK_OPTION)
    parser.add_argument("--temp-dir", default=None, help="Optional s3:// temp dir for Glue.")

    parser.add_argument("--input-path", default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--openalex-prefix", default=DEFAULT_OPENALEX_PREFIX)
    parser.add_argument("--substring-len", type=int, default=DEFAULT_SUBSTRING_LEN)
    parser.add_argument("--write-mode", default=DEFAULT_WRITE_MODE)

    return parser.parse_args()


def build_default_arguments(args: argparse.Namespace) -> dict[str, str]:
    default_arguments = {
        "--job-bookmark-option": args.job_bookmark_option,
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-job-insights": "true",
        "--INPUT_PATH": args.input_path,
        "--OUTPUT_PATH": args.output_path,
        "--OPENALEX_PREFIX": args.openalex_prefix,
        "--SUBSTRING_LEN": str(args.substring_len),
        "--WRITE_MODE": args.write_mode,
    }
    if args.temp_dir:
        default_arguments["--TempDir"] = args.temp_dir
    return default_arguments


def build_job_update(args: argparse.Namespace, role: str, script_s3_uri: str) -> dict:
    return {
        "Description": args.description,
        "Role": role,
        "ExecutionProperty": {"MaxConcurrentRuns": args.max_concurrent_runs},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_uri,
            "PythonVersion": "3",
        },
        "DefaultArguments": build_default_arguments(args),
        "GlueVersion": args.glue_version,
        "WorkerType": args.worker_type,
        "NumberOfWorkers": args.number_of_workers,
        "Timeout": args.timeout_minutes,
        "MaxRetries": args.max_retries,
    }


def main() -> None:
    args = parse_args()
    if not args.script_local_path.exists():
        raise FileNotFoundError(f"Script not found: {args.script_local_path}")

    script_s3_prefix = normalize_prefix(args.script_s3_prefix)
    bucket, prefix_key = parse_s3_uri(script_s3_prefix)
    script_key = f"{prefix_key}{args.script_local_path.name}"
    script_s3_uri = f"s3://{bucket}/{script_key}"

    s3 = boto3.client("s3")
    glue = boto3.client("glue")

    print(f"Uploading script to {script_s3_uri} ...")
    s3.upload_file(str(args.script_local_path), bucket, script_key)
    print("[ok] script uploaded")

    existing_job = None
    try:
        existing_job = glue.get_job(JobName=args.job_name)["Job"]
        print(f"[ok] existing job found: {args.job_name}")
    except glue.exceptions.EntityNotFoundException:
        print(f"[create] job does not exist yet: {args.job_name}")

    resolved_role = args.role or (existing_job or {}).get("Role")
    if not resolved_role:
        raise RuntimeError(
            "Missing Glue role. Provide --role or set GLUE_JOB_ROLE (for new job creation)."
        )

    job_update = build_job_update(args, resolved_role, script_s3_uri)

    if existing_job is None:
        create_payload = {"Name": args.job_name, **job_update}
        glue.create_job(**create_payload)
        print(f"[create] glue job created: {args.job_name}")
    else:
        glue.update_job(JobName=args.job_name, JobUpdate=job_update)
        print(f"[update] glue job updated: {args.job_name}")

    print("Deployment complete.")
    print(f"Job name: {args.job_name}")
    print(f"Script: {script_s3_uri}")


if __name__ == "__main__":
    try:
        main()
    except ClientError as exc:
        raise RuntimeError(f"AWS API error: {exc}") from exc
