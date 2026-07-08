#!/usr/bin/env python3
"""
Deploy utility for Step 03 network-input ETL Glue job.

What it does:
1) Uploads local ETL script to the configured S3 scripts prefix.
2) Creates a Glue job if missing, otherwise updates it.
3) Applies job settings equivalent to the old notebook configuration.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


DEFAULT_SCRIPT_S3_PREFIX = "s3://aws-glue-assets-702228044494-ap-northeast-1/scripts/03-get-network-inputs/"
DEFAULT_JOB_NAME = "openalex_get_network_inputs"
DEFAULT_GLUE_VERSION = "5.0"
DEFAULT_WORKER_TYPE = "G.2X"
DEFAULT_NUMBER_OF_WORKERS = 10
DEFAULT_TIMEOUT_MINUTES = 240
DEFAULT_MAX_CONCURRENT_RUNS = 1
DEFAULT_MAX_RETRIES = 0
DEFAULT_JOB_BOOKMARK_OPTION = "job-bookmark-disable"
DEFAULT_SOURCE_JOB_FOR_ROLE = "edges_to_cwts_format_v3"
DEFAULT_SHUFFLE_STORAGE_PATH = "s3://openalex-outputs/cwts/spark/"
DEFAULT_INPUT_PATH_TEMPLATE = "s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/edges_query/"
DEFAULT_OUTPUT_PATH_TEMPLATE = "s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/"


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

    parser = argparse.ArgumentParser(description="Deploy Step 03 Glue job and script.")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME, help="Glue job name.")
    parser.add_argument(
        "--script-local-path",
        type=Path,
        default=repo_dir / "get_network_inputs_etl.py",
        help="Local path to ETL script.",
    )
    parser.add_argument(
        "--script-s3-prefix",
        default=DEFAULT_SCRIPT_S3_PREFIX,
        help="S3 prefix where ETL script is uploaded.",
    )
    parser.add_argument(
        "--role",
        default=None,
        help="Glue IAM role ARN/name to use for the job.",
    )
    parser.add_argument(
        "--source-job-for-role",
        default=DEFAULT_SOURCE_JOB_FOR_ROLE,
        help=(
            "Existing Glue job name to copy role from when --role is omitted. "
            "Set to empty string to disable lookup."
        ),
    )

    parser.add_argument("--description", default="Build network input files for Louvain from edges_query parquet.")
    parser.add_argument("--glue-version", default=DEFAULT_GLUE_VERSION)
    parser.add_argument("--worker-type", default=DEFAULT_WORKER_TYPE)
    parser.add_argument("--number-of-workers", type=int, default=DEFAULT_NUMBER_OF_WORKERS)
    parser.add_argument("--timeout-minutes", type=int, default=DEFAULT_TIMEOUT_MINUTES)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--max-concurrent-runs", type=int, default=DEFAULT_MAX_CONCURRENT_RUNS)
    parser.add_argument("--job-bookmark-option", default=DEFAULT_JOB_BOOKMARK_OPTION)
    parser.add_argument("--shuffle-storage-path", default=DEFAULT_SHUFFLE_STORAGE_PATH)

    parser.add_argument("--input-path-template", default=DEFAULT_INPUT_PATH_TEMPLATE)
    parser.add_argument("--output-path-template", default=DEFAULT_OUTPUT_PATH_TEMPLATE)

    return parser.parse_args()


def resolve_role(glue, explicit_role: str | None, source_job_for_role: str | None) -> str:
    role = str(explicit_role or "").strip()
    if role:
        return role

    source_job = str(source_job_for_role or "").strip()
    if source_job:
        try:
            source_job_def = glue.get_job(JobName=source_job)["Job"]
            source_role = str(source_job_def.get("Role", "")).strip()
            if source_role:
                print(f"[resolve] role copied from job {source_job}: {source_role}")
                return source_role
        except glue.exceptions.EntityNotFoundException:
            print(f"[warn] source job not found for role lookup: {source_job}")

    raise RuntimeError(
        "Unable to resolve Glue role. Provide --role, or set --source-job-for-role to an existing job."
    )


def build_default_arguments(args: argparse.Namespace) -> dict[str, str]:
    return {
        "--job-bookmark-option": args.job_bookmark_option,
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-job-insights": "true",
        "--enable-auto-scaling": "true",
        "--write-shuffle-files-to-s3": "true",
        "--conf": f"spark.shuffle.storage.path={args.shuffle_storage_path}",
        "--INPUT_PATH_TEMPLATE": args.input_path_template,
        "--OUTPUT_PATH_TEMPLATE": args.output_path_template,
    }


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

    role = resolve_role(glue, args.role, args.source_job_for_role)

    print(f"Uploading script to {script_s3_uri} ...")
    s3.upload_file(str(args.script_local_path), bucket, script_key)
    print("[ok] script uploaded")

    existing_job = None
    try:
        existing_job = glue.get_job(JobName=args.job_name)["Job"]
        print(f"[ok] existing job found: {args.job_name}")
    except glue.exceptions.EntityNotFoundException:
        print(f"[create] job does not exist yet: {args.job_name}")

    job_update = build_job_update(args, role, script_s3_uri)

    if existing_job is None:
        create_payload = {"Name": args.job_name, **job_update}
        glue.create_job(**create_payload)
        print(f"[create] glue job created: {args.job_name}")
    else:
        glue.update_job(JobName=args.job_name, JobUpdate=job_update)
        print(f"[update] glue job updated: {args.job_name}")

    print("Deployment complete.")
    print(f"Job name: {args.job_name}")
    print(f"Role: {role}")
    print(f"Script: {script_s3_uri}")


if __name__ == "__main__":
    try:
        main()
    except ClientError as exc:
        raise RuntimeError(f"AWS API error: {exc}") from exc
