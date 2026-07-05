"""
Create/refresh Glue Catalog database and discover tables with a crawler.

This script:
1) Creates a Glue database named exactly as --database (if missing).
2) Creates or updates one Glue crawler targeting both required S3 roots:
   - s3://openalex-outputs/athena/{database}/
   - s3://openalex-outputs/cwts/{database}/network_assets/{version}/
3) Runs the crawler and waits for completion (unless --no-wait).
4) Optional hard reset mode can delete crawler + all tables + database first.

Usage:
    .venv/bin/python create_glue_catalog.py \
        --database q20260629 \
        --version version3 \
        --crawler-role AWSGlueServiceRole-openalex

    # Destructive reset (recreate database and recrawl from scratch)
    .venv/bin/python create_glue_catalog.py \
        --database q20260629 \
        --version version3 \
        --crawler-role AWSGlueServiceRole-openalex \
        --hard-reset

Notes:
- The IAM role must allow Glue crawler execution and read access to both S3 roots.
- Crawler table discovery depends on supported file formats under those prefixes.
"""

from __future__ import annotations

import argparse
import os
import re
import time

import boto3
from botocore.exceptions import ClientError


DEFAULT_CRAWLER_ROLE_ENV = "GLUE_CRAWLER_ROLE"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Glue database and crawl source paths for table discovery."
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Glue database name to create/use, e.g. q20260629.",
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Network-assets version folder, e.g. version3.",
    )
    parser.add_argument(
        "--crawler-role",
        default=os.getenv(DEFAULT_CRAWLER_ROLE_ENV, "").strip() or None,
        help=(
            "Glue crawler IAM role name/ARN. "
            f"Defaults to ${DEFAULT_CRAWLER_ROLE_ENV} if set."
        ),
    )
    parser.add_argument(
        "--crawler-name",
        default=None,
        help="Optional crawler name override.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=10,
        help="Polling interval while waiting for crawler completion.",
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=3600,
        help="Maximum wait time for crawler completion.",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Start crawler and return immediately without waiting.",
    )
    parser.add_argument(
        "--hard-reset",
        action="store_true",
        help=(
            "Delete the selected crawler (if it exists), then delete all Glue "
            "tables in the database and drop the database before recreating. "
            "Use with care."
        ),
    )
    return parser.parse_args()


def sanitize_name(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", text).strip("_")


def resolve_crawler_role_arn(iam, crawler_role: str) -> str:
    # Glue CreateCrawler is more reliable with a full role ARN, especially for
    # roles under the /service-role/ path.
    role = (crawler_role or "").strip()
    if role.startswith("arn:aws:iam::") and ":role/" in role:
        return role

    try:
        resp = iam.get_role(RoleName=role)
        arn = resp["Role"]["Arn"]
        print(f"[resolve] crawler role name -> arn: {arn}")
        return arn
    except ClientError as exc:
        print(
            "[warn] could not resolve role name to ARN via iam:GetRole; "
            "using provided value as-is"
        )
        print(f"[warn] iam:GetRole error: {exc}")
        return role


def ensure_database(glue, database: str) -> None:
    try:
        glue.get_database(Name=database)
        print(f"[ok] database exists: {database}")
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_database(DatabaseInput={"Name": database})
    print(f"[create] database created: {database}")


def delete_crawler_if_exists(glue, crawler_name: str) -> None:
    try:
        glue.get_crawler(Name=crawler_name)
    except glue.exceptions.EntityNotFoundException:
        print(f"[reset] crawler not found (skip): {crawler_name}")
        return

    glue.delete_crawler(Name=crawler_name)
    print(f"[reset] crawler deleted: {crawler_name}")


def list_database_tables(glue, database: str) -> list[str]:
    names: list[str] = []
    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for table in page.get("TableList", []):
            name = table.get("Name")
            if name:
                names.append(name)
    return names


def reset_database(glue, database: str) -> None:
    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        print(f"[reset] database not found (skip): {database}")
        return

    table_names = list_database_tables(glue, database)
    for name in table_names:
        glue.delete_table(DatabaseName=database, Name=name)
        print(f"[reset] table deleted: {database}.{name}")

    glue.delete_database(Name=database)
    print(f"[reset] database deleted: {database}")


def ensure_crawler(
    glue,
    *,
    crawler_name: str,
    database: str,
    crawler_role: str,
    s3_paths: list[str],
) -> None:
    crawler_input = {
        "Name": crawler_name,
        "Role": crawler_role,
        "DatabaseName": database,
        "Targets": {"S3Targets": [{"Path": p} for p in s3_paths]},
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
    }

    try:
        glue.get_crawler(Name=crawler_name)
        glue.update_crawler(**crawler_input)
        print(f"[update] crawler updated: {crawler_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(**crawler_input)
        print(f"[create] crawler created: {crawler_name}")


def wait_until_ready(glue, crawler_name: str, poll_seconds: int) -> None:
    while True:
        state = glue.get_crawler(Name=crawler_name)["Crawler"]["State"]
        if state == "READY":
            return
        print(f"[wait] crawler state={state}; waiting for READY...")
        time.sleep(poll_seconds)


def run_crawler(
    glue,
    *,
    crawler_name: str,
    no_wait: bool,
    poll_seconds: int,
    max_wait_seconds: int,
) -> None:
    wait_until_ready(glue, crawler_name, poll_seconds)

    glue.start_crawler(Name=crawler_name)
    print(f"[run] crawler started: {crawler_name}")

    if no_wait:
        print("[done] crawler started (no wait mode)")
        return

    elapsed = 0
    while True:
        c = glue.get_crawler(Name=crawler_name)["Crawler"]
        state = c.get("State", "UNKNOWN")
        last_crawl = c.get("LastCrawl", {})

        if state == "READY":
            status = last_crawl.get("Status", "UNKNOWN")
            if status != "SUCCEEDED":
                err = last_crawl.get("ErrorMessage", "")
                raise RuntimeError(
                    f"Crawler finished with status {status}. Error: {err}"
                )
            print(f"[success] crawler completed: status={status}")
            return

        if elapsed >= max_wait_seconds:
            raise TimeoutError(
                f"Timed out waiting for crawler after {max_wait_seconds} seconds."
            )

        print(f"[wait] crawler running (state={state})...")
        time.sleep(poll_seconds)
        elapsed += poll_seconds


def main() -> None:
    args = parse_args()

    if not args.crawler_role:
        raise RuntimeError(
            "Missing crawler role. Provide --crawler-role or set GLUE_CRAWLER_ROLE."
        )

    athena_root = f"s3://openalex-outputs/athena/{args.database}/"
    network_assets_root = (
        f"s3://openalex-outputs/cwts/{args.database}/network_assets/{args.version}/"
    )
    s3_paths = [athena_root, network_assets_root]

    crawler_name = args.crawler_name or sanitize_name(
        f"{args.database}_{args.version}_bootstrap"
    )

    print(f"[config] database={args.database}")
    print(f"[config] version={args.version}")
    print(f"[config] crawler_name={crawler_name}")
    print(f"[config] crawler_role={args.crawler_role}")
    print(f"[config] hard_reset={args.hard_reset}")
    print("[config] targets:")
    for path in s3_paths:
        print(f"  - {path}")

    glue = boto3.client("glue")
    iam = boto3.client("iam")
    crawler_role = resolve_crawler_role_arn(iam, args.crawler_role)
    print(f"[config] crawler_role_resolved={crawler_role}")

    try:
        if args.hard_reset:
            delete_crawler_if_exists(glue, crawler_name)
            reset_database(glue, args.database)

        ensure_database(glue, args.database)
        ensure_crawler(
            glue,
            crawler_name=crawler_name,
            database=args.database,
            crawler_role=crawler_role,
            s3_paths=s3_paths,
        )
        run_crawler(
            glue,
            crawler_name=crawler_name,
            no_wait=args.no_wait,
            poll_seconds=args.poll_seconds,
            max_wait_seconds=args.max_wait_seconds,
        )
    except ClientError as exc:
        raise RuntimeError(f"AWS Glue API error: {exc}") from exc


if __name__ == "__main__":
    main()