"""
Optionally migrate leaf assignments by promoting meso clusters to effective micro.

For a selected Glue/Athena database + version, this script:
1) Validates source table schema and migration preconditions.
2) Creates a migrated temporary table where micro_cluster := meso_cluster.
3) Preserves lineage in columns:
   - micro_cluster_original
   - micro_assignment_policy
   - micro_assignment_version
   - micro_assignment_run_id
4) Replaces the canonical source table used by downstream scripts
    (default: louvain_clusters_txt) via a robust CTAS flow:
    - archive snapshot table (run-specific)
    - drop canonical source table
    - recreate canonical source table from migrated temp
5) Writes a migration manifest JSON to S3.

Usage:
    .venv/bin/python migrate_meso_as_micro.py \
        --database quantum \
        --version version3

Dry-run example:
    .venv/bin/python migrate_meso_as_micro.py \
        --database quantum \
        --version version3 \
        --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import awswrangler as wr
import boto3


DEFAULT_STAGING = "s3://openalex-outputs/athena-staging/"
DEFAULT_WORKGROUP = "primary"
DEFAULT_SOURCE_TABLE = "louvain_clusters_txt"
MIGRATION_POLICY = "meso_as_micro"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Promote meso assignments to effective micro assignments in source table."
    )
    parser.add_argument("--database", required=True, help="Glue/Athena database name, e.g. quantum.")
    parser.add_argument("--version", required=True, help="Version tag, e.g. version3.")
    parser.add_argument(
        "--source-table",
        default=DEFAULT_SOURCE_TABLE,
        help=f"Source table to migrate (default: {DEFAULT_SOURCE_TABLE}).",
    )
    parser.add_argument("--run-id", default=None, help="Optional run identifier. Auto-generated if omitted.")
    parser.add_argument("--staging", default=DEFAULT_STAGING, help="Athena query output S3 path.")
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP, help="Athena workgroup.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print plan without writing changes.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow running even if source table already appears migrated.",
    )
    return parser.parse_args()


def sanitize_token(text: str, max_len: int = 48) -> str:
    token = re.sub(r"[^a-zA-Z0-9_]+", "_", str(text).strip()).strip("_").lower()
    if not token:
        token = "na"
    return token[:max_len]


def safe_table_name(name: str) -> str:
    cleaned = sanitize_token(name, max_len=255)
    if not re.match(r"^[a-z_][a-z0-9_]*$", cleaned):
        raise ValueError(f"Invalid table identifier: {name}")
    return cleaned


def quote_literal(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def run_athena_query(
    client,
    sql: str,
    *,
    database: str,
    staging: str,
    workgroup: str,
    poll_seconds: int = 5,
) -> str:
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": staging},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]

    while True:
        meta = client.get_query_execution(QueryExecutionId=qid)
        state = meta["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            if state != "SUCCEEDED":
                reason = meta["QueryExecution"]["Status"].get("StateChangeReason", "")
                raise RuntimeError(f"Athena query {qid} ended as {state}: {reason}")
            return qid
        time.sleep(poll_seconds)


def q(sql: str, *, database: str, staging: str):
    return wr.athena.read_sql_query(
        sql,
        database=database,
        s3_output=staging,
        ctas_approach=False,
    )


def get_table_columns(*, database: str, table: str, staging: str) -> list[str]:
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = {quote_literal(database)}
      AND table_name = {quote_literal(table)}
    ORDER BY ordinal_position
    """
    df = q(sql, database=database, staging=staging)
    return [str(x) for x in df["column_name"].tolist()]


def table_exists(*, database: str, table: str, staging: str) -> bool:
    sql = f"""
    SELECT COUNT(*) AS n
    FROM information_schema.tables
    WHERE table_schema = {quote_literal(database)}
      AND table_name = {quote_literal(table)}
    """
    df = q(sql, database=database, staging=staging)
    return int(df.iloc[0]["n"]) > 0


def snapshot_counts(*, database: str, table: str, staging: str) -> dict[str, int]:
    sql = f"""
    SELECT
        COUNT(*) AS rows_total,
        COUNT(DISTINCT micro_cluster) AS distinct_micro,
        COUNT(DISTINCT meso_cluster) AS distinct_meso,
        COUNT(DISTINCT macro_cluster) AS distinct_macro
    FROM {table}
    """
    df = q(sql, database=database, staging=staging)
    row = df.iloc[0]
    return {
        "rows_total": int(row["rows_total"]),
        "distinct_micro": int(row["distinct_micro"]),
        "distinct_meso": int(row["distinct_meso"]),
        "distinct_macro": int(row["distinct_macro"]),
    }


def build_migrated_select(
    *,
    columns: list[str],
    version: str,
    run_id: str,
) -> str:
    select_parts: list[str] = []

    for col in columns:
        if col == "micro_cluster":
            select_parts.append("CAST(meso_cluster AS bigint) AS micro_cluster")
            continue
        if col in {
            "micro_cluster_original",
            "micro_assignment_policy",
            "micro_assignment_version",
            "micro_assignment_run_id",
        }:
            continue
        select_parts.append(col)

    original_expr = "micro_cluster_original" if "micro_cluster_original" in columns else "micro_cluster"
    select_parts.append(f"CAST({original_expr} AS bigint) AS micro_cluster_original")
    select_parts.append(f"{quote_literal(MIGRATION_POLICY)} AS micro_assignment_policy")
    select_parts.append(f"{quote_literal(version)} AS micro_assignment_version")
    select_parts.append(f"{quote_literal(run_id)} AS micro_assignment_run_id")

    return ",\n    ".join(select_parts)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    without = uri[len("s3://") :]
    bucket, _, key = without.partition("/")
    return bucket, key


def write_manifest(manifest_path: str, payload: dict[str, Any]) -> None:
    bucket, key = parse_s3_uri(manifest_path)
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def main() -> None:
    args = parse_args()

    database = safe_table_name(args.database)
    source_table = safe_table_name(args.source_table)
    version_token = sanitize_token(args.version)
    run_id = sanitize_token(args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"), max_len=64)

    migrated_temp_table = safe_table_name(f"{source_table}__migrated__{version_token}__{run_id}")
    archive_table = safe_table_name(f"{source_table}__archive__{version_token}__{run_id}")

    migration_root = (
        f"s3://openalex-outputs/classification/{database}/"
        f"migrations/meso_as_micro/{version_token}/{run_id}/"
    )
    migrated_location = f"{migration_root}migrated_table/"
    archive_location = f"{migration_root}archive_snapshot/"
    canonical_location = f"{migration_root}canonical_replacement/"
    rollback_location = f"{migration_root}rollback_restore/"
    manifest_path = f"{migration_root}manifest.json"

    print(f"[config] database={database}")
    print(f"[config] version={args.version}")
    print(f"[config] source_table={source_table}")
    print(f"[config] migrated_temp_table={migrated_temp_table}")
    print(f"[config] archive_table={archive_table}")
    print(f"[config] run_id={run_id}")
    print(f"[config] dry_run={args.dry_run}")
    print(f"[config] force={args.force}")

    cols = get_table_columns(database=database, table=source_table, staging=args.staging)
    if not cols:
        raise RuntimeError(f"Source table not found or has no columns: {database}.{source_table}")

    required = {"node_id", "micro_cluster", "meso_cluster", "macro_cluster"}
    missing = sorted(required - set(cols))
    if missing:
        raise RuntimeError(
            "Source table is missing required columns for downstream compatibility: "
            + ", ".join(missing)
        )

    already_migrated = "micro_assignment_policy" in cols or "micro_cluster_original" in cols
    if already_migrated and not args.force:
        raise RuntimeError(
            "Source table already appears migrated (found migration lineage columns). "
            "Re-run with --force only if this is intentional."
        )

    if table_exists(database=database, table=archive_table, staging=args.staging):
        raise RuntimeError(f"Archive table already exists: {archive_table}")
    if table_exists(database=database, table=migrated_temp_table, staging=args.staging):
        raise RuntimeError(f"Temporary migrated table already exists: {migrated_temp_table}")

    before = snapshot_counts(database=database, table=source_table, staging=args.staging)
    if before["rows_total"] == 0:
        raise RuntimeError("Source table has zero rows; refusing to migrate.")

    print("[precheck] source counts:", before)

    select_sql = build_migrated_select(columns=cols, version=args.version, run_id=run_id)
    create_migrated_sql = f"""
    CREATE TABLE {migrated_temp_table}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = '{migrated_location}'
    ) AS
    SELECT
        {select_sql}
    FROM {source_table}
    """.strip()

    preview_sql = f"""
    SELECT
        COUNT(*) AS rows_total,
        COUNT(DISTINCT micro_cluster) AS distinct_micro,
        COUNT(DISTINCT meso_cluster) AS distinct_meso,
        COUNT(DISTINCT macro_cluster) AS distinct_macro
    FROM (
        SELECT
            CAST(meso_cluster AS bigint) AS micro_cluster,
            meso_cluster,
            macro_cluster
        FROM {source_table}
    ) x
    """.strip()

    preview = q(preview_sql, database=database, staging=args.staging).iloc[0].to_dict()
    preview = {k: int(v) for k, v in preview.items()}
    print("[precheck] projected post-migration counts:", preview)

    if args.dry_run:
        print("[dry-run] planned migration complete; no tables were modified")
        return

    athena = boto3.client("athena")
    run_athena_query(
        athena,
        create_migrated_sql,
        database=database,
        staging=args.staging,
        workgroup=args.workgroup,
    )
    print(f"[create] migrated table created: {migrated_temp_table}")

    archive_snapshot_sql = f"""
    CREATE TABLE {archive_table}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = '{archive_location}'
    ) AS
    SELECT *
    FROM {source_table}
    """.strip()

    run_athena_query(
        athena,
        archive_snapshot_sql,
        database=database,
        staging=args.staging,
        workgroup=args.workgroup,
    )
    print(f"[archive] snapshot table created: {archive_table}")

    drop_source_sql = f"DROP TABLE {source_table}"
    run_athena_query(
        athena,
        drop_source_sql,
        database=database,
        staging=args.staging,
        workgroup=args.workgroup,
    )
    print(f"[drop] removed previous canonical table: {source_table}")

    promote_sql = f"""
    CREATE TABLE {source_table}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = '{canonical_location}'
    ) AS
    SELECT *
    FROM {migrated_temp_table}
    """.strip()

    try:
        run_athena_query(
            athena,
            promote_sql,
            database=database,
            staging=args.staging,
            workgroup=args.workgroup,
        )
    except Exception as exc:
        # Best-effort rollback to keep canonical name available.
        print("[error] failed to recreate canonical table; attempting rollback from archive snapshot")
        rollback_sql = f"""
        CREATE TABLE {source_table}
        WITH (
            format = 'PARQUET',
            write_compression = 'SNAPPY',
            external_location = '{rollback_location}'
        ) AS
        SELECT *
        FROM {archive_table}
        """.strip()
        try:
            run_athena_query(
                athena,
                rollback_sql,
                database=database,
                staging=args.staging,
                workgroup=args.workgroup,
            )
            print("[rollback] restored canonical source table name")
        except Exception as rollback_exc:
            print(f"[rollback] failed: {rollback_exc}")
        raise RuntimeError(f"Promotion failed: {exc}") from exc

    print(f"[promote] canonical table recreated from migrated data: {source_table}")

    try:
        drop_temp_sql = f"DROP TABLE {migrated_temp_table}"
        run_athena_query(
            athena,
            drop_temp_sql,
            database=database,
            staging=args.staging,
            workgroup=args.workgroup,
        )
        print(f"[cleanup] dropped migrated temp table: {migrated_temp_table}")
    except Exception as cleanup_exc:
        print(f"[cleanup] could not drop migrated temp table: {cleanup_exc}")

    after = snapshot_counts(database=database, table=source_table, staging=args.staging)
    print("[postcheck] source counts:", after)

    manifest = {
        "migration_policy": MIGRATION_POLICY,
        "database": database,
        "version": args.version,
        "run_id": run_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source_table": source_table,
        "archive_table": archive_table,
        "archive_location": archive_location,
        "migrated_location": migrated_location,
        "canonical_location": canonical_location,
        "staging": args.staging,
        "workgroup": args.workgroup,
        "before": before,
        "projected": preview,
        "after": after,
    }
    write_manifest(manifest_path, manifest)
    print(f"[manifest] wrote: {manifest_path}")


if __name__ == "__main__":
    main()
