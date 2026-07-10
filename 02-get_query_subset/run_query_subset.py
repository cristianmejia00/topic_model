#!/usr/bin/env python3
"""
Run query-subset extraction from snapshot parquet tables.

Workflow:
1) Read required YAML config (no defaults for SNAPSHOT/QUERY).
2) Resolve SQL files from {SNAPSHOT}/{QUERY} folder, render placeholders, and validate.
3) Ensure database snapshot_{SNAPSHOT} exists.
4) Ensure source external tables exist (nodes_snapshot, edges_snapshot).
5) Execute nodes_query.sql then edges_query.sql in Athena.
"""

from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from pathlib import Path

import boto3
import yaml


TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}
FAILURE_STATES = {"FAILED", "CANCELLED"}

REQUIRED_CONFIG_KEYS = (
    "SNAPSHOT",
    "QUERY",
    "ATHENA_STAGING",
    "ATHENA_WORKGROUP",
)

SQL_TOKENS = (
    "{SNAPSHOT}",
    "{QUERY}",
    "{DATABASE}",
)


@dataclass(frozen=True)
class Settings:
    snapshot: str
    query: str
    athena_staging: str
    athena_workgroup: str
    database: str
    snapshot_root: str
    nodes_snapshot_path: str
    edges_snapshot_path: str
    query_dir: Path
    nodes_sql_path: Path
    edges_sql_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate inputs, bootstrap snapshot catalog objects, and run query subset SQL in order."
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to YAML config file for this step.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Run all validations and stop before Athena DDL/DML execution.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=5,
        help="Polling interval for Athena query status.",
    )
    return parser.parse_args()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {uri}")
    remainder = uri[5:]
    bucket, _, key = remainder.partition("/")
    if not bucket:
        raise ValueError(f"Missing S3 bucket in URI: {uri}")
    return bucket, key


def normalize_s3_prefix(uri: str) -> str:
    return uri if uri.endswith("/") else f"{uri}/"


def list_one_under_prefix(s3, prefix_uri: str) -> bool:
    bucket, key_prefix = parse_s3_uri(normalize_s3_prefix(prefix_uri))
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
    return bool(resp.get("KeyCount", 0))


def load_settings(config_path: Path, repo_dir: Path) -> Settings:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError("Config must be a YAML object with key/value pairs.")

    values: dict[str, str] = {}
    missing: list[str] = []
    for key in REQUIRED_CONFIG_KEYS:
        value = str(raw.get(key, "")).strip()
        if not value:
            missing.append(key)
        values[key] = value

    if missing:
        missing_keys = ", ".join(missing)
        raise RuntimeError(f"Missing required config keys with non-empty values: {missing_keys}")

    snapshot = values["SNAPSHOT"]
    query = values["QUERY"]

    snapshot_root = f"s3://openalex-results/snapshot_{snapshot}/"
    nodes_snapshot_path = f"{snapshot_root}nodes_snapshot/"
    edges_snapshot_path = f"{snapshot_root}edges_snapshot/"

    query_dir = repo_dir / snapshot / query
    nodes_sql_path = query_dir / "nodes_query.sql"
    edges_sql_path = query_dir / "edges_query.sql"

    database = f"snapshot_{snapshot}"

    return Settings(
        snapshot=snapshot,
        query=query,
        athena_staging=normalize_s3_prefix(values["ATHENA_STAGING"]),
        athena_workgroup=values["ATHENA_WORKGROUP"],
        database=database,
        snapshot_root=snapshot_root,
        nodes_snapshot_path=nodes_snapshot_path,
        edges_snapshot_path=edges_snapshot_path,
        query_dir=query_dir,
        nodes_sql_path=nodes_sql_path,
        edges_sql_path=edges_sql_path,
    )


def validate_sql_dependencies(nodes_sql: str, edges_sql: str) -> None:
    if not nodes_sql.strip():
        raise RuntimeError("nodes_query.sql is empty.")
    if not edges_sql.strip():
        raise RuntimeError("edges_query.sql is empty.")

    if not re.search(r"\bnodes_snapshot\b", nodes_sql, flags=re.IGNORECASE):
        raise RuntimeError("nodes_query.sql must reference source table nodes_snapshot.")
    if not re.search(r"\bedges_snapshot\b", edges_sql, flags=re.IGNORECASE):
        raise RuntimeError("edges_query.sql must reference source table edges_snapshot.")

    if re.search(r"\bnodes_subset\b", edges_sql, flags=re.IGNORECASE):
        raise RuntimeError("edges_query.sql references nodes_subset; use nodes_query instead.")
    if not re.search(r"\bnodes_query\b", edges_sql, flags=re.IGNORECASE):
        raise RuntimeError("edges_query.sql must reference nodes_query.")


def render_sql_template(sql_template: str, settings: Settings) -> str:
    rendered = sql_template
    replacements = {
        "{SNAPSHOT}": settings.snapshot,
        "{QUERY}": settings.query,
        "{DATABASE}": settings.database,
    }
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered


def ensure_no_unresolved_tokens(sql: str, label: str) -> None:
    unresolved = sorted({token for token in SQL_TOKENS if token in sql})
    if unresolved:
        names = ", ".join(unresolved)
        raise RuntimeError(f"Unresolved SQL placeholders in {label}: {names}")


def run_athena_query(
    athena,
    *,
    sql: str,
    staging: str,
    workgroup: str,
    database: str | None,
    poll_seconds: int,
) -> str:
    start_kwargs = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": staging},
        "WorkGroup": workgroup,
    }
    if database:
        start_kwargs["QueryExecutionContext"] = {"Database": database}

    resp = athena.start_query_execution(**start_kwargs)
    qid = resp["QueryExecutionId"]

    while True:
        meta = athena.get_query_execution(QueryExecutionId=qid)
        status = meta["QueryExecution"]["Status"]
        state = status["State"]

        if state in TERMINAL_STATES:
            if state in FAILURE_STATES:
                reason = status.get("StateChangeReason", "")
                raise RuntimeError(f"Athena query {qid} ended as {state}: {reason}")
            return qid

        time.sleep(poll_seconds)


def ensure_catalog_sources(settings: Settings, athena, poll_seconds: int) -> None:
    # Athena engine v3 follows Trino SQL, where CREATE SCHEMA is the supported form.
    create_db_sql = f"CREATE SCHEMA IF NOT EXISTS `{settings.database}`"
    qid = run_athena_query(
        athena,
        sql=create_db_sql,
        staging=settings.athena_staging,
        workgroup=settings.athena_workgroup,
        database=None,
        poll_seconds=poll_seconds,
    )
    print(f"[ok] ensured database {settings.database} (query={qid})")

    create_nodes_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS nodes_snapshot (
    id string,
    doi string,
    title string,
    abstract string,
    language string,
    type_openalex string,
    type_crossref string,
    citations bigint,
    publication_source string,
    countries array<string>,
    institutions array<string>,
    institutions_ror array<string>,
    institutions_type array<string>,
    authors array<string>
)
PARTITIONED BY (publication_year int)
STORED AS PARQUET
LOCATION '{settings.nodes_snapshot_path}'
""".strip()

    qid = run_athena_query(
        athena,
        sql=create_nodes_sql,
        staging=settings.athena_staging,
        workgroup=settings.athena_workgroup,
        database=settings.database,
        poll_seconds=poll_seconds,
    )
    print(f"[ok] ensured table {settings.database}.nodes_snapshot (query={qid})")

    # Existing catalogs may have been created before `abstract` was required.
    # Add it once when missing so downstream nodes_query CTAS can always select it.
    alter_nodes_sql = "ALTER TABLE nodes_snapshot ADD COLUMNS (abstract string)"
    try:
        qid = run_athena_query(
            athena,
            sql=alter_nodes_sql,
            staging=settings.athena_staging,
            workgroup=settings.athena_workgroup,
            database=settings.database,
            poll_seconds=poll_seconds,
        )
        print(f"[ok] ensured column nodes_snapshot.abstract (query={qid})")
    except RuntimeError as exc:
        msg = str(exc)
        if re.search(r"already exists|duplicate", msg, flags=re.IGNORECASE):
            print("[ok] nodes_snapshot.abstract already present")
        else:
            raise

    repair_nodes_sql = "MSCK REPAIR TABLE nodes_snapshot"
    qid = run_athena_query(
        athena,
        sql=repair_nodes_sql,
        staging=settings.athena_staging,
        workgroup=settings.athena_workgroup,
        database=settings.database,
        poll_seconds=poll_seconds,
    )
    print(f"[ok] repaired partitions for nodes_snapshot (query={qid})")

    create_edges_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS edges_snapshot (
    `from` string,
    `to` string,
    weight int
)
STORED AS PARQUET
LOCATION '{settings.edges_snapshot_path}'
""".strip()

    qid = run_athena_query(
        athena,
        sql=create_edges_sql,
        staging=settings.athena_staging,
        workgroup=settings.athena_workgroup,
        database=settings.database,
        poll_seconds=poll_seconds,
    )
    print(f"[ok] ensured table {settings.database}.edges_snapshot (query={qid})")


def main() -> None:
    args = parse_args()
    repo_dir = Path(__file__).resolve().parent

    settings = load_settings(args.config, repo_dir)
    print(f"[config] SNAPSHOT={settings.snapshot}")
    print(f"[config] QUERY={settings.query}")
    print(f"[config] DATABASE={settings.database}")
    print(f"[config] ATHENA_STAGING={settings.athena_staging}")
    print(f"[config] ATHENA_WORKGROUP={settings.athena_workgroup}")
    print(f"[config] QUERY_DIR={settings.query_dir}")

    if not settings.query_dir.exists() or not settings.query_dir.is_dir():
        raise RuntimeError(f"Query folder does not exist: {settings.query_dir}")
    if not settings.nodes_sql_path.exists():
        raise RuntimeError(f"Missing required SQL file: {settings.nodes_sql_path}")
    if not settings.edges_sql_path.exists():
        raise RuntimeError(f"Missing required SQL file: {settings.edges_sql_path}")

    s3 = boto3.client("s3")
    if not list_one_under_prefix(s3, settings.snapshot_root):
        raise RuntimeError(
            f"Snapshot root is missing or empty: {settings.snapshot_root}"
        )
    if not list_one_under_prefix(s3, settings.nodes_snapshot_path):
        raise RuntimeError(
            f"Required snapshot source path is missing or empty: {settings.nodes_snapshot_path}"
        )
    if not list_one_under_prefix(s3, settings.edges_snapshot_path):
        raise RuntimeError(
            f"Required snapshot source path is missing or empty: {settings.edges_snapshot_path}"
        )

    nodes_sql_raw = settings.nodes_sql_path.read_text(encoding="utf-8")
    edges_sql_raw = settings.edges_sql_path.read_text(encoding="utf-8")

    nodes_sql = render_sql_template(nodes_sql_raw, settings)
    edges_sql = render_sql_template(edges_sql_raw, settings)

    ensure_no_unresolved_tokens(nodes_sql, "nodes_query.sql")
    ensure_no_unresolved_tokens(edges_sql, "edges_query.sql")
    validate_sql_dependencies(nodes_sql, edges_sql)

    print("[ok] validations passed")
    if args.validate_only:
        print("[done] validate-only mode: skipping Athena DDL/DML execution")
        return

    athena = boto3.client("athena")
    ensure_catalog_sources(settings, athena, args.poll_seconds)

    qid = run_athena_query(
        athena,
        sql=nodes_sql,
        staging=settings.athena_staging,
        workgroup=settings.athena_workgroup,
        database=settings.database,
        poll_seconds=args.poll_seconds,
    )
    print(f"[done] nodes_query.sql succeeded (query={qid})")

    qid = run_athena_query(
        athena,
        sql=edges_sql,
        staging=settings.athena_staging,
        workgroup=settings.athena_workgroup,
        database=settings.database,
        poll_seconds=args.poll_seconds,
    )
    print(f"[done] edges_query.sql succeeded (query={qid})")

    print("[success] query subset extraction finished")


if __name__ == "__main__":
    main()
