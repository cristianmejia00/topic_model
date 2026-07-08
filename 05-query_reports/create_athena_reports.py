"""
Create the four Athena output tables used by the clustering/report pipeline.

Tables created in order:
1) article_report
2) cluster_report_micro
3) cluster_report_meso
4) cluster_report_macro

Usage:
    .venv/bin/python create_athena_reports.py --database q20260629 --snapshot 2026-06-26 --query q20260629
    .venv/bin/python create_athena_reports.py --database q20260629 --snapshot 2026-06-26 --query q20260629 --overwrite

Notes:
- Without --overwrite, CTAS will fail if target table/location already exists.
- With --overwrite, this script drops existing tables and deletes objects in each
  table's external S3 location before recreating.
"""

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass

import awswrangler as wr
import boto3

from root_common_config import QUERY_ENV_VAR, RootPaths, SNAPSHOT_ENV_VAR


DEFAULT_STAGING = "s3://openalex-outputs/athena-staging/"
DEFAULT_WORKGROUP = "primary"


@dataclass(frozen=True)
class TableSpec:
    name: str
    location: str
    sql: str


def build_table_specs(database: str, snapshot: str, query: str) -> list[TableSpec]:
    base = RootPaths(database=database, snapshot=snapshot, query=query).clustering_root

    article_sql = f"""
CREATE TABLE article_report
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['publication_year'],
    external_location = '{base}article_report/'
) AS
SELECT
    ns.id,
    ns.title,
    ns.citations,
    ns.countries,
    ns.institutions,
    lc.micro_cluster,
    lc.meso_cluster,
    lc.macro_cluster,
    ns.publication_year
FROM louvain_clusters_txt lc
INNER JOIN nodes_index_txt idx
    ON lc.node_id = idx.col0
INNER JOIN nodes ns
    ON idx.col1 = ns.id
""".strip()

    micro_sql = f"""
CREATE TABLE cluster_report_micro
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = '{base}cluster_report_micro/'
)
AS
WITH ranked AS (
    SELECT
        citations,
        countries,
        micro_cluster,
        meso_cluster,
        macro_cluster,
        TRY_CAST(publication_year AS integer) AS py,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY publication_year) = 1 THEN 0.5
            ELSE percent_rank() OVER (
                     PARTITION BY publication_year
                     ORDER BY citations
                 )
        END AS paper_rank
    FROM article_report
    WHERE micro_cluster IS NOT NULL
)
SELECT
    micro_cluster,
    CAST(arbitrary(meso_cluster) AS bigint) AS meso_cluster,
    CAST(arbitrary(macro_cluster) AS bigint) AS macro_cluster,
    COUNT(*) AS publications,
    MIN(citations) AS min_citations,
    ROUND(AVG(citations), 2) AS ave_citations,
    APPROX_PERCENTILE(citations, 0.5) AS median_citations,
    MAX(citations) AS max_citations,
    ROUND(AVG(paper_rank), 4) AS yearly_rank_citations,
    MIN(py) AS min_py,
    ROUND(AVG(py), 1) AS ave_py,
    APPROX_PERCENTILE(py, 0.5) AS median_py,
    MAX(py) AS max_py,
    ROUND(CAST(COUNT_IF(py >= 2024) AS double) / COUNT(*), 4) AS recency_py,
    COUNT_IF(contains(countries, 'JP')) AS japan_count
FROM ranked
GROUP BY micro_cluster
ORDER BY publications DESC
""".strip()

    meso_sql = f"""
CREATE TABLE cluster_report_meso
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = '{base}cluster_report_meso/'
)
AS
WITH ranked AS (
    SELECT
        citations,
        countries,
        micro_cluster,
        meso_cluster,
        macro_cluster,
        TRY_CAST(publication_year AS integer) AS py,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY publication_year) = 1 THEN 0.5
            ELSE percent_rank() OVER (
                     PARTITION BY publication_year
                     ORDER BY citations
                 )
        END AS paper_rank
    FROM article_report
    WHERE meso_cluster IS NOT NULL
)
SELECT
    CAST(meso_cluster AS bigint) AS meso_cluster,
    CAST(arbitrary(macro_cluster) AS bigint) AS macro_cluster,
    COUNT(DISTINCT micro_cluster) AS micro_clusters,
    COUNT(*) AS publications,
    MIN(citations) AS min_citations,
    ROUND(AVG(citations), 2) AS ave_citations,
    APPROX_PERCENTILE(citations, 0.5) AS median_citations,
    MAX(citations) AS max_citations,
    ROUND(AVG(paper_rank), 4) AS yearly_rank_citations,
    MIN(py) AS min_py,
    ROUND(AVG(py), 1) AS ave_py,
    APPROX_PERCENTILE(py, 0.5) AS median_py,
    MAX(py) AS max_py,
    ROUND(CAST(COUNT_IF(py >= 2024) AS double) / COUNT(*), 4) AS recency_py,
    COUNT_IF(contains(countries, 'JP')) AS japan_count
FROM ranked
GROUP BY meso_cluster
ORDER BY publications DESC
""".strip()

    macro_sql = f"""
CREATE TABLE cluster_report_macro
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = '{base}cluster_report_macro/'
)
AS
WITH ranked AS (
    SELECT
        citations,
        countries,
        micro_cluster,
        meso_cluster,
        macro_cluster,
        TRY_CAST(publication_year AS integer) AS py,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY publication_year) = 1 THEN 0.5
            ELSE percent_rank() OVER (
                     PARTITION BY publication_year
                     ORDER BY citations
                 )
        END AS paper_rank
    FROM article_report
    WHERE macro_cluster IS NOT NULL
)
SELECT
    CAST(macro_cluster AS bigint) AS macro_cluster,
    COUNT(DISTINCT meso_cluster) AS meso_clusters,
    COUNT(DISTINCT micro_cluster) AS micro_clusters,
    COUNT(*) AS publications,
    MIN(citations) AS min_citations,
    ROUND(AVG(citations), 2) AS ave_citations,
    APPROX_PERCENTILE(citations, 0.5) AS median_citations,
    MAX(citations) AS max_citations,
    ROUND(AVG(paper_rank), 4) AS yearly_rank_citations,
    MIN(py) AS min_py,
    ROUND(AVG(py), 1) AS ave_py,
    APPROX_PERCENTILE(py, 0.5) AS median_py,
    MAX(py) AS max_py,
    ROUND(CAST(COUNT_IF(py >= 2024) AS double) / COUNT(*), 4) AS recency_py,
    COUNT_IF(contains(countries, 'JP')) AS japan_count
FROM ranked
GROUP BY macro_cluster
ORDER BY publications DESC
""".strip()

    return [
        TableSpec("article_report", f"{base}article_report/", article_sql),
        TableSpec("cluster_report_micro", f"{base}cluster_report_micro/", micro_sql),
        TableSpec("cluster_report_meso", f"{base}cluster_report_meso/", meso_sql),
        TableSpec("cluster_report_macro", f"{base}cluster_report_macro/", macro_sql),
    ]


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create article/cluster Athena report tables.")
    parser.add_argument("--database", required=True, help="Glue/Athena database name.")
    parser.add_argument(
        "--snapshot",
        default=os.getenv(SNAPSHOT_ENV_VAR, "").strip() or None,
        help=(
            "Snapshot token for S3 outputs, e.g. 2026-06-26. "
            f"Defaults to ${SNAPSHOT_ENV_VAR} if set."
        ),
    )
    parser.add_argument(
        "--query",
        default=os.getenv(QUERY_ENV_VAR, "").strip() or None,
        help=(
            "Query token for S3 outputs, e.g. q20260629. "
            f"Defaults to ${QUERY_ENV_VAR} if set."
        ),
    )
    parser.add_argument("--staging", default=DEFAULT_STAGING, help="Athena query result S3 path.")
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP, help="Athena workgroup.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Drop existing tables and clear each destination S3 prefix before recreating.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.snapshot:
        raise RuntimeError(f"Missing --snapshot (or env {SNAPSHOT_ENV_VAR}).")
    if not args.query:
        raise RuntimeError(f"Missing --query (or env {QUERY_ENV_VAR}).")

    specs = build_table_specs(args.database, args.snapshot, args.query)
    athena = boto3.client("athena")

    print(f"[config] database={args.database} workgroup={args.workgroup}")
    print(f"[config] snapshot={args.snapshot} query={args.query}")
    print(f"[config] staging={args.staging}")

    if args.overwrite:
        print("[cleanup] --overwrite enabled: dropping tables and clearing S3 destinations")
        for spec in specs:
            drop_sql = f"DROP TABLE IF EXISTS {spec.name}"
            qid = run_athena_query(
                athena,
                drop_sql,
                database=args.database,
                staging=args.staging,
                workgroup=args.workgroup,
            )
            print(f"  - dropped {spec.name} (query={qid})")
            wr.s3.delete_objects(path=spec.location)
            print(f"  - cleared {spec.location}")

    for idx, spec in enumerate(specs, start=1):
        print(f"\n[{idx}/{len(specs)}] creating {spec.name}")
        qid = run_athena_query(
            athena,
            spec.sql,
            database=args.database,
            staging=args.staging,
            workgroup=args.workgroup,
        )
        print(f"[done] {spec.name} (query={qid})")

    print("\n[success] all 4 tables were created.")


if __name__ == "__main__":
    main()
