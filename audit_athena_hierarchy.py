"""
Audit hierarchy consistency in Athena outputs.

Checks performed:
1) article_report null coverage for micro/meso/macro
2) micro -> meso/macro consistency (one parent per micro)
3) meso -> macro consistency (one parent per meso)
4) samples of problematic IDs

Usage:
    .venv/bin/python audit_athena_hierarchy.py --database q20260629
"""

from __future__ import annotations

import argparse

import awswrangler as wr


DEFAULT_STAGING = "s3://openalex-outputs/athena-staging/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit hierarchy consistency in Athena tables.")
    parser.add_argument("--database", required=True, help="Glue/Athena database name")
    parser.add_argument("--staging", default=DEFAULT_STAGING, help="Athena query output S3 path")
    parser.add_argument(
        "--show-limit",
        type=int,
        default=25,
        help="Number of problematic rows to print for each sample query",
    )
    return parser.parse_args()


def q(sql: str, *, database: str, staging: str):
    return wr.athena.read_sql_query(
        sql,
        database=database,
        s3_output=staging,
        ctas_approach=False,
    )


def main() -> None:
    args = parse_args()

    print(f"[config] database={args.database}")
    print(f"[config] staging={args.staging}")

    coverage = q(
        """
        SELECT
            COUNT(*) AS docs_total,
            SUM(CASE WHEN micro_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_micro,
            SUM(CASE WHEN meso_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_meso,
            SUM(CASE WHEN macro_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_macro,
            SUM(CASE WHEN micro_cluster IS NOT NULL AND meso_cluster IS NULL THEN 1 ELSE 0 END) AS docs_micro_without_meso,
            SUM(CASE WHEN micro_cluster IS NOT NULL AND macro_cluster IS NULL THEN 1 ELSE 0 END) AS docs_micro_without_macro
        FROM article_report
        """,
        database=args.database,
        staging=args.staging,
    )

    micro_consistency = q(
        """
        WITH per_micro AS (
            SELECT
                micro_cluster,
                COUNT(*) AS docs,
                COUNT(DISTINCT meso_cluster) AS meso_distinct_nonnull,
                COUNT(DISTINCT macro_cluster) AS macro_distinct_nonnull,
                SUM(CASE WHEN meso_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_meso,
                SUM(CASE WHEN macro_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_macro
            FROM article_report
            WHERE micro_cluster IS NOT NULL
            GROUP BY micro_cluster
        )
        SELECT
            COUNT(*) AS micro_total,
            SUM(CASE WHEN docs_null_meso > 0 THEN 1 ELSE 0 END) AS micro_with_any_null_meso,
            SUM(CASE WHEN docs_null_macro > 0 THEN 1 ELSE 0 END) AS micro_with_any_null_macro,
            SUM(CASE WHEN meso_distinct_nonnull = 0 THEN 1 ELSE 0 END) AS micro_without_any_meso,
            SUM(CASE WHEN macro_distinct_nonnull = 0 THEN 1 ELSE 0 END) AS micro_without_any_macro,
            SUM(CASE WHEN meso_distinct_nonnull > 1 THEN 1 ELSE 0 END) AS micro_with_multiple_meso,
            SUM(CASE WHEN macro_distinct_nonnull > 1 THEN 1 ELSE 0 END) AS micro_with_multiple_macro
        FROM per_micro
        """,
        database=args.database,
        staging=args.staging,
    )

    meso_consistency = q(
        """
        WITH per_meso AS (
            SELECT
                meso_cluster,
                COUNT(*) AS docs,
                COUNT(DISTINCT macro_cluster) AS macro_distinct_nonnull,
                SUM(CASE WHEN macro_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_macro
            FROM article_report
            WHERE meso_cluster IS NOT NULL
            GROUP BY meso_cluster
        )
        SELECT
            COUNT(*) AS meso_total,
            SUM(CASE WHEN docs_null_macro > 0 THEN 1 ELSE 0 END) AS meso_with_any_null_macro,
            SUM(CASE WHEN macro_distinct_nonnull = 0 THEN 1 ELSE 0 END) AS meso_without_any_macro,
            SUM(CASE WHEN macro_distinct_nonnull > 1 THEN 1 ELSE 0 END) AS meso_with_multiple_macro
        FROM per_meso
        """,
        database=args.database,
        staging=args.staging,
    )

    bad_micro = q(
        f"""
        WITH per_micro AS (
            SELECT
                micro_cluster,
                COUNT(*) AS docs,
                COUNT(DISTINCT meso_cluster) AS meso_distinct_nonnull,
                COUNT(DISTINCT macro_cluster) AS macro_distinct_nonnull,
                SUM(CASE WHEN meso_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_meso,
                SUM(CASE WHEN macro_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_macro
            FROM article_report
            WHERE micro_cluster IS NOT NULL
            GROUP BY micro_cluster
        )
        SELECT *
        FROM per_micro
        WHERE docs_null_meso > 0 OR docs_null_macro > 0 OR meso_distinct_nonnull > 1 OR macro_distinct_nonnull > 1
        ORDER BY docs DESC, micro_cluster
        LIMIT {int(args.show_limit)}
        """,
        database=args.database,
        staging=args.staging,
    )

    bad_meso = q(
        f"""
        WITH per_meso AS (
            SELECT
                meso_cluster,
                COUNT(*) AS docs,
                COUNT(DISTINCT macro_cluster) AS macro_distinct_nonnull,
                SUM(CASE WHEN macro_cluster IS NULL THEN 1 ELSE 0 END) AS docs_null_macro
            FROM article_report
            WHERE meso_cluster IS NOT NULL
            GROUP BY meso_cluster
        )
        SELECT *
        FROM per_meso
        WHERE docs_null_macro > 0 OR macro_distinct_nonnull > 1
        ORDER BY docs DESC, meso_cluster
        LIMIT {int(args.show_limit)}
        """,
        database=args.database,
        staging=args.staging,
    )

    print("\n=== article_report coverage ===")
    print(coverage.to_string(index=False))

    print("\n=== micro -> parents consistency ===")
    print(micro_consistency.to_string(index=False))

    print("\n=== meso -> macro consistency ===")
    print(meso_consistency.to_string(index=False))

    print(f"\n=== sample problematic micro clusters (top {args.show_limit}) ===")
    if bad_micro.empty:
        print("(none)")
    else:
        print(bad_micro.to_string(index=False))

    print(f"\n=== sample problematic meso clusters (top {args.show_limit}) ===")
    if bad_meso.empty:
        print("(none)")
    else:
        print(bad_meso.to_string(index=False))


if __name__ == "__main__":
    main()
