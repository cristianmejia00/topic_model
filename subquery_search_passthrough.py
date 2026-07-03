"""
subquery_search_passtrough.py
============================
No-filter subquery export: use all micro clusters and write the same output
artifacts as the topic and filter search scripts.

Outputs (under subqueries/everything/):
    matches/                 all micro clusters summary
    article_top10/           top-10 cited papers per micro cluster
    cluster_report_micro/    all micro report rows
    cluster_report_meso/     meso report rows for all referenced mesos
    cluster_report_macro/    macro report rows for all referenced macros
    top_countries/           top-20 countries per micro cluster
    top_institutions/        top-20 institutions per micro cluster

Requires: pandas, awswrangler, pyarrow
"""

from __future__ import annotations

import sys

import pandas as pd

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
DATABASE = "q20260629"
S3_STAGING = "s3://openalex-outputs/athena-staging/"
OUT_ROOT = "s3://openalex-outputs/classification/q20260629/subqueries/"
QUERY_FOLDER = "everything"
OUT_BASE = f"{OUT_ROOT}{QUERY_FOLDER}/"

TOP_PAPERS = 10
TOP_ENTITIES = 20


def _in_clause(ids) -> str:
    return ", ".join(str(int(x)) for x in ids)


def run_sql(sql: str) -> pd.DataFrame:
    import awswrangler as wr

    return wr.athena.read_sql_query(
        sql,
        database=DATABASE,
        s3_output=S3_STAGING,
        ctas_approach=False,
    )


def write(df: pd.DataFrame, name: str):
    import awswrangler as wr

    wr.s3.to_parquet(df, path=f"{OUT_BASE}{name}/", dataset=True, mode="overwrite")
    print(f"[save] {name}: {len(df):,} rows -> {OUT_BASE}{name}/")


def main():
    print("[load] cluster_report_micro ...")
    micro_rep = run_sql("SELECT * FROM cluster_report_micro")

    if micro_rep.empty:
        print("[stop] cluster_report_micro is empty; nothing to export.")
        sys.exit(0)

    final_micro = micro_rep["micro_cluster"].dropna().astype("int64").unique().tolist()
    meso_ids = micro_rep["meso_cluster"].dropna().astype("int64").unique().tolist()
    macro_ids = micro_rep["macro_cluster"].dropna().astype("int64").unique().tolist()

    print(
        f"[final] {len(final_micro):,} micro / {len(meso_ids):,} meso / "
        f"{len(macro_ids):,} macro clusters"
    )

    # Keep schema parity with other scripts by writing a matches summary.
    summary_cols = ["micro_cluster", "publications", "meso_cluster", "macro_cluster"]
    present_summary_cols = [c for c in summary_cols if c in micro_rep.columns]
    summary = micro_rep[present_summary_cols].copy()
    summary["filters_applied"] = "passthrough_all"
    write(summary.sort_values("micro_cluster"), "matches")

    micro_in = _in_clause(final_micro)

    write(
        run_sql(
            f"""
            SELECT id, title, citations, countries, institutions,
                   micro_cluster, meso_cluster, macro_cluster, publication_year
            FROM (
                SELECT id, title, citations, countries, institutions,
                       micro_cluster, meso_cluster, macro_cluster, publication_year,
                       ROW_NUMBER() OVER (PARTITION BY micro_cluster
                                          ORDER BY citations DESC, id) AS rn
                FROM article_report
                WHERE micro_cluster IN ({micro_in})
            )
            WHERE rn <= {TOP_PAPERS}
            """
        ),
        "article_top10",
    )

    write(micro_rep, "cluster_report_micro")
    write(
        run_sql(
            f"SELECT * FROM cluster_report_meso WHERE meso_cluster IN ({_in_clause(meso_ids)})"
        ),
        "cluster_report_meso",
    )
    write(
        run_sql(
            f"SELECT * FROM cluster_report_macro WHERE macro_cluster IN ({_in_clause(macro_ids)})"
        ),
        "cluster_report_macro",
    )

    write(
        run_sql(
            f"""
            SELECT micro_cluster, country, freq FROM (
                SELECT micro_cluster, country, COUNT(*) AS freq,
                       ROW_NUMBER() OVER (PARTITION BY micro_cluster
                                          ORDER BY COUNT(*) DESC, country) AS rn
                FROM article_report
                CROSS JOIN UNNEST(countries) AS t(country)
                WHERE micro_cluster IN ({micro_in})
                GROUP BY micro_cluster, country
            )
            WHERE rn <= {TOP_ENTITIES}
            """
        ),
        "top_countries",
    )

    write(
        run_sql(
            f"""
            SELECT micro_cluster, institution, freq FROM (
                SELECT micro_cluster, institution, COUNT(*) AS freq,
                       ROW_NUMBER() OVER (PARTITION BY micro_cluster
                                          ORDER BY COUNT(*) DESC, institution) AS rn
                FROM article_report
                CROSS JOIN UNNEST(institutions) AS t(institution)
                WHERE micro_cluster IN ({micro_in})
                GROUP BY micro_cluster, institution
            )
            WHERE rn <= {TOP_ENTITIES}
            """
        ),
        "top_institutions",
    )

    print(f"[done] all subsets written under {OUT_BASE}")


if __name__ == "__main__":
    main()
