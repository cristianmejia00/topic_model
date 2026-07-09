"""
subquery_search_passthrough.py
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

import argparse
import sys

import pandas as pd

from common_config import (
    DEFAULT_STAGING,
    DEFAULT_WORKGROUP,
    resolve_paths,
)

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
DATABASE = ""
S3_STAGING = DEFAULT_STAGING
ATHENA_WORKGROUP = DEFAULT_WORKGROUP
OUT_BASE = ""

TOP_PAPERS = 10
TOP_ENTITIES = 20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create subquery outputs for all micro clusters (no filters)."
    )
    parser.add_argument(
        "--snapshot",
        default=None,
        help="Snapshot token, e.g. 2026-06-26.",
    )
    parser.add_argument(
        "--query",
        default=None,
        help="Query token, e.g. q20260629.",
    )
    parser.add_argument(
        "--subquery",
        default=None,
        help="Subquery folder name under clustering/subqueries/.",
    )
    parser.add_argument(
        "--query-folder",
        default=None,
        help="Deprecated alias for --subquery.",
    )
    parser.add_argument(
        "--staging",
        default=DEFAULT_STAGING,
        help="Athena query output S3 path.",
    )
    parser.add_argument(
        "--workgroup",
        default=DEFAULT_WORKGROUP,
        help="Athena workgroup.",
    )
    return parser.parse_args()


def _in_clause(ids) -> str:
    return ", ".join(str(int(x)) for x in ids)


def run_sql(sql: str) -> pd.DataFrame:
    import awswrangler as wr

    return wr.athena.read_sql_query(
        sql,
        database=DATABASE,
        s3_output=S3_STAGING,
        workgroup=ATHENA_WORKGROUP,
        ctas_approach=False,
    )


def write(df: pd.DataFrame, name: str):
    import awswrangler as wr

    wr.s3.to_parquet(df, path=f"{OUT_BASE}{name}/", dataset=True, mode="overwrite")
    print(f"[save] {name}: {len(df):,} rows -> {OUT_BASE}{name}/")


def add_cluster_code(micro_rep: pd.DataFrame) -> pd.DataFrame:
    required = {"micro_cluster", "macro_cluster", "publications"}
    missing = sorted(c for c in required if c not in micro_rep.columns)
    if missing:
        raise KeyError(f"cluster_report_micro missing required columns for cluster_code: {missing}")

    out = micro_rep.copy()
    rank_base = out[["micro_cluster", "macro_cluster", "publications"]].copy()
    rank_base["micro_cluster"] = pd.to_numeric(rank_base["micro_cluster"], errors="coerce")
    rank_base["macro_cluster"] = pd.to_numeric(rank_base["macro_cluster"], errors="coerce")
    rank_base["publications"] = pd.to_numeric(rank_base["publications"], errors="coerce")
    rank_base = rank_base.dropna(subset=["micro_cluster", "macro_cluster"]).copy()

    if rank_base.empty:
        out["cluster_code"] = ""
        return out

    rank_base["micro_cluster"] = rank_base["micro_cluster"].astype("int64")
    rank_base["macro_cluster"] = rank_base["macro_cluster"].astype("int64")
    rank_base = (
        rank_base.groupby(["micro_cluster", "macro_cluster"], as_index=False)["publications"]
        .max()
        .fillna({"publications": 0.0})
    )

    macro_rank = (
        rank_base.groupby("macro_cluster", as_index=False)["publications"]
        .sum()
        .sort_values(["publications", "macro_cluster"], ascending=[False, True])
        .reset_index(drop=True)
    )
    macro_rank["macro_display_id"] = range(1, len(macro_rank) + 1)

    micro_rank = rank_base.sort_values(
        ["macro_cluster", "publications", "micro_cluster"],
        ascending=[True, False, True],
    ).copy()
    micro_rank["micro_rank"] = micro_rank.groupby("macro_cluster").cumcount() + 1
    cluster_map = micro_rank.merge(
        macro_rank[["macro_cluster", "macro_display_id"]],
        on="macro_cluster",
        how="left",
    )
    cluster_map["cluster_code"] = (
        cluster_map["macro_display_id"].astype("int64").astype(str)
        + "-"
        + cluster_map["micro_rank"].astype("int64").astype(str)
    )

    out["_micro_key"] = pd.to_numeric(out["micro_cluster"], errors="coerce")
    out["_macro_key"] = pd.to_numeric(out["macro_cluster"], errors="coerce")
    mapped = cluster_map.rename(
        columns={"micro_cluster": "_micro_key", "macro_cluster": "_macro_key"}
    )
    out = out.merge(mapped[["_micro_key", "_macro_key", "cluster_code"]], on=["_micro_key", "_macro_key"], how="left")
    out["cluster_code"] = out["cluster_code"].fillna("")
    out = out.drop(columns=["_micro_key", "_macro_key"])
    return out


def main():
    global DATABASE, OUT_BASE, S3_STAGING, ATHENA_WORKGROUP

    args = parse_args()
    paths = resolve_paths(
        snapshot=args.snapshot,
        query=args.query,
        subquery=args.subquery,
        query_folder=args.query_folder,
    )
    DATABASE = paths.database
    OUT_BASE = paths.subquery_base
    S3_STAGING = args.staging
    ATHENA_WORKGROUP = args.workgroup

    print("[config] database:", DATABASE)
    print("[config] snapshot:", paths.snapshot)
    print("[config] query:", paths.query)
    print("[config] query_folder:", paths.subquery)
    print("[config] staging:", S3_STAGING)
    print("[config] workgroup:", ATHENA_WORKGROUP)

    print("[load] cluster_report_micro ...")
    micro_rep = run_sql("SELECT * FROM cluster_report_micro")

    if micro_rep.empty:
        print("[stop] cluster_report_micro is empty; nothing to export.")
        sys.exit(0)

    micro_rep = add_cluster_code(micro_rep)

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
            SELECT micro_cluster, country, freq, avg_publication_year, avg_citation FROM (
                SELECT micro_cluster,
                       country,
                       COUNT(*) AS freq,
                       ROUND(AVG(TRY_CAST(publication_year AS double)), 1) AS avg_publication_year,
                       ROUND(AVG(citations), 2) AS avg_citation,
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
            SELECT micro_cluster, institution, freq, avg_publication_year, avg_citation FROM (
                SELECT micro_cluster,
                       institution,
                       COUNT(*) AS freq,
                       ROUND(AVG(TRY_CAST(publication_year AS double)), 1) AS avg_publication_year,
                       ROUND(AVG(citations), 2) AS avg_citation,
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
