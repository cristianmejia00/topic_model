"""
subquery_search_by_filters.py
=============================
Find micro clusters by applying numeric filters on cluster_report_micro,
then export the same focused subsets as the topic-based search.

Default filters (AND):
    ave_py >= 2022
    recency_py >= 0.4

Filter syntax:
    --filter "column>=value"
    --filter "column<=value"

You can pass one or multiple --filter arguments. Multiple filters are combined
with AND.

Outputs (under subqueries/{SUBQUERY}/):
    matches/                 matched micro clusters summary
    article_top10/           top-10 cited papers per matched micro cluster
    cluster_report_micro/    micro report rows for the matched clusters
    cluster_report_meso/     meso report rows for the parent mesos
    cluster_report_macro/    macro report rows for the parent macros
    top_countries/           top-20 countries per matched micro cluster
    top_institutions/        top-20 institutions per matched micro cluster

Requires: numpy, pandas, awswrangler, pyarrow
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass

import pandas as pd

from common_config import (
    DEFAULT_STAGING,
    DEFAULT_WORKGROUP,
    resolve_paths,
)

# ----------------------------------------------------------------------------
# DEFAULT PARAMETERS
# ----------------------------------------------------------------------------
DEFAULT_FILTERS = ["ave_py>=2022", "recency_py>=0.4"]
MIN_SIZE_DEFAULT = 50

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
DATABASE = ""
S3_STAGING = DEFAULT_STAGING
ATHENA_WORKGROUP = DEFAULT_WORKGROUP

TOP_PAPERS = 10
TOP_ENTITIES = 20

ALLOWED_FILTER_COLUMNS = {
    "publications",
    "ave_citations",
    "median_citations",
    "yearly_rank_citations",
    "ave_py",
    "median_py",
    "recency_py",
    "japan_count",
}


@dataclass(frozen=True)
class FilterRule:
    column: str
    op: str
    value: float


FILTER_RE = re.compile(r"^([a-z_]+)\s*(>=|<=)\s*(-?\d+(?:\.\d+)?)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create subquery outputs by filtering micro cluster numeric metrics."
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
    parser.add_argument(
        "--filter",
        action="append",
        dest="filters",
        help=(
            "Filter expression. Repeat for multiple filters, e.g. "
            "--filter 'ave_py>=2022' --filter 'recency_py>=0.4'"
        ),
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=MIN_SIZE_DEFAULT,
        help="Minimum publications required per micro cluster after filter matching.",
    )
    return parser.parse_args()


def parse_filter_rule(expr: str) -> FilterRule:
    m = FILTER_RE.match(expr.strip())
    if not m:
        raise ValueError(
            f"Invalid filter '{expr}'. Use format column>=value or column<=value."
        )

    column, op, raw_value = m.groups()
    if column not in ALLOWED_FILTER_COLUMNS:
        allowed = ", ".join(sorted(ALLOWED_FILTER_COLUMNS))
        raise ValueError(f"Unsupported filter column '{column}'. Allowed: {allowed}")

    return FilterRule(column=column, op=op, value=float(raw_value))


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


def write(df: pd.DataFrame, out_base: str, name: str):
    import awswrangler as wr

    wr.s3.to_parquet(df, path=f"{out_base}{name}/", dataset=True, mode="overwrite")
    print(f"[save] {name}: {len(df):,} rows -> {out_base}{name}/")


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


def apply_filters(micro_rep: pd.DataFrame, rules: list[FilterRule]) -> pd.DataFrame:
    filtered = micro_rep.copy()

    for rule in rules:
        if rule.column not in filtered.columns:
            raise ValueError(
                f"Column '{rule.column}' not found in cluster_report_micro table."
            )

        col = pd.to_numeric(filtered[rule.column], errors="coerce")
        before = len(filtered)
        if rule.op == ">=":
            mask = col >= rule.value
        else:
            mask = col <= rule.value

        filtered = filtered[mask].copy()
        print(
            f"[filter] {rule.column} {rule.op} {rule.value:g}: "
            f"{before:,} -> {len(filtered):,} micro clusters"
        )

    return filtered


def main():
    global DATABASE, S3_STAGING, ATHENA_WORKGROUP

    args = parse_args()
    paths = resolve_paths(
        snapshot=args.snapshot,
        query=args.query,
        subquery=args.subquery,
        query_folder=args.query_folder,
    )
    DATABASE = paths.database
    query_folder = paths.subquery
    S3_STAGING = args.staging
    ATHENA_WORKGROUP = args.workgroup
    filter_exprs = args.filters if args.filters else DEFAULT_FILTERS
    out_base = paths.subquery_base

    try:
        rules = [parse_filter_rule(expr) for expr in filter_exprs]
    except ValueError as exc:
        print(f"[error] {exc}")
        sys.exit(2)

    print("[config] database:", DATABASE)
    print("[config] snapshot:", paths.snapshot)
    print("[config] query:", paths.query)
    print("[config] query_folder:", query_folder)
    print("[config] staging:", S3_STAGING)
    print("[config] workgroup:", ATHENA_WORKGROUP)
    print("[config] filters (AND):")
    for expr in filter_exprs:
        print("   -", expr)
    print("[config] min_size:", args.min_size)

    print("[load] cluster_report_micro ...")
    micro_rep = run_sql("SELECT * FROM cluster_report_micro")

    try:
        filtered = apply_filters(micro_rep, rules)
    except ValueError as exc:
        print(f"[error] {exc}")
        sys.exit(2)

    if filtered.empty:
        print("[stop] no micro clusters matched the requested filters.")
        sys.exit(0)

    before_size = len(filtered)
    filtered = filtered[pd.to_numeric(filtered["publications"], errors="coerce") >= args.min_size]
    print(
        f"[filter] publications >= {args.min_size}: "
        f"{before_size:,} -> {len(filtered):,} micro clusters"
    )

    if filtered.empty:
        print(f"[stop] matches found, but none has >= {args.min_size} papers.")
        sys.exit(0)

    filtered = add_cluster_code(filtered)

    final_micro = filtered["micro_cluster"].astype("int64").tolist()
    meso_ids = filtered["meso_cluster"].dropna().astype("int64").unique().tolist()
    macro_ids = filtered["macro_cluster"].dropna().astype("int64").unique().tolist()

    print(
        f"[final] {len(final_micro):,} micro / {len(meso_ids):,} meso / "
        f"{len(macro_ids):,} macro clusters"
    )

    # Summary output mirrors the role of by-topic matches output, but keyed by filters.
    summary_cols = [
        "micro_cluster",
        "publications",
        "meso_cluster",
        "macro_cluster",
    ]
    for r in rules:
        if r.column not in summary_cols:
            summary_cols.append(r.column)

    summary = filtered[summary_cols].copy().sort_values(
        [r.column for r in rules if r.column in filtered.columns],
        ascending=False,
    )
    summary["filters_applied"] = " AND ".join(filter_exprs)
    write(summary, out_base, "matches")

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
        out_base,
        "article_top10",
    )

    write(filtered, out_base, "cluster_report_micro")
    write(
        run_sql(
            f"SELECT * FROM cluster_report_meso WHERE meso_cluster IN ({_in_clause(meso_ids)})"
        ),
        out_base,
        "cluster_report_meso",
    )
    write(
        run_sql(
            f"SELECT * FROM cluster_report_macro WHERE macro_cluster IN ({_in_clause(macro_ids)})"
        ),
        out_base,
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
        out_base,
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
        out_base,
        "top_institutions",
    )

    print(f"[done] all subsets written under {out_base}")


if __name__ == "__main__":
    main()
