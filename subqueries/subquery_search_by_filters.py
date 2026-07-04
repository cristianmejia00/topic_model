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

Outputs (under subqueries/{QUERY_FOLDER}/):
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

# ----------------------------------------------------------------------------
# DEFAULT PARAMETERS
# ----------------------------------------------------------------------------
DEFAULT_QUERY_FOLDER = "filters_ave_py_ge_2022_and_recency_py_ge_0_4"
DEFAULT_FILTERS = ["ave_py>=2022", "recency_py>=0.4"]
MIN_SIZE_DEFAULT = 30

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
DATABASE = "q20260629"
S3_STAGING = "s3://openalex-outputs/athena-staging/"
OUT_ROOT = "s3://openalex-outputs/classification/q20260629/subqueries/"

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
        "--query-folder",
        default=DEFAULT_QUERY_FOLDER,
        help="S3 subfolder name under subqueries/ for this run.",
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
        ctas_approach=False,
    )


def write(df: pd.DataFrame, out_base: str, name: str):
    import awswrangler as wr

    wr.s3.to_parquet(df, path=f"{out_base}{name}/", dataset=True, mode="overwrite")
    print(f"[save] {name}: {len(df):,} rows -> {out_base}{name}/")


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
    args = parse_args()
    filter_exprs = args.filters if args.filters else DEFAULT_FILTERS
    out_base = f"{OUT_ROOT}{args.query_folder}/"

    try:
        rules = [parse_filter_rule(expr) for expr in filter_exprs]
    except ValueError as exc:
        print(f"[error] {exc}")
        sys.exit(2)

    print("[config] query_folder:", args.query_folder)
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
