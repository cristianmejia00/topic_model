"""Generate a UTokyo-focused Excel workbook for one subquery.

This script builds one workbook with two sheets:
1) cluster_summary_utokyo
2) article_report_utokyo_top10

Output path:
    utokyo/{database}/{subquery}/utokyo_cluster_and_articles.xlsx

Data sources:
- Subquery micro summary from
    s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/clustering/subqueries/{SUBQUERY}/cluster_report_micro/
- Optional names from
    s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/clustering/subqueries/{SUBQUERY}/cluster_names/
- Canonical article table (Athena): article_report
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
import re
from typing import Any

import awswrangler as wr
import pandas as pd

from common_config import (
    DEFAULT_STAGING,
    DEFAULT_WORKGROUP,
    resolve_paths,
)

UTOKYO_INSTITUTION = "The University of Tokyo"
WORKBOOK_NAME = "utokyo_cluster_and_articles.xlsx"
ILLEGAL_XLSX_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a UTokyo-focused two-sheet Excel workbook for one subquery."
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


def read_subset(base: str, name: str, required: bool = True) -> pd.DataFrame:
    path = f"{base}{name}/"
    try:
        return wr.s3.read_parquet(path)
    except Exception as exc:
        if required:
            raise RuntimeError(f"Could not read required subset at {path}: {exc}") from exc
        print(f"[warn] optional subset not available at {path}: {exc}")
        return pd.DataFrame()


def pick_col(df: pd.DataFrame, candidates: list[str], required: bool = False) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    if required:
        raise KeyError(f"Missing required columns. Expected one of: {candidates}")
    return None


def sanitize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def sanitize_excel_text(value: Any) -> Any:
    if isinstance(value, str):
        return ILLEGAL_XLSX_CHARS_RE.sub("", value)
    return value


def run_sql(sql: str, *, database: str, staging: str, workgroup: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql,
        database=database,
        s3_output=staging,
        workgroup=workgroup,
        ctas_approach=False,
    )


def in_clause(values: list[int]) -> str:
    return ", ".join(str(int(v)) for v in values)


def build_micro_base(micro_rep: pd.DataFrame, names: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
    micro_id_col = pick_col(micro_rep, ["micro_cluster", "cluster"], required=True)
    pub_col = pick_col(micro_rep, ["publications"], required=True)
    rank_col = pick_col(
        micro_rep,
        ["yearly_rank_citations", "ranked_citation", "ranked_citation_score"],
        required=False,
    )
    ave_py_col = pick_col(
        micro_rep,
        ["ave_py", "avg_publication_year", "average_publication_year"],
        required=False,
    )
    ave_cit_col = pick_col(
        micro_rep,
        ["ave_citations", "avg_citations", "average_citations"],
        required=False,
    )

    out = micro_rep.copy()
    out[micro_id_col] = pd.to_numeric(out[micro_id_col], errors="coerce")
    out = out.dropna(subset=[micro_id_col]).copy()
    out[micro_id_col] = out[micro_id_col].astype("int64")
    out = out.rename(columns={micro_id_col: "micro_cluster"})

    out[pub_col] = pd.to_numeric(out[pub_col], errors="coerce")
    sort_cols = [pub_col]
    sort_orders = [False]
    if rank_col and rank_col in out.columns:
        out[rank_col] = pd.to_numeric(out[rank_col], errors="coerce")
        sort_cols.append(rank_col)
        sort_orders.append(False)
    sort_cols.append("micro_cluster")
    sort_orders.append(True)

    out = out.sort_values(sort_cols, ascending=sort_orders).reset_index(drop=True)
    out["display_id"] = range(1, len(out) + 1)
    out["global_id"] = out["micro_cluster"]

    # Normalize total metrics into explicit *_total columns.
    out["publications_total"] = pd.to_numeric(out[pub_col], errors="coerce")
    if ave_py_col:
        out["ave_py_total"] = pd.to_numeric(out[ave_py_col], errors="coerce")
    else:
        out["ave_py_total"] = pd.NA
    if ave_cit_col:
        out["ave_citations_total"] = pd.to_numeric(out[ave_cit_col], errors="coerce")
    else:
        out["ave_citations_total"] = pd.NA

    # Merge optional names from cluster_names.
    for c in ["short_name", "name", "description"]:
        if c not in out.columns:
            out[c] = ""
    if not names.empty and "micro_cluster" in names.columns:
        n = names.copy()
        n["micro_cluster"] = pd.to_numeric(n["micro_cluster"], errors="coerce")
        n = n.dropna(subset=["micro_cluster"]).copy()
        n["micro_cluster"] = n["micro_cluster"].astype("int64")
        keep = ["micro_cluster"] + [c for c in ["short_name", "name", "description"] if c in n.columns]
        n = n[keep].drop_duplicates(subset=["micro_cluster"])
        out = out.merge(n, on="micro_cluster", how="left", suffixes=("", "_named"))
        for c in ["short_name", "name", "description"]:
            named_col = f"{c}_named"
            if named_col in out.columns:
                out[c] = out[named_col].where(out[named_col].notna(), out[c])
                out = out.drop(columns=[named_col])

    out["short_name"] = out["short_name"].map(sanitize_text)
    out["name"] = out["name"].map(sanitize_text)
    out["description"] = out["description"].map(sanitize_text)

    micro_ids = out["micro_cluster"].astype("int64").tolist()
    return out, micro_ids


def build_utokyo_micro_agg(*, database: str, staging: str, workgroup: str, micro_ids: list[int]) -> pd.DataFrame:
    sql = f"""
    WITH utokyo_papers AS (
        SELECT DISTINCT
            ar.id,
            ar.micro_cluster,
            ar.publication_year,
            ar.citations
        FROM article_report ar
        CROSS JOIN UNNEST(ar.institutions) AS t(institution)
        WHERE institution = '{UTOKYO_INSTITUTION}'
          AND ar.micro_cluster IN ({in_clause(micro_ids)})
    )
    SELECT
        micro_cluster,
        COUNT(*) AS publications_utokyo,
        ROUND(AVG(TRY_CAST(publication_year AS double)), 1) AS ave_py_utokyo,
        ROUND(AVG(citations), 2) AS ave_citations_utokyo
    FROM utokyo_papers
    GROUP BY micro_cluster
    """
    out = run_sql(sql, database=database, staging=staging, workgroup=workgroup)
    if out.empty:
        return pd.DataFrame(
            columns=[
                "micro_cluster",
                "publications_utokyo",
                "ave_py_utokyo",
                "ave_citations_utokyo",
            ]
        )

    out["micro_cluster"] = pd.to_numeric(out["micro_cluster"], errors="coerce").astype("Int64")
    out["publications_utokyo"] = pd.to_numeric(out["publications_utokyo"], errors="coerce")
    out["ave_py_utokyo"] = pd.to_numeric(out["ave_py_utokyo"], errors="coerce")
    out["ave_citations_utokyo"] = pd.to_numeric(out["ave_citations_utokyo"], errors="coerce")
    out = out.dropna(subset=["micro_cluster"]).copy()
    out["micro_cluster"] = out["micro_cluster"].astype("int64")
    return out


def build_utokyo_article_top10(*, database: str, staging: str, workgroup: str, micro_ids: list[int]) -> pd.DataFrame:
    sql = f"""
    WITH utokyo_papers AS (
        SELECT DISTINCT
            ar.id,
            ar.title,
            ar.publication_year,
            ar.citations,
            ar.micro_cluster,
            ar.meso_cluster,
            ar.macro_cluster
        FROM article_report ar
        CROSS JOIN UNNEST(ar.institutions) AS t(institution)
        WHERE institution = '{UTOKYO_INSTITUTION}'
          AND ar.micro_cluster IN ({in_clause(micro_ids)})
    )
    SELECT
        id,
        title,
        publication_year,
        citations,
        micro_cluster,
        meso_cluster,
        macro_cluster,
        '{UTOKYO_INSTITUTION}' AS institution
    FROM (
        SELECT
            id,
            title,
            publication_year,
            citations,
            micro_cluster,
            meso_cluster,
            macro_cluster,
            ROW_NUMBER() OVER (PARTITION BY micro_cluster ORDER BY citations DESC, id) AS rn
        FROM utokyo_papers
    ) ranked
    WHERE rn <= 10
    """
    out = run_sql(sql, database=database, staging=staging, workgroup=workgroup)
    if out.empty:
        return out
    for col in ["publication_year", "citations", "micro_cluster", "meso_cluster", "macro_cluster"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def build_cluster_summary(base: pd.DataFrame, utokyo_agg: pd.DataFrame) -> pd.DataFrame:
    summary = base.merge(utokyo_agg, on="micro_cluster", how="left")
    summary["publications_total"] = pd.to_numeric(summary["publications_total"], errors="coerce").fillna(0)
    summary["publications_utokyo"] = (
        pd.to_numeric(summary["publications_utokyo"], errors="coerce").fillna(0).astype("int64")
    )
    summary["ave_py_utokyo"] = pd.to_numeric(summary["ave_py_utokyo"], errors="coerce")
    summary["ave_citations_utokyo"] = pd.to_numeric(summary["ave_citations_utokyo"], errors="coerce")

    # Keep only clusters with at least one UTokyo publication.
    summary = summary[summary["publications_utokyo"] > 0].copy()

    summary["pct_utokyo"] = summary.apply(
        lambda r: (float(r["publications_utokyo"]) / float(r["publications_total"]))
        if pd.notna(r["publications_total"]) and float(r["publications_total"]) > 0
        else 0.0,
        axis=1,
    )
    summary["pct_utokyo"] = pd.to_numeric(summary["pct_utokyo"], errors="coerce").round(4)

    id_cols = [
        "display_id",
        "global_id",
        "micro_cluster",
        "short_name",
        "name",
        "description",
    ]
    parent_cols = [c for c in ["meso_cluster", "macro_cluster"] if c in summary.columns]
    metric_cols = [
        "publications_total",
        "publications_utokyo",
        "pct_utokyo",
        "ave_py_total",
        "ave_py_utokyo",
        "ave_citations_total",
        "ave_citations_utokyo",
    ]
    existing_id_cols = [c for c in id_cols if c in summary.columns]
    existing_metric_cols = [c for c in metric_cols if c in summary.columns]

    used = set(existing_id_cols + parent_cols + existing_metric_cols)
    remainder = [c for c in summary.columns if c not in used]
    ordered = existing_id_cols + parent_cols + existing_metric_cols + remainder

    summary = summary[ordered].sort_values(
        ["publications_utokyo", "display_id"],
        ascending=[False, True],
    ).reset_index(drop=True)
    return summary


def build_article_sheet(cluster_summary: pd.DataFrame, utokyo_articles: pd.DataFrame) -> pd.DataFrame:
    if utokyo_articles.empty:
        return utokyo_articles

    map_cols = ["micro_cluster", "display_id", "global_id"]
    key_map = cluster_summary[[c for c in map_cols if c in cluster_summary.columns]].copy()

    out = utokyo_articles.merge(key_map, on="micro_cluster", how="inner")
    out = out.sort_values(["display_id", "citations", "id"], ascending=[True, False, True]).reset_index(drop=True)

    first = [
        "display_id",
        "global_id",
        "micro_cluster",
        "id",
        "title",
        "publication_year",
        "citations",
        "meso_cluster",
        "macro_cluster",
        "institution",
    ]
    first_present = [c for c in first if c in out.columns]
    rest = [c for c in out.columns if c not in first_present]
    return out[first_present + rest]


def write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                safe_df = df.copy()
                object_cols = safe_df.select_dtypes(include=["object"]).columns
                for col in object_cols:
                    safe_df[col] = safe_df[col].map(sanitize_excel_text)
                safe_df.to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]
                ws.freeze_panes = "A2"
                ws.auto_filter.ref = ws.dimensions
    except ImportError as exc:
        raise RuntimeError(
            "openpyxl is required to write .xlsx files. Install openpyxl and rerun."
        ) from exc


def main() -> None:
    args = parse_args()
    paths = resolve_paths(
        snapshot=args.snapshot,
        query=args.query,
        subquery=args.subquery,
        query_folder=args.query_folder,
    )
    database = paths.database
    query_folder = paths.subquery

    subquery_base = paths.subquery_base
    root_dir = Path(__file__).resolve().parent.parent
    output_dir = root_dir / "utokyo" / database / query_folder
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / WORKBOOK_NAME

    print("[config] database:", database)
    print("[config] snapshot:", paths.snapshot)
    print("[config] query:", paths.query)
    print("[config] query_folder:", query_folder)
    print("[config] source:", subquery_base)
    print("[config] output:", out_path)
    print("[config] institution:", UTOKYO_INSTITUTION)
    print("[config] staging:", args.staging)
    print("[config] workgroup:", args.workgroup)

    micro_rep = read_subset(subquery_base, "cluster_report_micro", required=True)
    names = read_subset(subquery_base, "cluster_names", required=False)

    micro_base, micro_ids = build_micro_base(micro_rep, names)
    if not micro_ids:
        raise RuntimeError("No micro clusters found in subquery cluster_report_micro.")

    utokyo_agg = build_utokyo_micro_agg(
        database=database,
        staging=args.staging,
        workgroup=args.workgroup,
        micro_ids=micro_ids,
    )
    summary_df = build_cluster_summary(micro_base, utokyo_agg)

    utokyo_articles = build_utokyo_article_top10(
        database=database,
        staging=args.staging,
        workgroup=args.workgroup,
        micro_ids=micro_ids,
    )
    article_df = build_article_sheet(summary_df, utokyo_articles)

    write_excel(
        out_path,
        {
            "cluster_summary_utokyo": summary_df,
            "article_report_utokyo_top10": article_df,
        },
    )

    print("[done] summary rows:", len(summary_df))
    print("[done] article rows:", len(article_df))
    print("[file]", out_path)


if __name__ == "__main__":
    main()
