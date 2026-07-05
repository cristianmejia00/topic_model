"""Generate four Excel exports for a subquery result set.

Outputs are written locally to:
  excel/{database}/{query_folder}/

Files created:
1) article_report_top10.xlsx
   - Top-10 papers per micro cluster with article + hierarchy IDs.
2) cluster_profiles.xlsx
   - 3 sheets: micro, meso, macro cluster summaries with display/global IDs.
3) countries_summary.xlsx
   - Per-micro country frequencies (all rows from article_report; no top-N cap).
4) institutions_summary.xlsx
   - Per-micro institution frequencies (all rows from article_report; no top-N cap).
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
import sys
from typing import Any

import awswrangler as wr
import pandas as pd
import pycountry

ROOT = Path(__file__).resolve().parent
SUBQUERIES_DIR = ROOT / "subqueries"
if str(SUBQUERIES_DIR) not in sys.path:
    sys.path.insert(0, str(SUBQUERIES_DIR))

from common_config import (  # type: ignore
    DEFAULT_QUERY_FOLDER_TOPIC,
    macro_name_path,
    resolve_database,
    resolve_query_folder,
    subqueries_root,
)


DEFAULT_STAGING = "s3://openalex-outputs/athena-staging/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Excel exports for one subquery folder."
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Classification database id, e.g. q20260629.",
    )
    parser.add_argument(
        "--query-folder",
        default=None,
        help="Subquery folder name under subqueries/.",
    )
    parser.add_argument(
        "--staging",
        default=DEFAULT_STAGING,
        help="Athena query output S3 path.",
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


def iso2_to_country_name(value: Any) -> str:
    raw = sanitize_text(value)
    if len(raw) == 2 and raw.isalpha():
        hit = pycountry.countries.get(alpha_2=raw.upper())
        if hit is not None:
            raw = hit.name
    if raw == "Taiwan, Province of China":
        return "Taiwan"
    return raw


def run_sql(sql: str, *, database: str, staging: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql,
        database=database,
        s3_output=staging,
        ctas_approach=False,
    )


def in_clause(values: list[int]) -> str:
    return ", ".join(str(int(v)) for v in values)


def load_macro_name_map(database: str) -> dict[int, str]:
    path = macro_name_path(database)
    try:
        df = wr.s3.read_parquet(path)
    except Exception:
        return {}

    if not {"macro_cluster", "name"}.issubset(df.columns):
        return {}

    clean = df[["macro_cluster", "name"]].copy()
    clean["macro_cluster"] = pd.to_numeric(clean["macro_cluster"], errors="coerce")
    clean = clean.dropna(subset=["macro_cluster"])
    clean["macro_cluster"] = clean["macro_cluster"].astype("int64")
    clean["name"] = clean["name"].map(sanitize_text)
    return dict(zip(clean["macro_cluster"], clean["name"]))


def build_article_table(article_top10: pd.DataFrame) -> pd.DataFrame:
    article_id_col = pick_col(article_top10, ["id", "article_id"], required=True)
    year_col = pick_col(article_top10, ["publication_year", "year"], required=True)
    citation_col = pick_col(article_top10, ["citations", "citation"], required=True)

    required = [
        article_id_col,
        "title",
        year_col,
        citation_col,
        "micro_cluster",
        "meso_cluster",
        "macro_cluster",
    ]
    missing = [c for c in required if c not in article_top10.columns]
    if missing:
        raise KeyError(f"article_top10 missing columns: {missing}")

    out = article_top10[required].copy()
    out = out.rename(
        columns={
            article_id_col: "article_id",
            year_col: "year",
            citation_col: "citation",
        }
    )

    for col in ["year", "citation", "micro_cluster", "meso_cluster", "macro_cluster"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.sort_values(
        ["micro_cluster", "citation", "article_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    return out


def build_cluster_table(
    df: pd.DataFrame,
    *,
    level: str,
    id_col: str,
    pub_col: str,
    micro_names: pd.DataFrame,
    macro_name_map: dict[int, str],
) -> pd.DataFrame:
    out = df.copy()
    out[id_col] = pd.to_numeric(out[id_col], errors="coerce")
    out[pub_col] = pd.to_numeric(out[pub_col], errors="coerce")
    out = out.dropna(subset=[id_col]).copy()
    out[id_col] = out[id_col].astype("int64")

    out = out.sort_values([pub_col, id_col], ascending=[False, True]).reset_index(drop=True)
    out["display_id"] = range(1, len(out) + 1)
    out["global_id"] = out[id_col]

    if "short_name" not in out.columns:
        out["short_name"] = ""
    if "name" not in out.columns:
        out["name"] = ""
    if "description" not in out.columns:
        out["description"] = ""

    if level == "micro" and not micro_names.empty and "micro_cluster" in micro_names.columns:
        m = micro_names.copy()
        m["micro_cluster"] = pd.to_numeric(m["micro_cluster"], errors="coerce")
        m = m.dropna(subset=["micro_cluster"]).copy()
        m["micro_cluster"] = m["micro_cluster"].astype("int64")
        merge_cols = ["micro_cluster"]
        for c in ["short_name", "name", "description"]:
            if c in m.columns:
                merge_cols.append(c)
        m = m[merge_cols].drop_duplicates(subset=["micro_cluster"])
        out = out.merge(m, on="micro_cluster", how="left", suffixes=("", "_named"))
        for c in ["short_name", "name", "description"]:
            named_col = f"{c}_named"
            if named_col in out.columns:
                out[c] = out[named_col].where(out[named_col].notna(), out[c])
                out = out.drop(columns=[named_col])

    if level == "macro" and macro_name_map and "macro_cluster" in out.columns:
        macro_series = out["macro_cluster"].map(
            lambda x: macro_name_map.get(int(x), "") if pd.notna(x) else ""
        )
        out["name"] = macro_series.where(macro_series.astype(bool), out["name"])

    out["short_name"] = out["short_name"].map(sanitize_text)
    out["name"] = out["name"].map(sanitize_text)
    out["description"] = out["description"].map(sanitize_text)

    first_cols = ["display_id", "global_id", id_col, "short_name", "name", "description", pub_col]
    existing_first = [c for c in first_cols if c in out.columns]
    remaining = [c for c in out.columns if c not in existing_first]
    return out[existing_first + remaining]


def build_country_summary(*, database: str, staging: str, micro_ids: list[int]) -> pd.DataFrame:
    sql = f"""
    SELECT
        micro_cluster,
        country,
        COUNT(*) AS freq,
        ROUND(AVG(TRY_CAST(publication_year AS double)), 1) AS avg_publication_year,
        ROUND(AVG(citations), 2) AS avg_citation
    FROM article_report
    CROSS JOIN UNNEST(countries) AS t(country)
    WHERE micro_cluster IN ({in_clause(micro_ids)})
    GROUP BY micro_cluster, country
    """
    out = run_sql(sql, database=database, staging=staging)
    out["micro_cluster"] = pd.to_numeric(out["micro_cluster"], errors="coerce")
    out["freq"] = pd.to_numeric(out["freq"], errors="coerce")
    out["avg_publication_year"] = pd.to_numeric(out["avg_publication_year"], errors="coerce")
    out["avg_citation"] = pd.to_numeric(out["avg_citation"], errors="coerce")
    out["country"] = out["country"].map(iso2_to_country_name)
    out = out[out["country"].map(lambda x: bool(sanitize_text(x)))]
    return out.sort_values(["micro_cluster", "freq", "country"], ascending=[True, False, True]).reset_index(drop=True)


def build_institution_summary(*, database: str, staging: str, micro_ids: list[int]) -> pd.DataFrame:
    sql = f"""
    SELECT
        micro_cluster,
        institution,
        COUNT(*) AS freq,
        ROUND(AVG(TRY_CAST(publication_year AS double)), 1) AS avg_publication_year,
        ROUND(AVG(citations), 2) AS avg_citation
    FROM article_report
    CROSS JOIN UNNEST(institutions) AS t(institution)
    WHERE micro_cluster IN ({in_clause(micro_ids)})
    GROUP BY micro_cluster, institution
    """
    out = run_sql(sql, database=database, staging=staging)
    out["micro_cluster"] = pd.to_numeric(out["micro_cluster"], errors="coerce")
    out["freq"] = pd.to_numeric(out["freq"], errors="coerce")
    out["avg_publication_year"] = pd.to_numeric(out["avg_publication_year"], errors="coerce")
    out["avg_citation"] = pd.to_numeric(out["avg_citation"], errors="coerce")
    out["institution"] = out["institution"].map(sanitize_text)
    out = out[out["institution"].astype(bool)]
    return out.sort_values(["micro_cluster", "freq", "institution"], ascending=[True, False, True]).reset_index(drop=True)


def write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]
                ws.freeze_panes = "A2"
                ws.auto_filter.ref = ws.dimensions
    except ImportError as exc:
        raise RuntimeError(
            "openpyxl is required to write .xlsx files. Install openpyxl and rerun."
        ) from exc


def main() -> None:
    args = parse_args()
    database = resolve_database(args.database)
    query_folder = resolve_query_folder(args.query_folder, DEFAULT_QUERY_FOLDER_TOPIC)

    out_base = f"{subqueries_root(database)}{query_folder}/"
    output_dir = ROOT / "excel" / database / query_folder
    output_dir.mkdir(parents=True, exist_ok=True)

    print("[config] database:", database)
    print("[config] query_folder:", query_folder)
    print("[config] source:", out_base)
    print("[config] output_dir:", output_dir)

    article_top10 = read_subset(out_base, "article_top10", required=True)
    micro_rep = read_subset(out_base, "cluster_report_micro", required=True)
    meso_rep = read_subset(out_base, "cluster_report_meso", required=True)
    macro_rep = read_subset(out_base, "cluster_report_macro", required=True)
    micro_names = read_subset(out_base, "cluster_names", required=False)
    macro_name_map = load_macro_name_map(database)

    article_df = build_article_table(article_top10)

    micro_id_col = pick_col(micro_rep, ["micro_cluster", "cluster"], required=True)
    micro_pub_col = pick_col(micro_rep, ["publications"], required=True)
    meso_id_col = pick_col(meso_rep, ["meso_cluster", "cluster"], required=True)
    meso_pub_col = pick_col(meso_rep, ["publications"], required=True)
    macro_id_col = pick_col(macro_rep, ["macro_cluster", "cluster"], required=True)
    macro_pub_col = pick_col(macro_rep, ["publications"], required=True)

    micro_sheet = build_cluster_table(
        micro_rep,
        level="micro",
        id_col=str(micro_id_col),
        pub_col=str(micro_pub_col),
        micro_names=micro_names,
        macro_name_map=macro_name_map,
    )
    meso_sheet = build_cluster_table(
        meso_rep,
        level="meso",
        id_col=str(meso_id_col),
        pub_col=str(meso_pub_col),
        micro_names=pd.DataFrame(),
        macro_name_map=macro_name_map,
    )
    macro_sheet = build_cluster_table(
        macro_rep,
        level="macro",
        id_col=str(macro_id_col),
        pub_col=str(macro_pub_col),
        micro_names=pd.DataFrame(),
        macro_name_map=macro_name_map,
    )

    micro_ids = sorted(set(pd.to_numeric(micro_sheet[micro_id_col], errors="coerce").dropna().astype("int64").tolist()))
    if not micro_ids:
        raise RuntimeError("No micro cluster IDs found in cluster_report_micro.")

    countries_df = build_country_summary(database=database, staging=args.staging, micro_ids=micro_ids)
    institutions_df = build_institution_summary(database=database, staging=args.staging, micro_ids=micro_ids)

    article_path = output_dir / "article_report_top10.xlsx"
    clusters_path = output_dir / "cluster_profiles.xlsx"
    countries_path = output_dir / "countries_summary.xlsx"
    institutions_path = output_dir / "institutions_summary.xlsx"

    write_excel(article_path, {"article_report": article_df})
    write_excel(
        clusters_path,
        {
            "micro": micro_sheet,
            "meso": meso_sheet,
            "macro": macro_sheet,
        },
    )
    write_excel(countries_path, {"countries": countries_df})
    write_excel(institutions_path, {"institutions": institutions_df})

    print("[done] article rows:", len(article_df))
    print("[done] micro/meso/macro rows:", len(micro_sheet), len(meso_sheet), len(macro_sheet))
    print("[done] countries rows:", len(countries_df))
    print("[done] institutions rows:", len(institutions_df))
    print("[file]", article_path)
    print("[file]", clusters_path)
    print("[file]", countries_path)
    print("[file]", institutions_path)


if __name__ == "__main__":
    main()
