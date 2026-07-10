"""Generate per-level exports for a subquery result set.

Outputs are written locally to:
    excel/snapshot_{SNAPSHOT}_{QUERY}/{SUBQUERY}/{LEVEL}/

Where LEVEL is one of: macro, meso, micro.

Files created in each level folder:
1) article_report_top20.xlsx
    - Top-20 papers per cluster at that level, enriched with authors and publication source.
2) cluster_profile.xlsx
    - Two sheets only: info and the level profile sheet.
    - Excludes display_id from the exported level sheet.
3) countries_summary.csv
    - Top-100 countries per cluster (by frequency) at that level.
4) institutions_summary.csv
    - Top-100 institutions per cluster (by frequency) at that level.
"""

from __future__ import annotations

import argparse
import math
import re
import time
from pathlib import Path
from typing import Any

import awswrangler as wr
import httpx
import pandas as pd
import pycountry

ROOT = Path(__file__).resolve().parent
OPENALEX_WORKS_URL = "https://api.openalex.org/works"
ABSTRACTS_OPENALEX_API_FALLBACK = False
TOP_ENTITY_ROWS_PER_CLUSTER = 100

from common_config import (
    DEFAULT_STAGING,
    DEFAULT_WORKGROUP,
    resolve_paths,
)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate subquery exports for one subquery folder."
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
        "--abstracts-openalex-api-fallback",
        action="store_true",
        default=ABSTRACTS_OPENALEX_API_FALLBACK,
        help=(
            "If set, fetch abstracts from OpenAlex API when local abstract metadata is empty. "
            "Default is disabled."
        ),
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
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
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


def sanitize_excel_text(value: Any) -> str:
    raw = sanitize_text(value)
    return "".join(ch for ch in raw if ch in "\t\n\r" or ord(ch) >= 32)


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


def sort_by_cluster_code(df: pd.DataFrame, *, level: str, extra_sort: list[tuple[str, bool]]) -> pd.DataFrame:
    out = df.copy()

    if "cluster_code" not in out.columns:
        out["cluster_code"] = ""

    if level == "micro":
        cluster_parts = out["cluster_code"].astype("string").str.extract(r"^\s*([1-9][0-9]*)-([1-9][0-9]*)\s*$")
        out["_cluster_macro_sort"] = pd.to_numeric(cluster_parts[0], errors="coerce")
        out["_cluster_micro_sort"] = pd.to_numeric(cluster_parts[1], errors="coerce")
        sort_cols = ["_cluster_macro_sort", "_cluster_micro_sort"]
        sort_asc = [True, True]
        drop_cols = ["_cluster_macro_sort", "_cluster_micro_sort"]
    else:
        out["_cluster_sort"] = pd.to_numeric(out["cluster_code"], errors="coerce")
        sort_cols = ["_cluster_sort"]
        sort_asc = [True]
        drop_cols = ["_cluster_sort"]

    for col, asc in extra_sort:
        if col in out.columns:
            sort_cols.append(col)
            sort_asc.append(asc)

    out = out.sort_values(sort_cols, ascending=sort_asc, na_position="last").reset_index(drop=True)
    return out.drop(columns=drop_cols, errors="ignore")


def load_macro_name_map(path: str) -> dict[int, str]:
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


def ensure_cluster_code(df: pd.DataFrame) -> pd.DataFrame:
    required = {"micro_cluster", "macro_cluster", "publications"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"cluster_report_micro missing required columns for cluster_code: {sorted(missing)}")

    out = df.copy()
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
    rank_base["publications"] = rank_base["publications"].fillna(0.0)
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

    cluster_map = rank_base.sort_values(
        ["macro_cluster", "publications", "micro_cluster"],
        ascending=[True, False, True],
    ).copy()
    cluster_map["micro_rank"] = cluster_map.groupby("macro_cluster").cumcount() + 1
    cluster_map = cluster_map.merge(
        macro_rank[["macro_cluster", "macro_display_id"]],
        on="macro_cluster",
        how="left",
    )
    cluster_map["cluster_code"] = (
        cluster_map["macro_display_id"].astype("int64").astype(str)
        + "-"
        + cluster_map["micro_rank"].astype("int64").astype(str)
    )

    mapped = cluster_map[["micro_cluster", "macro_cluster", "cluster_code"]].copy()
    mapped["_micro_key"] = mapped["micro_cluster"].astype("int64")
    mapped["_macro_key"] = mapped["macro_cluster"].astype("int64")

    if "cluster_code" in out.columns:
        existing = out["cluster_code"].map(sanitize_text)
    else:
        existing = pd.Series(["" for _ in range(len(out))], index=out.index)
    valid_existing = existing.str.match(r"^[1-9][0-9]*-[1-9][0-9]*$", na=False)

    out["_micro_key"] = pd.to_numeric(out["micro_cluster"], errors="coerce")
    out["_macro_key"] = pd.to_numeric(out["macro_cluster"], errors="coerce")

    out = out.drop(columns=["cluster_code"], errors="ignore")
    out = out.merge(mapped[["_micro_key", "_macro_key", "cluster_code"]], on=["_micro_key", "_macro_key"], how="left")
    fallback = out["cluster_code"].map(sanitize_text)
    out["cluster_code"] = existing.where(valid_existing, fallback)

    out = out.drop(columns=["_micro_key", "_macro_key"], errors="ignore")
    return out


def build_article_topn(
    *,
    database: str,
    staging: str,
    workgroup: str,
    cluster_col: str,
    cluster_ids: list[int],
    top_n: int,
) -> pd.DataFrame:
    if cluster_col not in {"micro_cluster", "meso_cluster", "macro_cluster"}:
        raise ValueError(f"Unsupported cluster column: {cluster_col}")

    sql = f"""
    SELECT id, title, citations,
           micro_cluster, meso_cluster, macro_cluster, publication_year,
           {cluster_col} AS report_cluster
    FROM (
        SELECT id, title, citations,
               micro_cluster, meso_cluster, macro_cluster, publication_year,
               ROW_NUMBER() OVER (PARTITION BY {cluster_col}
                                  ORDER BY citations DESC, id) AS rn
        FROM article_report
        WHERE {cluster_col} IN ({in_clause(cluster_ids)})
    ) ranked
    WHERE rn <= {int(top_n)}
    """
    return run_sql(sql, database=database, staging=staging, workgroup=workgroup)


def format_authors(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, (list, tuple, set)):
        items = [sanitize_text(v) for v in value if sanitize_text(v)]
        return "; ".join(items)
    return sanitize_text(value)


def format_abstract(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, dict):
        # OpenAlex-style inverted index: {word: [pos1, pos2, ...]}.
        tokens: list[tuple[int, str]] = []
        for word, positions in value.items():
            word_text = sanitize_text(word)
            if not word_text:
                continue
            if not isinstance(positions, (list, tuple, set)):
                continue
            for pos in positions:
                try:
                    tokens.append((int(pos), word_text))
                except (TypeError, ValueError):
                    continue
        if not tokens:
            return ""
        tokens.sort(key=lambda x: x[0])
        return " ".join(word for _, word in tokens)
    if isinstance(value, (list, tuple, set)):
        items = [sanitize_text(v) for v in value if sanitize_text(v)]
        return " ".join(items)
    return sanitize_text(value)


def normalize_openalex_work_id(value: Any) -> str | None:
    raw = sanitize_text(value)
    if not raw:
        return None
    match = re.search(r"(W\d+)", raw, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def fetch_openalex_abstracts(
    article_ids: list[str],
    *,
    batch_size: int = 50,
    max_retries: int = 3,
) -> dict[str, str]:
    work_ids = [normalize_openalex_work_id(v) for v in article_ids]
    unique_ids = sorted({wid for wid in work_ids if wid})
    if not unique_ids:
        return {}

    out: dict[str, str] = {}
    total_batches = (len(unique_ids) + batch_size - 1) // batch_size

    with httpx.Client(timeout=30.0, headers={"User-Agent": "topic-model-exporter/1.0"}) as client:
        for batch_idx, start in enumerate(range(0, len(unique_ids), batch_size), start=1):
            batch = unique_ids[start : start + batch_size]
            filter_value = "openalex_id:" + "|".join(f"https://openalex.org/{wid}" for wid in batch)
            params = {
                "filter": filter_value,
                "per-page": str(len(batch)),
            }

            response: httpx.Response | None = None
            for attempt in range(max_retries):
                try:
                    response = client.get(OPENALEX_WORKS_URL, params=params)
                except httpx.HTTPError:
                    response = None

                if response is not None and response.status_code == 200:
                    break

                if attempt + 1 < max_retries:
                    time.sleep(1.5 * (attempt + 1))

            if response is None or response.status_code != 200:
                code = response.status_code if response is not None else "n/a"
                print(
                    f"[warn] OpenAlex abstract fetch failed for batch {batch_idx}/{total_batches} "
                    f"(status={code})"
                )
                continue

            payload = response.json()
            for record in payload.get("results", []):
                wid = normalize_openalex_work_id(record.get("id"))
                if not wid:
                    continue
                abstract = format_abstract(record.get("abstract_inverted_index"))
                if not abstract:
                    abstract = format_abstract(record.get("abstract"))
                if abstract:
                    out[wid] = abstract

            if batch_idx % 20 == 0 or batch_idx == total_batches:
                print(
                    f"[progress] OpenAlex abstract batches: {batch_idx}/{total_batches} "
                    f"(filled so far: {len(out)})"
                )

    return out


def load_nodes_metadata(nodes_query_path: str) -> pd.DataFrame:
    try:
        df = wr.s3.read_parquet(nodes_query_path)
    except Exception as exc:
        print(f"[warn] could not load nodes_query metadata at {nodes_query_path}: {exc}")
        return pd.DataFrame(columns=["article_id", "authors", "publication_source", "abstract"])

    if df.empty:
        return pd.DataFrame(columns=["article_id", "authors", "publication_source", "abstract"])

    id_col = pick_col(df, ["id", "article_id"], required=False)
    if not id_col:
        print("[warn] nodes_query metadata has no id/article_id column; authors/source enrichment skipped")
        return pd.DataFrame(columns=["article_id", "authors", "publication_source", "abstract"])

    out = pd.DataFrame({"article_id": df[id_col].map(sanitize_text)})
    if "authors" in df.columns:
        out["authors"] = df["authors"].map(format_authors)
    else:
        out["authors"] = ""

    if "publication_source" in df.columns:
        out["publication_source"] = df["publication_source"].map(sanitize_text)
    else:
        out["publication_source"] = ""

    abstract_col = pick_col(
        df,
        ["abstract", "abstract_text", "abstract_plaintext", "abstract_inverted_index"],
        required=False,
    )
    if abstract_col:
        out["abstract"] = df[abstract_col].map(format_abstract)
        non_empty_abstracts = (out["abstract"].fillna("").astype(str).str.strip() != "").sum()
        if non_empty_abstracts == 0:
            print(
                "[warn] nodes_query has an abstract-like column but all values are empty; "
                "abstract in article_report_top20 will be blank"
            )
    else:
        print(
            "[warn] nodes_query has no abstract field; abstract in article_report_top20 will be blank"
        )
        out["abstract"] = ""

    out = out[out["article_id"].astype(bool)].drop_duplicates(subset=["article_id"])
    return out


def build_article_table(
    article_topn: pd.DataFrame,
    *,
    level: str,
    level_sheet: pd.DataFrame,
    level_id_col: str,
    level_cluster_col: str,
    nodes_meta: pd.DataFrame,
    abstracts_openalex_api_fallback: bool = ABSTRACTS_OPENALEX_API_FALLBACK,
) -> pd.DataFrame:
    article_id_col = pick_col(article_topn, ["id", "article_id"], required=True)
    year_col = pick_col(article_topn, ["publication_year", "year"], required=True)
    citation_col = pick_col(article_topn, ["citations", "citation"], required=True)

    required = [
        article_id_col,
        "title",
        year_col,
        citation_col,
        "micro_cluster",
        "meso_cluster",
        "macro_cluster",
    ]
    missing = [c for c in required if c not in article_topn.columns]
    if missing:
        raise KeyError(f"article top-N rows missing columns: {missing}")

    out = article_topn[required].copy()
    out = out.rename(
        columns={
            article_id_col: "article_id",
            year_col: "year",
            citation_col: "citation",
        }
    )

    for col in ["year", "citation", "micro_cluster", "meso_cluster", "macro_cluster"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["article_id"] = out["article_id"].map(sanitize_text)

    if nodes_meta.empty:
        out["authors"] = ""
        out["publication_source"] = ""
        out["abstract"] = ""
    else:
        out = out.merge(nodes_meta, on="article_id", how="left")
        out["authors"] = out["authors"].map(format_authors)
        out["publication_source"] = out["publication_source"].map(sanitize_text)
        out["abstract"] = out["abstract"].map(format_abstract)

    abstract_non_empty = (out["abstract"].fillna("").astype(str).str.strip() != "").sum()
    if len(out) > 0 and abstract_non_empty == 0:
        if abstracts_openalex_api_fallback:
            print(
                "[info] abstract column is empty after local enrichment; "
                "fetching abstracts from OpenAlex API for top-20 report"
            )
            api_abstract_map = fetch_openalex_abstracts(out["article_id"].astype(str).tolist())
            if api_abstract_map:
                work_ids = out["article_id"].map(normalize_openalex_work_id)
                fetched = work_ids.map(lambda wid: api_abstract_map.get(wid or "", ""))
                out["abstract"] = out["abstract"].where(
                    out["abstract"].fillna("").astype(str).str.strip().astype(bool),
                    fetched,
                )
                filled = (out["abstract"].fillna("").astype(str).str.strip() != "").sum()
                print(f"[done] abstracts populated from OpenAlex API: {int(filled)} / {len(out)}")
            else:
                print("[warn] OpenAlex fallback returned no abstracts; keeping blank abstract column")
        else:
            print(
                "[info] abstract column is empty after local enrichment and "
                "OpenAlex API fallback is disabled"
            )

    context_cols = [c for c in [level_id_col, "cluster_code"] if c in level_sheet.columns]
    if context_cols:
        context = level_sheet[context_cols].copy()
        if level_id_col in context.columns:
            context = context.rename(columns={level_id_col: level_cluster_col})
        context[level_cluster_col] = pd.to_numeric(context[level_cluster_col], errors="coerce")
        context = context.dropna(subset=[level_cluster_col]).copy()
        context[level_cluster_col] = context[level_cluster_col].astype("int64")
        context = context.drop_duplicates(subset=[level_cluster_col])
        out = out.merge(context, on=level_cluster_col, how="left")

    out = sort_by_cluster_code(
        out,
        level=level,
        extra_sort=[("citation", False), ("article_id", True)],
    )

    first_cols = [
        "cluster_code",
        "article_id",
        "title",
        "abstract",
        "year",
        "citation",
        "authors",
        "publication_source",
    ]
    existing_first = [c for c in first_cols if c in out.columns]
    if level_cluster_col in out.columns and level_cluster_col not in existing_first:
        existing_first.append(level_cluster_col)
    out = out[existing_first]
    return out


def build_cluster_table(
    df: pd.DataFrame,
    *,
    level: str,
    id_col: str,
    pub_col: str,
    micro_names: pd.DataFrame,
    macro_name_map: dict[int, str],
    tie_break_col: str | None = None,
) -> pd.DataFrame:
    out = df.copy()
    out[id_col] = pd.to_numeric(out[id_col], errors="coerce")
    out[pub_col] = pd.to_numeric(out[pub_col], errors="coerce")
    out = out.dropna(subset=[id_col]).copy()
    out[id_col] = out[id_col].astype("int64")

    sort_cols = [pub_col]
    sort_orders = [False]
    if tie_break_col and tie_break_col in out.columns:
        out[tie_break_col] = pd.to_numeric(out[tie_break_col], errors="coerce")
        sort_cols.append(tie_break_col)
        sort_orders.append(False)
    sort_cols.append(id_col)
    sort_orders.append(True)

    out = out.sort_values(sort_cols, ascending=sort_orders).reset_index(drop=True)
    out["display_id"] = range(1, len(out) + 1)

    if "cluster_code" not in out.columns:
        out["cluster_code"] = ""

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

    if level in {"meso", "macro"}:
        out["cluster_code"] = out["display_id"].astype("int64").astype(str)

    if level == "micro":
        code_parts = out["cluster_code"].astype("string").str.extract(r"^\s*([1-9][0-9]*)-([1-9][0-9]*)\s*$")
        out["_cluster_macro_sort"] = pd.to_numeric(code_parts[0], errors="coerce")
        out["_cluster_micro_sort"] = pd.to_numeric(code_parts[1], errors="coerce")
        out = out.sort_values(
            ["_cluster_macro_sort", "_cluster_micro_sort", "display_id", id_col],
            ascending=[True, True, True, True],
            na_position="last",
        ).reset_index(drop=True)
        out = out.drop(columns=["_cluster_macro_sort", "_cluster_micro_sort"], errors="ignore")
    else:
        out["_cluster_sort"] = pd.to_numeric(out["cluster_code"], errors="coerce")
        out = out.sort_values(["_cluster_sort", "display_id", id_col], ascending=[True, True, True], na_position="last").reset_index(drop=True)
        out = out.drop(columns=["_cluster_sort"], errors="ignore")

    out["short_name"] = out["short_name"].map(sanitize_text)
    out["name"] = out["name"].map(sanitize_text)
    out["description"] = out["description"].map(sanitize_text)

    first_cols = ["display_id", "cluster_code", id_col, "short_name", "name", "description", pub_col]
    existing_first = [c for c in first_cols if c in out.columns]
    remaining = [c for c in out.columns if c not in existing_first]
    return out[existing_first + remaining]


def build_country_summary(
    *,
    database: str,
    staging: str,
    workgroup: str,
    cluster_col: str,
    cluster_ids: list[int],
    top_k: int = TOP_ENTITY_ROWS_PER_CLUSTER,
) -> pd.DataFrame:
    if cluster_col not in {"micro_cluster", "meso_cluster", "macro_cluster"}:
        raise ValueError(f"Unsupported cluster column: {cluster_col}")

    sql = f"""
    WITH agg AS (
        SELECT
            {cluster_col} AS report_cluster,
            country,
            COUNT(*) AS freq,
            ROUND(AVG(TRY_CAST(publication_year AS double)), 1) AS avg_publication_year,
            ROUND(AVG(citations), 2) AS avg_citation
        FROM article_report
        CROSS JOIN UNNEST(countries) AS t(country)
        WHERE {cluster_col} IN ({in_clause(cluster_ids)})
        GROUP BY {cluster_col}, country
    ), ranked AS (
        SELECT
            report_cluster,
            country,
            freq,
            avg_publication_year,
            avg_citation,
            ROW_NUMBER() OVER (
                PARTITION BY report_cluster
                ORDER BY freq DESC, country ASC
            ) AS rn
        FROM agg
    )
    SELECT
        report_cluster,
        country,
        freq,
        avg_publication_year,
        avg_citation
    FROM ranked
    WHERE rn <= {int(top_k)}
    """
    out = run_sql(sql, database=database, staging=staging, workgroup=workgroup)
    out["report_cluster"] = pd.to_numeric(out["report_cluster"], errors="coerce")
    out["freq"] = pd.to_numeric(out["freq"], errors="coerce")
    out["avg_publication_year"] = pd.to_numeric(out["avg_publication_year"], errors="coerce")
    out["avg_citation"] = pd.to_numeric(out["avg_citation"], errors="coerce")
    out["country"] = out["country"].map(iso2_to_country_name)
    out = out[out["country"].map(lambda x: bool(sanitize_text(x)))]
    return out


def build_institution_summary(
    *,
    database: str,
    staging: str,
    workgroup: str,
    cluster_col: str,
    cluster_ids: list[int],
    top_k: int = TOP_ENTITY_ROWS_PER_CLUSTER,
) -> pd.DataFrame:
    if cluster_col not in {"micro_cluster", "meso_cluster", "macro_cluster"}:
        raise ValueError(f"Unsupported cluster column: {cluster_col}")

    sql = f"""
    WITH agg AS (
        SELECT
            {cluster_col} AS report_cluster,
            institution,
            COUNT(*) AS freq,
            ROUND(AVG(TRY_CAST(publication_year AS double)), 1) AS avg_publication_year,
            ROUND(AVG(citations), 2) AS avg_citation
        FROM article_report
        CROSS JOIN UNNEST(institutions) AS t(institution)
        WHERE {cluster_col} IN ({in_clause(cluster_ids)})
        GROUP BY {cluster_col}, institution
    ), ranked AS (
        SELECT
            report_cluster,
            institution,
            freq,
            avg_publication_year,
            avg_citation,
            ROW_NUMBER() OVER (
                PARTITION BY report_cluster
                ORDER BY freq DESC, institution ASC
            ) AS rn
        FROM agg
    )
    SELECT
        report_cluster,
        institution,
        freq,
        avg_publication_year,
        avg_citation
    FROM ranked
    WHERE rn <= {int(top_k)}
    """
    out = run_sql(sql, database=database, staging=staging, workgroup=workgroup)
    out["report_cluster"] = pd.to_numeric(out["report_cluster"], errors="coerce")
    out["freq"] = pd.to_numeric(out["freq"], errors="coerce")
    out["avg_publication_year"] = pd.to_numeric(out["avg_publication_year"], errors="coerce")
    out["avg_citation"] = pd.to_numeric(out["avg_citation"], errors="coerce")
    out["institution"] = out["institution"].map(sanitize_text)
    out = out[out["institution"].astype(bool)]
    return out


def attach_cluster_code_and_sort(
    df: pd.DataFrame,
    *,
    level: str,
    value_col: str,
    cluster_code_map: dict[int, str],
    cluster_col: str = "report_cluster",
    cluster_code_col: str = "cluster_code",
) -> pd.DataFrame:
    out = df.copy()

    if cluster_col not in out.columns:
        raise KeyError(f"Summary table missing required column: {cluster_col}")

    out[cluster_code_col] = pd.to_numeric(out[cluster_col], errors="coerce").map(
        lambda x: cluster_code_map.get(int(x), "") if pd.notna(x) else ""
    )

    out = sort_by_cluster_code(
        out,
        level=level,
        extra_sort=[("freq", False), (value_col, True)],
    )

    out = out.drop(columns=[cluster_col], errors="ignore")

    first_cols = [cluster_code_col, value_col, "freq", "avg_publication_year", "avg_citation"]
    existing_first = [c for c in first_cols if c in out.columns]
    remaining = [c for c in out.columns if c not in existing_first]
    return out[existing_first + remaining]


def write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    # Excel worksheet row limit is 1,048,576 including header row.
    # Keep one row for headers and split oversized tables across part sheets.
    max_data_rows = 1_048_575

    def _iter_sheet_parts(sheet_name: str, df: pd.DataFrame):
        total_rows = len(df)
        if total_rows <= max_data_rows:
            yield sheet_name, df
            return

        part_idx = 1
        start = 0
        while start < total_rows:
            end = min(start + max_data_rows, total_rows)
            suffix = f"_p{part_idx:02d}"
            base_len = max(1, 31 - len(suffix))
            part_name = f"{sheet_name[:base_len]}{suffix}"
            yield part_name, df.iloc[start:end].copy()
            part_idx += 1
            start = end

    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                for part_sheet_name, part_df in _iter_sheet_parts(sheet_name, df):
                    safe_df = part_df.copy()
                    object_cols = safe_df.select_dtypes(include=["object", "string"]).columns
                    for col in object_cols:
                        safe_df[col] = safe_df[col].map(sanitize_excel_text)

                    safe_df.to_excel(writer, sheet_name=part_sheet_name, index=False)
                    ws = writer.sheets[part_sheet_name]
                    ws.freeze_panes = "A2"
                    ws.auto_filter.ref = ws.dimensions
    except ImportError as exc:
        raise RuntimeError(
            "openpyxl is required to write .xlsx files. Install openpyxl and rerun."
        ) from exc


def write_csv(path: Path, df: pd.DataFrame) -> None:
    safe_df = df.copy()
    object_cols = safe_df.select_dtypes(include=["object", "string"]).columns
    for col in object_cols:
        safe_df[col] = safe_df[col].map(sanitize_excel_text)
    safe_df.to_csv(path, index=False)


def build_level_info_sheet(
    *,
    snapshot: str,
    query: str,
    subquery: str,
    level: str,
    documents: int,
    clusters: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = [
        {"item": "data", "value": f"snapshot_{snapshot}"},
        {"item": "query", "value": query},
        {"item": "subquery", "value": subquery},
        {"item": "level", "value": level},
        {"item": "documents", "value": int(documents)},
        {"item": "clusters", "value": int(clusters)},
    ]
    return pd.DataFrame(rows, columns=["item", "value"])


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

    out_base = paths.subquery_base
    output_root = ROOT / "excel" / f"snapshot_{paths.snapshot}_{paths.query}" / query_folder
    output_root.mkdir(parents=True, exist_ok=True)

    print("[config] database:", database)
    print("[config] snapshot:", paths.snapshot)
    print("[config] query:", paths.query)
    print("[config] query_folder:", query_folder)
    print("[config] source:", out_base)
    print("[config] nodes_query source:", f"{paths.results_root}nodes_query/")
    print("[config] output_root:", output_root)
    print("[config] staging:", args.staging)
    print("[config] workgroup:", args.workgroup)
    print("[config] top countries/institutions per cluster:", TOP_ENTITY_ROWS_PER_CLUSTER)
    print("[config] abstracts_openalex_api_fallback:", args.abstracts_openalex_api_fallback)

    micro_rep = read_subset(out_base, "cluster_report_micro", required=True)
    meso_rep = read_subset(out_base, "cluster_report_meso", required=True)
    macro_rep = read_subset(out_base, "cluster_report_macro", required=True)
    micro_names = read_subset(out_base, "cluster_names", required=False)
    macro_name_map = load_macro_name_map(paths.macro_name_path)

    if "cluster_code" not in micro_rep.columns:
        micro_rep["cluster_code"] = ""

    micro_rep = ensure_cluster_code(micro_rep)

    micro_id_col = pick_col(micro_rep, ["micro_cluster", "cluster"], required=True)
    micro_pub_col = pick_col(micro_rep, ["publications"], required=True)
    micro_rank_col = pick_col(
        micro_rep,
        ["yearly_rank_citations", "ranked_citation", "ranked_citation_score"],
        required=False,
    )
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
        tie_break_col=micro_rank_col,
    )
    meso_sheet = build_cluster_table(
        meso_rep,
        level="meso",
        id_col=str(meso_id_col),
        pub_col=str(meso_pub_col),
        micro_names=pd.DataFrame(),
        macro_name_map=macro_name_map,
        tie_break_col=None,
    )
    macro_sheet = build_cluster_table(
        macro_rep,
        level="macro",
        id_col=str(macro_id_col),
        pub_col=str(macro_pub_col),
        micro_names=pd.DataFrame(),
        macro_name_map=macro_name_map,
        tie_break_col=None,
    )

    macro_global_series = pd.to_numeric(macro_sheet[macro_id_col], errors="coerce")
    macro_display_series = pd.to_numeric(macro_sheet["display_id"], errors="coerce")
    macro_display_map = {
        int(k): int(v)
        for k, v in zip(macro_global_series, macro_display_series)
        if pd.notna(k) and pd.notna(v)
    }

    micro_macro_col = pick_col(micro_sheet, ["macro_cluster"], required=False)
    if micro_macro_col:
        macro_ids_for_micro = pd.to_numeric(micro_sheet[micro_macro_col], errors="coerce")
        micro_sheet["macro_cluster_id"] = macro_ids_for_micro.map(
            lambda x: macro_display_map.get(int(x), pd.NA) if pd.notna(x) else pd.NA
        )
    else:
        micro_sheet["macro_cluster_id"] = pd.NA

    if "cluster_code" not in micro_sheet.columns:
        micro_sheet["cluster_code"] = ""

    micro_first_cols = ["display_id", "cluster_code", str(micro_id_col), "macro_cluster_id", "short_name", "name", "description", str(micro_pub_col)]
    micro_existing_first = [c for c in micro_first_cols if c in micro_sheet.columns]
    micro_remaining = [c for c in micro_sheet.columns if c not in micro_existing_first]
    micro_sheet = micro_sheet[micro_existing_first + micro_remaining]

    nodes_meta = load_nodes_metadata(f"{paths.results_root}nodes_query/")

    levels = [
        {
            "level": "macro",
            "sheet_name": "macro",
            "cluster_col": "macro_cluster",
            "sheet": macro_sheet,
            "id_col": str(macro_id_col),
            "pub_col": str(macro_pub_col),
        },
        {
            "level": "meso",
            "sheet_name": "meso",
            "cluster_col": "meso_cluster",
            "sheet": meso_sheet,
            "id_col": str(meso_id_col),
            "pub_col": str(meso_pub_col),
        },
        {
            "level": "micro",
            "sheet_name": "micro",
            "cluster_col": "micro_cluster",
            "sheet": micro_sheet,
            "id_col": str(micro_id_col),
            "pub_col": str(micro_pub_col),
        },
    ]

    for level_cfg in levels:
        level = level_cfg["level"]
        sheet_name = level_cfg["sheet_name"]
        cluster_col = level_cfg["cluster_col"]
        level_sheet = level_cfg["sheet"]
        id_col = level_cfg["id_col"]
        pub_col = level_cfg["pub_col"]

        cluster_ids = sorted(
            set(pd.to_numeric(level_sheet[id_col], errors="coerce").dropna().astype("int64").tolist())
        )
        if not cluster_ids:
            raise RuntimeError(f"No cluster IDs found in {sheet_name} report.")

        article_topn = build_article_topn(
            database=database,
            staging=args.staging,
            workgroup=args.workgroup,
            cluster_col=cluster_col,
            cluster_ids=cluster_ids,
            top_n=20,
        )
        article_df = build_article_table(
            article_topn,
            level=level,
            level_sheet=level_sheet,
            level_id_col=id_col,
            level_cluster_col=cluster_col,
            nodes_meta=nodes_meta,
            abstracts_openalex_api_fallback=args.abstracts_openalex_api_fallback,
        )

        countries_df = build_country_summary(
            database=database,
            staging=args.staging,
            workgroup=args.workgroup,
            cluster_col=cluster_col,
            cluster_ids=cluster_ids,
            top_k=TOP_ENTITY_ROWS_PER_CLUSTER,
        )
        institutions_df = build_institution_summary(
            database=database,
            staging=args.staging,
            workgroup=args.workgroup,
            cluster_col=cluster_col,
            cluster_ids=cluster_ids,
            top_k=TOP_ENTITY_ROWS_PER_CLUSTER,
        )

        key_series = pd.to_numeric(level_sheet[id_col], errors="coerce")
        code_series = level_sheet["cluster_code"].map(sanitize_text)
        cluster_code_map = {
            int(k): v
            for k, v in zip(key_series, code_series)
            if pd.notna(k) and bool(v)
        }

        countries_df = attach_cluster_code_and_sort(
            countries_df,
            level=level,
            value_col="country",
            cluster_code_map=cluster_code_map,
        )
        institutions_df = attach_cluster_code_and_sort(
            institutions_df,
            level=level,
            value_col="institution",
            cluster_code_map=cluster_code_map,
        )

        documents_count = int(pd.to_numeric(level_sheet[pub_col], errors="coerce").fillna(0).sum())
        level_clusters_count = int(len(level_sheet))
        profile_sheet = level_sheet.drop(columns=["display_id"], errors="ignore")
        info_sheet = build_level_info_sheet(
            snapshot=paths.snapshot,
            query=paths.query,
            subquery=query_folder,
            level=level,
            documents=documents_count,
            clusters=level_clusters_count,
        )

        level_dir = output_root / level
        level_dir.mkdir(parents=True, exist_ok=True)

        article_path = level_dir / "article_report_top20.xlsx"
        cluster_path = level_dir / "cluster_profile.xlsx"
        countries_path = level_dir / "countries_summary.csv"
        institutions_path = level_dir / "institutions_summary.csv"

        write_excel(article_path, {"article_report": article_df})
        write_excel(cluster_path, {"info": info_sheet, sheet_name: profile_sheet})
        write_csv(countries_path, countries_df)
        write_csv(institutions_path, institutions_df)

        print(
            f"[done] level={level} rows: article={len(article_df)} "
            f"clusters={len(profile_sheet)} countries={len(countries_df)} institutions={len(institutions_df)}"
        )
        print("[file]", article_path)
        print("[file]", cluster_path)
        print("[file]", countries_path)
        print("[file]", institutions_path)


if __name__ == "__main__":
    main()
