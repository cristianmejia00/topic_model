"""Build enriched embeddings for subquery-matched papers at macro level.

This script:
1) Collects all papers from query-level article_report that belong to micro clusters
   matched by a subquery.
2) Prepends macro cluster names to paper text before embedding.
    By default, uses only article_report abstracts.
    Optional fallback from query-level nodes_query can be enabled via CLI.
3) Applies proportional stratified sampling by macro cluster with guaranteed
   macro representation and a hard cap (default 100,000).
4) Assigns macro display IDs (1 = largest by matched-paper count).
5) Writes embeddings + metadata to:
   s3://.../subqueries/{SUBQUERY}/charts/enriched_embeds/

Inputs (query-level):
- article_report/
- cluster_color_macro/
- cluster_name_macro/
- cluster_report_macro/

Input (subquery-level):
- cluster_report_micro/

Outputs:
- enriched_embeds/embeddings.npy
- enriched_embeds/embeddings_ids.json
- enriched_embeds/sampled_records/ (parquet dataset)
- enriched_embeds/macro_display/ (parquet dataset)
- enriched_embeds/build_settings.json
"""

from __future__ import annotations

import argparse
import colorsys
import io
import json
import os
import sys
from dataclasses import dataclass
from typing import Iterable

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer


SNAPSHOT_ENV_VAR = "TOPIC_MODEL_SNAPSHOT"
QUERY_ENV_VAR = "TOPIC_MODEL_QUERY"
SUBQUERY_ENV_VAR = "TOPIC_MODEL_SUBQUERY"
LEGACY_SUBQUERY_ENV_VAR = "TOPIC_MODEL_QUERY_FOLDER"

DEFAULT_STAGING = "s3://openalex-outputs/athena-staging/"
DEFAULT_WORKGROUP = "primary"
DEFAULT_MODEL = "all-MiniLM-L6-v2"
DEFAULT_MAX_DOCS = 200_000
DEFAULT_SEED = 100
DEFAULT_MICRO_BATCH = 1000
DEFAULT_ABSTRACT_BATCH = 2000


@dataclass(frozen=True)
class ChartPaths:
    snapshot: str
    query: str
    subquery: str

    @property
    def database(self) -> str:
        return f"snapshot_{self.snapshot}-{self.query}"

    @property
    def results_root(self) -> str:
        return f"s3://openalex-results/snapshot_{self.snapshot}/queries/{self.query}/"

    @property
    def clustering_root(self) -> str:
        return f"{self.results_root}network/clustering/"

    @property
    def article_report_path(self) -> str:
        return f"{self.clustering_root}article_report/"

    @property
    def macro_color_path(self) -> str:
        return f"{self.clustering_root}cluster_color_macro/"

    @property
    def macro_name_path(self) -> str:
        return f"{self.clustering_root}cluster_name_macro/"

    @property
    def macro_report_path(self) -> str:
        return f"{self.clustering_root}cluster_report_macro/"

    @property
    def subquery_root(self) -> str:
        return f"{self.clustering_root}subqueries/{self.subquery}/"

    @property
    def subquery_micro_path(self) -> str:
        return f"{self.subquery_root}cluster_report_micro/"

    @property
    def charts_root(self) -> str:
        return f"{self.subquery_root}charts/"

    @property
    def enriched_root(self) -> str:
        return f"{self.charts_root}enriched_embeds/"


def _clean(value: str | None) -> str:
    return "" if value is None else str(value).strip()


def _require(value: str, hint: str) -> str:
    if value:
        return value
    raise RuntimeError(f"Missing required value. Provide {hint}.")


def resolve_snapshot(cli_value: str | None) -> str:
    return _require(_clean(cli_value) or _clean(os.getenv(SNAPSHOT_ENV_VAR)), "--snapshot or TOPIC_MODEL_SNAPSHOT")


def resolve_query(cli_value: str | None) -> str:
    return _require(_clean(cli_value) or _clean(os.getenv(QUERY_ENV_VAR)), "--query or TOPIC_MODEL_QUERY")


def resolve_subquery(cli_subquery: str | None, cli_query_folder: str | None) -> str:
    value = (
        _clean(cli_subquery)
        or _clean(cli_query_folder)
        or _clean(os.getenv(SUBQUERY_ENV_VAR))
        or _clean(os.getenv(LEGACY_SUBQUERY_ENV_VAR))
    )
    return _require(value, "--subquery or TOPIC_MODEL_SUBQUERY")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build enriched embeddings for subquery charts.")
    parser.add_argument("--snapshot", default=None, help="Snapshot token, e.g. 2026-06-26.")
    parser.add_argument("--query", default=None, help="Query token, e.g. q20260629.")
    parser.add_argument("--subquery", default=None, help="Subquery folder token.")
    parser.add_argument("--query-folder", default=None, help="Deprecated alias for --subquery.")
    parser.add_argument("--staging", default=DEFAULT_STAGING, help="Athena output staging path.")
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP, help="Athena workgroup.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="SentenceTransformer model name.")
    parser.add_argument("--max-docs", type=int, default=DEFAULT_MAX_DOCS, help="Maximum records to embed.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed for sampling.")
    parser.add_argument(
        "--micro-batch-size",
        type=int,
        default=DEFAULT_MICRO_BATCH,
        help="Number of micro IDs per Athena query chunk.",
    )
    parser.add_argument(
        "--abstract-batch-size",
        type=int,
        default=DEFAULT_ABSTRACT_BATCH,
        help="Number of paper IDs per nodes_query abstract fallback chunk.",
    )
    parser.add_argument(
        "--use-abstract-fallback",
        action="store_true",
        help="Enable fallback lookup of missing abstracts from nodes_query.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing enriched_embeds outputs.",
    )
    parser.add_argument(
        "--exclude-abstract-in-text",
        action="store_true",
        help="Build embedding text from macro name + title only (ignore abstract).",
    )
    return parser.parse_args()


def parse_s3_uri(path: str) -> tuple[str, str]:
    if not path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got: {path}")
    body = path[5:]
    parts = body.split("/", 1)
    bucket = parts[0]
    key = "" if len(parts) == 1 else parts[1]
    return bucket, key


def stable_color_hex(macro_id: int) -> str:
    hue = (int(macro_id) * 0.6180339887498949) % 1.0
    r, g, b = colorsys.hsv_to_rgb(hue, 0.62, 0.85)
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"


def run_sql(sql: str, *, database: str, staging: str, workgroup: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql,
        database=database,
        s3_output=staging,
        workgroup=workgroup,
        ctas_approach=False,
    )


def chunked(values: list[int], size: int) -> Iterable[list[int]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def chunked_strings(values: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def sql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_macro_lookup(paths: ChartPaths) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    names = wr.s3.read_parquet(paths.macro_name_path)
    colors = wr.s3.read_parquet(paths.macro_color_path)
    macro_rep = wr.s3.read_parquet(paths.macro_report_path)

    if "macro_cluster" not in names.columns or "name" not in names.columns:
        raise RuntimeError("cluster_name_macro must contain macro_cluster and name columns.")
    if "macro_cluster" not in colors.columns or "color_hex" not in colors.columns:
        raise RuntimeError("cluster_color_macro must contain macro_cluster and color_hex columns.")
    if "macro_cluster" not in macro_rep.columns:
        raise RuntimeError("cluster_report_macro must contain macro_cluster column.")

    names = names[["macro_cluster", "name"]].copy()
    colors = colors[["macro_cluster", "color_hex"]].copy()

    names["macro_cluster"] = pd.to_numeric(names["macro_cluster"], errors="coerce")
    colors["macro_cluster"] = pd.to_numeric(colors["macro_cluster"], errors="coerce")
    macro_rep["macro_cluster"] = pd.to_numeric(macro_rep["macro_cluster"], errors="coerce")

    names = names.dropna(subset=["macro_cluster"])
    colors = colors.dropna(subset=["macro_cluster"])
    macro_rep = macro_rep.dropna(subset=["macro_cluster"])

    names["macro_cluster"] = names["macro_cluster"].astype("int64")
    colors["macro_cluster"] = colors["macro_cluster"].astype("int64")
    macro_rep["macro_cluster"] = macro_rep["macro_cluster"].astype("int64")

    if "publications" in macro_rep.columns:
        macro_rep["publications"] = pd.to_numeric(macro_rep["publications"], errors="coerce").fillna(0).astype("int64")
    else:
        macro_rep["publications"] = 0

    return names.drop_duplicates("macro_cluster"), colors.drop_duplicates("macro_cluster"), macro_rep[["macro_cluster", "publications"]].drop_duplicates("macro_cluster")


def discover_article_columns(*, database: str, staging: str, workgroup: str) -> dict[str, str | None]:
    # Some Athena/engine combinations return an empty frame for SHOW COLUMNS
    # through awswrangler even when the table exists. Prefer information_schema.
    info_sql = (
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema='{database}' AND table_name='article_report' "
        "ORDER BY ordinal_position"
    )
    cols_df = run_sql(info_sql, database=database, staging=staging, workgroup=workgroup)

    if cols_df.empty:
        cols_df = run_sql(
            "SHOW COLUMNS IN article_report",
            database=database,
            staging=staging,
            workgroup=workgroup,
        )

    if cols_df.empty or len(cols_df.columns) == 0:
        raise RuntimeError("Could not inspect article_report columns.")

    if "column_name" in cols_df.columns:
        col_series = cols_df["column_name"]
    else:
        col_series = cols_df[cols_df.columns[0]]

    available = set(col_series.astype(str).str.strip().str.lower().tolist())

    def pick(*candidates: str) -> str | None:
        for cand in candidates:
            if cand in available:
                return cand
        return None

    id_col = pick("id", "article_id")
    title_col = pick("title", "ti")
    abstract_col = pick("abstract", "ab")
    micro_col = pick("micro_cluster")
    macro_col = pick("macro_cluster")

    if not id_col or not title_col or not micro_col or not macro_col:
        raise RuntimeError(
            "article_report is missing required columns among: id/article_id, title/ti, micro_cluster, macro_cluster"
        )

    return {
        "id": id_col,
        "title": title_col,
        "abstract": abstract_col,
        "micro": micro_col,
        "macro": macro_col,
    }


def load_matched_micro_ids(paths: ChartPaths) -> list[int]:
    micro = wr.s3.read_parquet(paths.subquery_micro_path)
    if "micro_cluster" not in micro.columns:
        raise RuntimeError("subquery cluster_report_micro is missing micro_cluster column.")
    ids = (
        pd.to_numeric(micro["micro_cluster"], errors="coerce")
        .dropna()
        .astype("int64")
        .drop_duplicates()
        .tolist()
    )
    if not ids:
        raise RuntimeError("No micro_cluster IDs found in subquery cluster_report_micro.")
    return sorted(ids)


def fetch_candidate_docs(
    *,
    database: str,
    staging: str,
    workgroup: str,
    micro_ids: list[int],
    column_map: dict[str, str | None],
    batch_size: int,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []

    abstract_sql = f", {column_map['abstract']} AS abstract" if column_map["abstract"] else ""

    for i, micro_chunk in enumerate(chunked(micro_ids, batch_size), start=1):
        in_clause = ", ".join(str(int(x)) for x in micro_chunk)
        sql = f"""
        SELECT
            CAST({column_map['id']} AS varchar) AS paper_id,
            {column_map['title']} AS title
            {abstract_sql},
            CAST({column_map['micro']} AS bigint) AS micro_cluster,
            CAST({column_map['macro']} AS bigint) AS macro_cluster
        FROM article_report
        WHERE {column_map['micro']} IN ({in_clause})
        """
        df = run_sql(sql, database=database, staging=staging, workgroup=workgroup)
        parts.append(df)
        print(f"[load] article_report chunk {i}: {len(df):,} rows")

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if out.empty:
        raise RuntimeError("No candidate papers found in article_report for matched micro clusters.")

    out["paper_id"] = out["paper_id"].astype(str)
    out = out.drop_duplicates(subset=["paper_id"]).reset_index(drop=True)
    out["micro_cluster"] = pd.to_numeric(out["micro_cluster"], errors="coerce")
    out["macro_cluster"] = pd.to_numeric(out["macro_cluster"], errors="coerce")
    out = out.dropna(subset=["micro_cluster", "macro_cluster"]).copy()
    out["micro_cluster"] = out["micro_cluster"].astype("int64")
    out["macro_cluster"] = out["macro_cluster"].astype("int64")
    return out


def discover_table_id_abstract_columns(
    *,
    database: str,
    staging: str,
    workgroup: str,
    table_name: str,
) -> tuple[str | None, str | None]:
    info_sql = (
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema='{database}' AND table_name='{table_name}' "
        "ORDER BY ordinal_position"
    )
    try:
        cols_df = run_sql(info_sql, database=database, staging=staging, workgroup=workgroup)
    except Exception:
        cols_df = pd.DataFrame()

    if cols_df.empty:
        try:
            cols_df = run_sql(
                f"SHOW COLUMNS IN {table_name}",
                database=database,
                staging=staging,
                workgroup=workgroup,
            )
        except Exception:
            cols_df = pd.DataFrame()

    if cols_df.empty or len(cols_df.columns) == 0:
        return None, None

    if "column_name" in cols_df.columns:
        col_series = cols_df["column_name"]
    else:
        col_series = cols_df[cols_df.columns[0]]

    available = set(col_series.astype(str).str.strip().str.lower().tolist())

    def pick(*candidates: str) -> str | None:
        for cand in candidates:
            if cand in available:
                return cand
        return None

    id_col = pick("id", "article_id")
    abstract_col = pick("abstract", "abstract_text", "abstract_plaintext", "abstract_inverted_index")
    return id_col, abstract_col


def fetch_abstract_map_from_table(
    *,
    table_name: str,
    id_col: str,
    abstract_col: str,
    paper_ids: list[str],
    database: str,
    staging: str,
    workgroup: str,
    batch_size: int,
) -> dict[str, str]:
    mapped: dict[str, str] = {}
    for idx, id_chunk in enumerate(chunked_strings(paper_ids, batch_size), start=1):
        in_clause = ", ".join(sql_quote(pid) for pid in id_chunk)
        sql = f"""
        SELECT
            CAST({id_col} AS varchar) AS paper_id,
            CAST({abstract_col} AS varchar) AS abstract
        FROM {table_name}
        WHERE {id_col} IN ({in_clause})
          AND {abstract_col} IS NOT NULL
          AND TRIM(CAST({abstract_col} AS varchar)) <> ''
        """
        fetched = run_sql(sql, database=database, staging=staging, workgroup=workgroup)
        if not fetched.empty:
            fetched["paper_id"] = fetched["paper_id"].fillna("").astype(str).str.strip()
            fetched["abstract"] = fetched["abstract"].fillna("").astype(str).str.strip()
            fetched = fetched[(fetched["paper_id"] != "") & (fetched["abstract"] != "")]
            for row in fetched.itertuples(index=False):
                if row.paper_id not in mapped:
                    mapped[row.paper_id] = row.abstract
        if idx % 10 == 0 or idx == 1:
            print(f"[load] {table_name} fallback chunk {idx}: mapped={len(mapped):,}")
    return mapped


def backfill_abstracts_from_nodes_query(
    docs: pd.DataFrame,
    *,
    database: str,
    staging: str,
    workgroup: str,
    batch_size: int,
) -> tuple[pd.DataFrame, int, int, bool]:
    if "paper_id" not in docs.columns:
        return docs, 0, 0, False

    if "abstract" not in docs.columns:
        docs = docs.copy()
        docs["abstract"] = ""

    docs = docs.copy()
    docs["abstract"] = docs["abstract"].fillna("").astype(str).str.strip()
    missing_mask = docs["abstract"] == ""
    missing_ids = docs.loc[missing_mask, "paper_id"].dropna().astype(str).drop_duplicates().tolist()
    if not missing_ids:
        return docs, 0, 0, False

    fallback_map: dict[str, str] = {}

    id_col, abstract_col = discover_table_id_abstract_columns(
        database=database,
        staging=staging,
        workgroup=workgroup,
        table_name="nodes_query",
    )
    if not id_col or not abstract_col:
        print(f"[warn] {database}.nodes_query does not expose an id+abstract column pair for fallback")
        return docs, len(missing_ids), 0, True

    print(
        f"[load] abstract fallback from {database}.nodes_query using id={id_col} "
        f"abstract={abstract_col} for {len(missing_ids):,} papers"
    )

    found = fetch_abstract_map_from_table(
        table_name="nodes_query",
        id_col=id_col,
        abstract_col=abstract_col,
        paper_ids=missing_ids,
        database=database,
        staging=staging,
        workgroup=workgroup,
        batch_size=batch_size,
    )
    fallback_map.update(found)

    if not fallback_map:
        return docs, len(missing_ids), 0, True

    fill_mask = docs["abstract"] == ""
    mapped = docs.loc[fill_mask, "paper_id"].astype(str).map(fallback_map)
    to_fill = mapped.notna() & mapped.astype(str).str.strip().ne("")
    filled_count = int(to_fill.sum())
    if filled_count > 0:
        docs.loc[mapped.index[to_fill], "abstract"] = mapped[to_fill].astype(str).str.strip()

    return docs, len(missing_ids), filled_count, True


def safe_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    return str(value).strip()


def proportional_sample_by_macro(df: pd.DataFrame, *, max_docs: int, seed: int) -> pd.DataFrame:
    total = len(df)
    if total <= max_docs:
        print(f"[sample] no downsampling needed: {total:,} <= {max_docs:,}")
        return df.copy().reset_index(drop=True)

    sizes = (
        df.groupby("macro_cluster", as_index=False)
        .size()
        .rename(columns={"size": "n_docs"})
        .sort_values("macro_cluster")
        .reset_index(drop=True)
    )

    n_groups = len(sizes)
    if n_groups > max_docs:
        raise RuntimeError(
            f"Cannot guarantee macro representation: macros={n_groups:,} exceeds max_docs={max_docs:,}."
        )

    alloc = np.ones(n_groups, dtype=np.int64)
    remaining_budget = max_docs - n_groups
    available_extra = sizes["n_docs"].to_numpy(dtype=np.int64) - 1

    if remaining_budget > 0 and available_extra.sum() > 0:
        ideal_extra = (remaining_budget * available_extra) / float(available_extra.sum())
        floor_extra = np.floor(ideal_extra).astype(np.int64)
        floor_extra = np.minimum(floor_extra, available_extra)
        alloc += floor_extra

        remaining_budget -= int(floor_extra.sum())
        capacity = available_extra - floor_extra
        remainders = ideal_extra - floor_extra

        while remaining_budget > 0:
            candidates = np.where(capacity > 0)[0]
            if len(candidates) == 0:
                break

            order = sorted(
                candidates.tolist(),
                key=lambda idx: (
                    -remainders[idx],
                    -int(sizes.loc[idx, "n_docs"]),
                    int(sizes.loc[idx, "macro_cluster"]),
                ),
            )
            progressed = False
            for idx in order:
                if remaining_budget == 0:
                    break
                if capacity[idx] <= 0:
                    continue
                alloc[idx] += 1
                capacity[idx] -= 1
                remaining_budget -= 1
                progressed = True
            if not progressed:
                break

    sampled_parts: list[pd.DataFrame] = []
    for row_idx, row in sizes.iterrows():
        macro_id = int(row["macro_cluster"])
        n_take = int(min(alloc[row_idx], row["n_docs"]))
        block = df[df["macro_cluster"] == macro_id]
        if len(block) == n_take:
            sampled_parts.append(block)
        else:
            sampled_parts.append(block.sample(n=n_take, random_state=seed + macro_id))

    sampled = pd.concat(sampled_parts, ignore_index=True)

    if len(sampled) < max_docs and len(sampled) < len(df):
        needed = min(max_docs - len(sampled), len(df) - len(sampled))
        taken = set(sampled["paper_id"].astype(str).tolist())
        remaining_pool = df[~df["paper_id"].astype(str).isin(taken)]
        if needed > 0 and not remaining_pool.empty:
            sampled = pd.concat(
                [sampled, remaining_pool.sample(n=needed, random_state=seed + 1_000_003)],
                ignore_index=True,
            )

    sampled = sampled.drop_duplicates(subset=["paper_id"]).reset_index(drop=True)
    if len(sampled) > max_docs:
        sampled = sampled.sample(n=max_docs, random_state=seed).reset_index(drop=True)

    coverage_before = int(df["macro_cluster"].nunique())
    coverage_after = int(sampled["macro_cluster"].nunique())
    print(f"[sample] downsampled {total:,} -> {len(sampled):,} (macro coverage {coverage_after}/{coverage_before})")
    return sampled


def put_json_s3(path: str, payload: object) -> None:
    bucket, key = parse_s3_uri(path)
    body = json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def put_npy_s3(path: str, arr: np.ndarray) -> None:
    bucket, key = parse_s3_uri(path)
    buffer = io.BytesIO()
    np.save(buffer, arr)
    buffer.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(), ContentType="application/octet-stream")


def main() -> None:
    args = parse_args()

    snapshot = resolve_snapshot(args.snapshot)
    query = resolve_query(args.query)
    subquery = resolve_subquery(args.subquery, args.query_folder)
    paths = ChartPaths(snapshot=snapshot, query=query, subquery=subquery)

    if args.max_docs <= 0:
        raise RuntimeError("--max-docs must be positive.")
    if args.micro_batch_size <= 0:
        raise RuntimeError("--micro-batch-size must be positive.")
    if args.use_abstract_fallback and args.abstract_batch_size <= 0:
        raise RuntimeError("--abstract-batch-size must be positive.")

    print("[config] snapshot:", snapshot)
    print("[config] query:", query)
    print("[config] subquery:", subquery)
    print("[config] database:", paths.database)
    print("[config] use_abstract_fallback:", args.use_abstract_fallback)
    print("[config] staging:", args.staging)
    print("[config] workgroup:", args.workgroup)
    print("[config] model:", args.model)
    print("[config] max_docs:", args.max_docs)
    print("[config] seed:", args.seed)
    print("[config] exclude_abstract_in_text:", args.exclude_abstract_in_text)
    if args.use_abstract_fallback:
        print("[config] abstract_batch_size:", args.abstract_batch_size)
    print("[config] output:", paths.enriched_root)

    emb_npy_path = f"{paths.enriched_root}embeddings.npy"
    if wr.s3.does_object_exist(emb_npy_path):
        if not args.force:
            print(f"[stop] found existing embeddings at {emb_npy_path}. Use --force to overwrite.")
            return
        print(f"[cleanup] --force enabled, deleting existing prefix: {paths.enriched_root}")
        wr.s3.delete_objects(paths.enriched_root)

    matched_micro = load_matched_micro_ids(paths)
    print(f"[load] matched micro clusters: {len(matched_micro):,}")

    column_map = discover_article_columns(
        database=paths.database,
        staging=args.staging,
        workgroup=args.workgroup,
    )
    print("[schema] article_report columns:", column_map)

    docs = fetch_candidate_docs(
        database=paths.database,
        staging=args.staging,
        workgroup=args.workgroup,
        micro_ids=matched_micro,
        column_map=column_map,
        batch_size=args.micro_batch_size,
    )
    print(f"[load] candidate papers: {len(docs):,}")

    article_report_has_abstract_col = bool(column_map.get("abstract"))
    if "abstract" not in docs.columns:
        docs["abstract"] = ""
    docs["abstract"] = docs["abstract"].fillna("").astype(str).str.strip()
    abstract_non_empty_before = int(docs["abstract"].ne("").sum())
    abstract_missing_before = int(docs["abstract"].eq("").sum())

    if args.use_abstract_fallback:
        docs, fallback_asked, abstract_filled_from_nodes_query, nodes_query_fallback_attempted = backfill_abstracts_from_nodes_query(
            docs,
            database=paths.database,
            staging=args.staging,
            workgroup=args.workgroup,
            batch_size=args.abstract_batch_size,
        )
        docs["abstract"] = docs["abstract"].fillna("").astype(str).str.strip()
        abstract_non_empty_after = int(docs["abstract"].ne("").sum())
        print(
            "[load] abstract coverage before/after fallback: "
            f"{abstract_non_empty_before:,}/{len(docs):,} -> {abstract_non_empty_after:,}/{len(docs):,}"
        )
    else:
        fallback_asked = 0
        abstract_filled_from_nodes_query = 0
        nodes_query_fallback_attempted = False
        abstract_non_empty_after = abstract_non_empty_before
        print(
            "[load] abstract fallback disabled; using article_report abstracts only: "
            f"{abstract_non_empty_after:,}/{len(docs):,} non-empty"
        )

    names, colors, macro_rep = load_macro_lookup(paths)

    docs = docs.merge(names, on="macro_cluster", how="left")
    docs = docs.merge(colors, on="macro_cluster", how="left")
    docs = docs.merge(macro_rep, on="macro_cluster", how="left", suffixes=("", "_global"))
    docs["name"] = docs["name"].map(safe_text)
    docs["name"] = np.where(docs["name"].astype(bool), docs["name"], docs["macro_cluster"].map(lambda x: f"Macro {int(x)}"))
    docs["color_hex"] = docs["color_hex"].map(safe_text)
    docs["color_hex"] = np.where(docs["color_hex"].astype(bool), docs["color_hex"], docs["macro_cluster"].map(lambda x: stable_color_hex(int(x))))
    docs["publications"] = pd.to_numeric(docs["publications"], errors="coerce").fillna(0).astype("int64")

    macro_counts = (
        docs.groupby("macro_cluster", as_index=False)
        .size()
        .rename(columns={"size": "subquery_documents"})
        .sort_values(["subquery_documents", "macro_cluster"], ascending=[False, True])
        .reset_index(drop=True)
    )
    macro_counts["display_id"] = np.arange(1, len(macro_counts) + 1, dtype=np.int64)

    macro_meta = (
        macro_counts
        .merge(docs[["macro_cluster", "name", "color_hex", "publications"]].drop_duplicates("macro_cluster"), on="macro_cluster", how="left")
        .rename(columns={"name": "macro_name", "publications": "macro_publications_global"})
    )

    docs = docs.merge(macro_meta[["macro_cluster", "display_id", "macro_name"]], on="macro_cluster", how="left")

    title_col = "title"
    abstract_col = "abstract"

    def build_text(row: pd.Series) -> str:
        macro_name = safe_text(row.get("macro_name", ""))
        title = safe_text(row.get(title_col, ""))
        abstract = "" if args.exclude_abstract_in_text else safe_text(row.get(abstract_col, ""))
        body = " ".join(part for part in [title, abstract] if part).strip()
        if macro_name and body:
            return f"{macro_name}. {body}"
        if body:
            return body
        return macro_name

    docs["text_enriched"] = docs.apply(build_text, axis=1)
    docs = docs[docs["text_enriched"].map(bool)].copy()
    if docs.empty:
        raise RuntimeError("No non-empty enriched text rows available for embedding.")

    pre_sample_docs = int(len(docs))
    pre_sample_macro_clusters = int(docs["macro_cluster"].nunique())
    pre_sample_micro_clusters = int(docs["micro_cluster"].nunique())

    sampled = proportional_sample_by_macro(docs, max_docs=args.max_docs, seed=args.seed)
    sampled = sampled.sort_values(["display_id", "macro_cluster", "paper_id"]).reset_index(drop=True)

    print(f"[embed] loading model: {args.model}")
    model = SentenceTransformer(args.model)
    vectors = model.encode(sampled["text_enriched"].tolist(), show_progress_bar=True)
    embeddings = np.asarray(vectors)

    ids = sampled["paper_id"].astype(str).tolist()
    if embeddings.shape[0] != len(ids):
        raise RuntimeError("Embeddings row count does not match sampled IDs.")

    put_npy_s3(emb_npy_path, embeddings)
    put_json_s3(f"{paths.enriched_root}embeddings_ids.json", ids)

    wr.s3.to_parquet(
        sampled[
            [
                "paper_id",
                "micro_cluster",
                "macro_cluster",
                "display_id",
                "macro_name",
                "color_hex",
                "title",
                "text_enriched",
            ]
            + ["abstract"]
        ],
        path=f"{paths.enriched_root}sampled_records/",
        dataset=True,
        mode="overwrite",
    )

    wr.s3.to_parquet(
        macro_meta.sort_values("display_id"),
        path=f"{paths.enriched_root}macro_display/",
        dataset=True,
        mode="overwrite",
    )

    settings = {
        "snapshot": snapshot,
        "query": query,
        "subquery": subquery,
        "database": paths.database,
        "article_report_path": paths.article_report_path,
        "subquery_micro_path": paths.subquery_micro_path,
        "macro_name_path": paths.macro_name_path,
        "macro_color_path": paths.macro_color_path,
        "macro_report_path": paths.macro_report_path,
        "model": args.model,
        "max_docs": int(args.max_docs),
        "seed": int(args.seed),
        "candidate_docs": pre_sample_docs,
        "pre_sample_docs": pre_sample_docs,
        "candidate_macro_clusters": pre_sample_macro_clusters,
        "candidate_micro_clusters": pre_sample_micro_clusters,
        "matched_micro_clusters": int(len(matched_micro)),
        "sampled_docs": int(len(sampled)),
        "macro_count_sampled": int(sampled["macro_cluster"].nunique()),
        "article_report_has_abstract_column": article_report_has_abstract_col,
        "abstract_non_empty_before_fallback": abstract_non_empty_before,
        "abstract_missing_before_fallback": abstract_missing_before,
        "use_abstract_fallback": bool(args.use_abstract_fallback),
        "nodes_query_fallback_attempted": nodes_query_fallback_attempted,
        "nodes_query_fallback_requested_ids": int(fallback_asked),
        "abstract_filled_from_nodes_query": int(abstract_filled_from_nodes_query),
        "abstract_non_empty_after_fallback": abstract_non_empty_after,
        "abstract_column_used": bool(abstract_non_empty_after > 0),
        "exclude_abstract_in_text": bool(args.exclude_abstract_in_text),
    }
    put_json_s3(f"{paths.enriched_root}build_settings.json", settings)

    print(f"[done] embeddings: {embeddings.shape}")
    print(f"[done] wrote: {emb_npy_path}")
    print(f"[done] wrote: {paths.enriched_root}embeddings_ids.json")
    print(f"[done] wrote: {paths.enriched_root}sampled_records/")
    print(f"[done] wrote: {paths.enriched_root}macro_display/")
    print(f"[done] wrote: {paths.enriched_root}build_settings.json")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
