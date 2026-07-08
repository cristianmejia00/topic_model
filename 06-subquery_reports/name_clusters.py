"""
name_clusters.py
================
Assign a natural-language name, a short_name (<=5 words), and a description to the
micro clusters of one subquery, using the OpenAI API with Structured Outputs.

Inputs per cluster (from the subquery + bertopic outputs):
    * the c-TF-IDF keywords (bertopic/micro/)
    * the top-10 cited paper titles (subqueries/{SUBQUERY}/article_top10/)

Two execution modes (same schema, same prompts, same output):
    USE_BATCH = False -> concurrent synchronous calls (best for a query's ~10s-100s
                         of clusters; fast, immediate)
    USE_BATCH = True  -> OpenAI Batch API: one JSONL, 50% cheaper, <=24h turnaround
                         (use when naming very large sets, e.g. all 191k clusters)

Output: subqueries/{SUBQUERY}/cluster_names/  (micro_cluster, name, short_name,
description, model)

Requires: openai, numpy, pandas, awswrangler, pyarrow
The API key is read from a local .key file (single line).
"""

from __future__ import annotations
import argparse
import os
import io
import json
import time
from pathlib import Path
import concurrent.futures as cf
import pandas as pd

from common_config import resolve_paths

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
DATABASE = ""
SUBQUERY = ""

SUBQUERIES_ROOT = ""
KEYWORDS_DIR = ""   # micro keywords
ROOT_DIR        = Path(__file__).resolve().parent.parent
KEY_FILE        = ROOT_DIR / ".key"

# Model names change often -- verify against platform.openai.com/docs/models.
# A small model is plenty for labeling given rich context; bump to a flagship
# (e.g. a gpt-5.x full model) if names/descriptions need more polish.
MODEL       = "gpt-4o-mini"
USE_BATCH   = False
MAX_WORKERS = 8          # concurrency for the synchronous path
MAX_CLUSTERS = None      # cap for a quick test run (None = all)

OUT_BASE = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Name micro clusters for a selected subquery output folder."
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
        help="Subquery folder name to read/write under clustering/subqueries/.",
    )
    parser.add_argument(
        "--query-folder",
        default=None,
        help="Deprecated alias for --subquery.",
    )
    return parser.parse_args()


# ----------------------------------------------------------------------------
# prompt + schema  (shared by both modes)
# ----------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a scientific taxonomist labeling ONE micro cluster inside a hierarchical "
    "map of ALL of science that contains more than 100,000 sibling micro clusters. "
    "Because there are so many clusters, a broad label will collide with dozens of "
    "others and is WRONG. For example, dozens of distinct clusters are about quantum "
    "computing, so 'Quantum Computing' is far too generic -- a correct label pins down "
    "the specific method, system, subfield, or application (e.g. 'Surface-Code Error "
    "Correction for Superconducting Qubits'). "
    "You are given the cluster's distinctive keywords and its most-cited paper titles; "
    "many of these papers are likely in your training data, so use what you know about "
    "them. Base the label strictly on this evidence and capture the dominant shared theme.\n"
    "Return three fields:\n"
    "- name: a specific, descriptive topic label in Title Case, ~4-12 words, no trailing "
    "punctuation, not a copy of any single paper title, specific enough to distinguish this "
    "cluster from other clusters on the same broad topic.\n"
    "- short_name: at most 5 words, easy for a human to remember, derived from the name.\n"
    "- description: 2-5 sentences, specific and detailed, explaining what this body of work "
    "studies (methods, systems, questions, applications) grounded in the given papers."
)

RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "cluster_label",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "name":        {"type": "string"},
                "short_name":  {"type": "string"},
                "description": {"type": "string"},
            },
            "required": ["name", "short_name", "description"],
            "additionalProperties": False,
        },
    },
}


def build_user_prompt(keywords: str, titles: list[str]) -> str:
    kw = keywords.strip() if isinstance(keywords, str) else ""
    lines = "\n".join(f"{i}. {t}" for i, t in enumerate(titles, 1))
    return (f"Keywords (most distinctive terms for this cluster):\n{kw}\n\n"
            f"Top cited paper titles in this cluster:\n{lines}")


def build_messages(keywords, titles):
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": build_user_prompt(keywords, titles)}]


def parse_content(content, refusal=None):
    """Return dict with name/short_name/description, or an error marker."""
    if refusal:
        return {"name": None, "short_name": None, "description": None, "error": f"refusal: {refusal}"}
    try:
        d = json.loads(content)
        return {"name": d["name"], "short_name": d["short_name"],
                "description": d["description"], "error": None}
    except Exception as e:
        return {"name": None, "short_name": None, "description": None, "error": f"parse: {e}"}


# ----------------------------------------------------------------------------
# load the clusters to name
# ----------------------------------------------------------------------------
def load_inputs() -> pd.DataFrame:
    """One row per micro cluster in this query: micro_cluster, keywords, titles(list)."""
    import awswrangler as wr
    micro = wr.s3.read_parquet(f"{OUT_BASE}cluster_report_micro/")[["micro_cluster"]].copy()
    micro["micro_cluster"] = micro["micro_cluster"].astype("int64")

    kw = wr.s3.read_parquet(f"{KEYWORDS_DIR}micro/")[["cluster", "keywords"]]
    kw["cluster"] = kw["cluster"].astype("int64")
    kw_map = dict(zip(kw["cluster"], kw["keywords"].fillna("")))

    papers = wr.s3.read_parquet(f"{OUT_BASE}article_top10/")[
        ["micro_cluster", "title", "citations"]].copy()
    papers["micro_cluster"] = papers["micro_cluster"].astype("int64")
    papers = papers.sort_values(["micro_cluster", "citations"], ascending=[True, False])
    titles_map = papers.groupby("micro_cluster")["title"].apply(list).to_dict()

    micro["keywords"] = micro["micro_cluster"].map(kw_map).fillna("")
    micro["titles"]   = micro["micro_cluster"].map(lambda m: titles_map.get(m, []))
    if MAX_CLUSTERS:
        micro = micro.head(MAX_CLUSTERS)
    print(f"[load] {len(micro):,} micro clusters to name for '{SUBQUERY}'")
    return micro


def get_client():
    from openai import OpenAI
    with open(KEY_FILE) as f:
        return OpenAI(api_key=f.read().strip())


# ----------------------------------------------------------------------------
# mode A: concurrent synchronous
# ----------------------------------------------------------------------------
def run_sync(client, rows) -> dict:
    def one(mid, messages):
        try:
            r = client.chat.completions.create(
                model=MODEL, messages=messages, response_format=RESPONSE_FORMAT)
            msg = r.choices[0].message
            return mid, parse_content(msg.content, getattr(msg, "refusal", None))
        except Exception as e:
            return mid, {"name": None, "short_name": None, "description": None, "error": str(e)}

    results, done = {}, 0
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(one, r.micro_cluster, build_messages(r.keywords, r.titles))
                for r in rows.itertuples(index=False)]
        for fut in cf.as_completed(futs):
            mid, res = fut.result()
            results[mid] = res
            done += 1
            if done % 25 == 0 or done == len(futs):
                print(f"  named {done}/{len(futs)}")
    return results


# ----------------------------------------------------------------------------
# mode B: OpenAI Batch API (50% cheaper, <=24h)
# ----------------------------------------------------------------------------
def run_batch(client, rows) -> dict:
    # build one JSONL request per cluster
    buf = io.BytesIO()
    for r in rows.itertuples(index=False):
        line = {
            "custom_id": f"micro-{int(r.micro_cluster)}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {"model": MODEL,
                     "messages": build_messages(r.keywords, r.titles),
                     "response_format": RESPONSE_FORMAT},
        }
        buf.write((json.dumps(line) + "\n").encode())
    buf.seek(0)

    # NB: a single file allows up to 50,000 requests; chunk beyond that.
    f = client.files.create(file=("cluster_names.jsonl", buf), purpose="batch")
    batch = client.batches.create(input_file_id=f.id, endpoint="/v1/chat/completions",
                                  completion_window="24h")
    print(f"[batch] submitted {batch.id}; polling (<=24h) ...")
    while True:
        batch = client.batches.retrieve(batch.id)
        if batch.status == "completed":
            break
        if batch.status in ("failed", "expired", "cancelled"):
            raise RuntimeError(f"batch ended: {batch.status}")
        time.sleep(30)

    results = {}
    out_text = client.files.content(batch.output_file_id).text
    for line in out_text.splitlines():
        obj = json.loads(line)
        mid = int(obj["custom_id"].split("-")[1])
        try:
            msg = obj["response"]["body"]["choices"][0]["message"]
            results[mid] = parse_content(msg.get("content"), msg.get("refusal"))
        except Exception as e:
            results[mid] = {"name": None, "short_name": None, "description": None, "error": str(e)}
    return results


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main():
    global DATABASE, SUBQUERY, SUBQUERIES_ROOT, KEYWORDS_DIR, OUT_BASE

    args = parse_args()
    paths = resolve_paths(
        snapshot=args.snapshot,
        query=args.query,
        subquery=args.subquery,
        query_folder=args.query_folder,
    )
    DATABASE = paths.database
    SUBQUERY = paths.subquery
    SUBQUERIES_ROOT = paths.subqueries_root
    KEYWORDS_DIR = paths.bertopic_root
    OUT_BASE = paths.subquery_base

    print("[config] database:", DATABASE)
    print("[config] snapshot:", paths.snapshot)
    print("[config] query:", paths.query)
    print("[config] subquery:", SUBQUERY)

    import awswrangler as wr
    rows = load_inputs()
    client = get_client()

    results = (run_batch if USE_BATCH else run_sync)(client, rows)

    out = pd.DataFrame([{"micro_cluster": mid, **res} for mid, res in results.items()])
    out["model"] = MODEL
    n_err = int(out["error"].notna().sum())
    if n_err:
        print(f"[warn] {n_err} clusters returned an error (see 'error' column)")
    keep = ["micro_cluster", "name", "short_name", "description", "model", "error"]
    wr.s3.to_parquet(out[keep].sort_values("micro_cluster"),
                     path=f"{OUT_BASE}cluster_names/", dataset=True, mode="overwrite")
    print(f"[done] {len(out):,} names -> {OUT_BASE}cluster_names/")


if __name__ == "__main__":
    main()