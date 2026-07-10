"""
name_macro_clusters_from_topCited.py
===================================
Assign umbrella macro-cluster names using only top-cited paper titles.

Design goals:
- Use no keyword context and no meso context.
- Send top-N cited titles per macro cluster (default 50) to the LLM.
- Produce distinct macro names across all macro clusters.
- Keep output schema compatible with existing downstream usage:
    columns = [macro_cluster, name, description]

Inputs (S3/Athena):
- cluster_report_macro/ for complete macro_cluster ID coverage
- article_report Athena table for top-cited titles per macro cluster

Output (S3):
- cluster_name_macro/
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import awswrangler as wr
import pandas as pd

from root_common_config import get_root_paths


ROOT_PATHS = get_root_paths()
KEY_FILE = Path(__file__).resolve().parent.parent / ".key"

OUT_PATH = ROOT_PATHS.cluster_name_macro
MACRO_REPORT_PATH = ROOT_PATHS.macro_report

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_TOP_TITLES = 50
DEFAULT_MACROS_PER_REQUEST = 8
DEFAULT_STAGING = "s3://openalex-outputs/athena-staging/"
DEFAULT_WORKGROUP = "primary"


SYSTEM_PROMPT = (
    "You are a scientific taxonomy expert naming macro-level academic clusters. "
    "You will receive ONLY top cited paper titles per macro cluster. "
    "Infer an umbrella topic that captures the major underlying subtopics represented by those papers.\n\n"
    "Rules:\n"
    "1) Produce one name per macro_cluster id provided.\n"
    "2) Produce one short description per macro_cluster id provided.\n"
    "3) Names must be distinct across macro clusters in this request.\n"
    "4) Use concise umbrella names that are still informative about underlying subtopics.\n"
    "5) Avoid generic labels like 'General Research', 'Miscellaneous', or 'Interdisciplinary Studies'.\n"
    "6) Use Title Case, 2-8 words, no trailing punctuation.\n"
    "7) Do not copy any paper title verbatim.\n"
    "8) Description must be 1-3 sentences and summarize major subtopics suggested by the titles.\n"
    "9) Output only valid JSON matching the schema."
)


RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "macro_cluster_names",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "macro_cluster": {"type": "integer"},
                            "name": {"type": "string"},
                            "description": {"type": "string"},
                        },
                        "required": ["macro_cluster", "name", "description"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["items"],
            "additionalProperties": False,
        },
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Name macro clusters using only top cited paper titles."
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI model name.")
    parser.add_argument(
        "--top-titles",
        type=int,
        default=DEFAULT_TOP_TITLES,
        help="Top cited titles per macro cluster used as LLM context.",
    )
    parser.add_argument(
        "--macros-per-request",
        type=int,
        default=DEFAULT_MACROS_PER_REQUEST,
        help="How many macro clusters to send per LLM request.",
    )
    parser.add_argument("--staging", default=DEFAULT_STAGING, help="Athena staging S3 path.")
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP, help="Athena workgroup.")
    return parser.parse_args()


def get_client():
    from openai import OpenAI

    api_key = KEY_FILE.read_text(encoding="utf-8").strip()
    if not api_key:
        raise RuntimeError(f"Empty API key file: {KEY_FILE}")
    return OpenAI(api_key=api_key)


def chunked(values: list[int], size: int):
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def run_sql(sql: str, *, database: str, staging: str, workgroup: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql,
        database=database,
        s3_output=staging,
        workgroup=workgroup,
        ctas_approach=False,
    )


def load_macro_ids() -> list[int]:
    macro_rep = wr.s3.read_parquet(MACRO_REPORT_PATH)
    if "macro_cluster" not in macro_rep.columns:
        raise KeyError("cluster_report_macro must contain 'macro_cluster'")
    ids = (
        pd.to_numeric(macro_rep["macro_cluster"], errors="coerce")
        .dropna()
        .astype("int64")
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    if not ids:
        raise RuntimeError("No macro clusters found in cluster_report_macro")
    return ids


def load_top_titles_by_macro(
    *,
    top_titles: int,
    database: str,
    staging: str,
    workgroup: str,
) -> dict[int, list[str]]:
    sql = f"""
    WITH ranked AS (
      SELECT
        CAST(macro_cluster AS bigint) AS macro_cluster,
        TRIM(CAST(title AS varchar)) AS title,
        CAST(citations AS bigint) AS citations,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(macro_cluster AS bigint)
          ORDER BY CAST(citations AS bigint) DESC NULLS LAST,
                   TRIM(CAST(title AS varchar)) ASC
        ) AS rn
      FROM article_report
      WHERE macro_cluster IS NOT NULL
        AND title IS NOT NULL
        AND TRIM(CAST(title AS varchar)) <> ''
    )
    SELECT macro_cluster, title
    FROM ranked
    WHERE rn <= {int(top_titles)}
    ORDER BY macro_cluster, rn
    """

    df = run_sql(sql, database=database, staging=staging, workgroup=workgroup)
    if df.empty:
        return {}

    df["macro_cluster"] = pd.to_numeric(df["macro_cluster"], errors="coerce")
    df = df.dropna(subset=["macro_cluster"]).copy()
    df["macro_cluster"] = df["macro_cluster"].astype("int64")
    df["title"] = df["title"].fillna("").astype(str).str.strip()
    df = df[df["title"] != ""]

    grouped = df.groupby("macro_cluster", sort=True)["title"].apply(list)
    return {int(k): v for k, v in grouped.to_dict().items()}


def fallback_name_from_titles(macro_cluster: int, titles: list[str]) -> str:
    if not titles:
        return f"Macro Topic {macro_cluster}"

    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9\-']*", titles[0])
    if not words:
        return f"Macro Topic {macro_cluster}"

    label = " ".join(words[:6]).strip().title()
    return label or f"Macro Topic {macro_cluster}"


def fallback_description_from_titles(macro_cluster: int, titles: list[str]) -> str:
    if not titles:
        return (
            "Umbrella topic inferred from highly cited work represented in this "
            f"macro cluster {macro_cluster}."
        )

    snippets = [str(t).strip() for t in titles[:3] if str(t).strip()]
    if not snippets:
        return (
            "Umbrella topic inferred from highly cited work represented in this "
            f"macro cluster {macro_cluster}."
        )

    joined = "; ".join(snippets)
    return (
        "Umbrella topic inferred from highly cited studies spanning subtopics suggested by: "
        f"{joined}."
    )


def build_user_payload(
    *,
    macro_ids: list[int],
    title_map: dict[int, list[str]],
    disallowed_names: list[str],
) -> str:
    items = []
    for macro_id in macro_ids:
        items.append(
            {
                "macro_cluster": int(macro_id),
                "top_titles": title_map.get(int(macro_id), []),
            }
        )

    avoid_text = ""
    if disallowed_names:
        avoid_text = (
            "Already used names in previous batches (do not reuse exactly):\n"
            + json.dumps(disallowed_names, ensure_ascii=True)
            + "\n\n"
        )

    return (
        "Assign one umbrella macro topic name to each macro cluster below.\n"
        "Return JSON with items: [{macro_cluster, name, description}] and include every macro_cluster exactly once.\n"
        "Names must be distinct and informative.\n\n"
        f"{avoid_text}"
        f"clusters:\n{json.dumps(items, ensure_ascii=True)}"
    )


def request_names(client, *, model: str, payload_text: str) -> pd.DataFrame:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": payload_text},
        ],
        response_format=RESPONSE_FORMAT,
    )

    message = response.choices[0].message
    if getattr(message, "refusal", None):
        raise RuntimeError(f"Model refusal: {message.refusal}")

    data = json.loads(message.content)
    items = data.get("items", [])
    out = pd.DataFrame(items)
    if out.empty:
        raise RuntimeError("Model returned empty naming output")

    if not {"macro_cluster", "name", "description"}.issubset(set(out.columns)):
        raise RuntimeError("Model output missing required fields")

    out["macro_cluster"] = pd.to_numeric(out["macro_cluster"], errors="coerce")
    out = out.dropna(subset=["macro_cluster"]).copy()
    out["macro_cluster"] = out["macro_cluster"].astype("int64")
    out["name"] = out["name"].astype(str).str.strip()
    out["description"] = out["description"].astype(str).str.strip()
    return out


def normalize_chunk_result(
    *,
    raw: pd.DataFrame,
    chunk_ids: list[int],
    title_map: dict[int, list[str]],
    used_names_lower: set[str],
) -> pd.DataFrame:
    expected = set(chunk_ids)
    out = raw[raw["macro_cluster"].isin(expected)].copy()

    missing = sorted(expected - set(out["macro_cluster"].tolist()))
    if missing:
        print(f"[warn] filling {len(missing)} missing names with fallback labels")
        fill = []
        for mid in missing:
            fill.append(
                {
                    "macro_cluster": mid,
                    "name": fallback_name_from_titles(mid, title_map.get(mid, [])),
                    "description": fallback_description_from_titles(mid, title_map.get(mid, [])),
                }
            )
        out = pd.concat([out, pd.DataFrame(fill)], ignore_index=True)

    out = out.drop_duplicates(subset=["macro_cluster"], keep="first").copy()

    # Enforce global distinct names case-insensitively.
    local_seen: dict[str, int] = {}
    fixed_names: list[str] = []
    fixed_descriptions: list[str] = []
    for row in out.itertuples(index=False):
        name = str(row.name).strip() or fallback_name_from_titles(int(row.macro_cluster), title_map.get(int(row.macro_cluster), []))
        description = str(row.description).strip() or fallback_description_from_titles(
            int(row.macro_cluster),
            title_map.get(int(row.macro_cluster), []),
        )
        base_lower = name.lower()
        local_seen[base_lower] = local_seen.get(base_lower, 0) + 1

        if base_lower in used_names_lower or local_seen[base_lower] > 1:
            name = f"{name} ({int(row.macro_cluster)})"
            base_lower = name.lower()

        fixed_names.append(name)
        fixed_descriptions.append(description)
        used_names_lower.add(base_lower)

    out = out.copy()
    out["name"] = fixed_names
    out["description"] = fixed_descriptions
    return out[["macro_cluster", "name", "description"]].sort_values("macro_cluster")


def main() -> None:
    args = parse_args()

    if args.top_titles <= 0:
        raise RuntimeError("--top-titles must be positive")
    if args.macros_per_request <= 0:
        raise RuntimeError("--macros-per-request must be positive")

    database = ROOT_PATHS.database
    print("[config] snapshot:", ROOT_PATHS.snapshot)
    print("[config] query:", ROOT_PATHS.query)
    print("[config] database:", database)
    print("[config] model:", args.model)
    print("[config] top_titles:", args.top_titles)
    print("[config] macros_per_request:", args.macros_per_request)

    macro_ids = load_macro_ids()
    print(f"[load] macro clusters: {len(macro_ids):,}")

    title_map = load_top_titles_by_macro(
        top_titles=args.top_titles,
        database=database,
        staging=args.staging,
        workgroup=args.workgroup,
    )

    with_titles = sum(1 for mid in macro_ids if title_map.get(mid))
    print(f"[load] macros with at least one title: {with_titles:,}/{len(macro_ids):,}")

    client = get_client()

    used_names_lower: set[str] = set()
    parts: list[pd.DataFrame] = []
    chunks = list(chunked(macro_ids, args.macros_per_request))

    for idx, macro_chunk in enumerate(chunks, start=1):
        payload = build_user_payload(
            macro_ids=macro_chunk,
            title_map=title_map,
            disallowed_names=sorted(used_names_lower),
        )
        print(f"[openai] batch {idx}/{len(chunks)} macros={len(macro_chunk)}")
        raw = request_names(client, model=args.model, payload_text=payload)
        fixed = normalize_chunk_result(
            raw=raw,
            chunk_ids=macro_chunk,
            title_map=title_map,
            used_names_lower=used_names_lower,
        )
        parts.append(fixed)

    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["macro_cluster"], keep="first").sort_values("macro_cluster")

    missing = sorted(set(macro_ids) - set(out["macro_cluster"].tolist()))
    if missing:
        fill_rows = [
            {
                "macro_cluster": mid,
                "name": fallback_name_from_titles(mid, title_map.get(mid, [])),
                "description": fallback_description_from_titles(mid, title_map.get(mid, [])),
            }
            for mid in missing
        ]
        out = pd.concat([out, pd.DataFrame(fill_rows)], ignore_index=True).sort_values("macro_cluster")

    # Final distinctness guard.
    seen: set[str] = set()
    final_names: list[str] = []
    final_descriptions: list[str] = []
    for row in out.itertuples(index=False):
        name = str(row.name).strip() or fallback_name_from_titles(int(row.macro_cluster), title_map.get(int(row.macro_cluster), []))
        description = str(row.description).strip() or fallback_description_from_titles(
            int(row.macro_cluster),
            title_map.get(int(row.macro_cluster), []),
        )
        key = name.lower()
        if key in seen:
            name = f"{name} ({int(row.macro_cluster)})"
            key = name.lower()
        seen.add(key)
        final_names.append(name)
        final_descriptions.append(description)

    out = out.copy()
    out["name"] = final_names
    out["description"] = final_descriptions
    out = out[["macro_cluster", "name", "description"]].sort_values("macro_cluster")

    print("\n=== macro cluster names ===")
    for row in out.itertuples(index=False):
        print(f"{int(row.macro_cluster):>4}  {row.name}")
        print(f"      {row.description}")

    wr.s3.to_parquet(out, path=OUT_PATH, dataset=True, mode="overwrite")
    print(f"\n[done] {len(out):,} macro names + descriptions -> {OUT_PATH}")


if __name__ == "__main__":
    main()
