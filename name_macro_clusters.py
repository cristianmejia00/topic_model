"""
name_macro_clusters.py
======================
Assign a human-readable field/category name to each macro cluster.

Inputs (S3):
  - bertopic/macro/ keywords (required)
  - cluster_report_macro/ (required for complete macro list + optional stats)
  - cluster_report_meso/ + bertopic/meso/ (optional context)

Output (S3):
  s3://openalex-outputs/classification/q20260629/cluster_name_macro/
  with two columns: macro_cluster, name

Also prints the computed names in the terminal.
"""

from __future__ import annotations

import json
from collections import Counter

import awswrangler as wr
import pandas as pd


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BASE = "s3://openalex-outputs/classification/q20260629/"
KEYWORDS_DIR = f"{BASE}bertopic/"
KEY_FILE = ".key"

MACRO_REPORT_PATH = f"{BASE}cluster_report_macro/"
MESO_REPORT_PATH = f"{BASE}cluster_report_meso/"

OUT_PATH = f"{BASE}cluster_name_macro/"

MODEL = "gpt-4o-mini"

# Optional extra context from meso clusters.
INCLUDE_MESO_CONTEXT = True
MAX_MESO_PER_MACRO = 4


SYSTEM_PROMPT = (
    "You are a scientific taxonomy expert. "
    "You are naming macro-level academic fields from OpenAlex clusters. "
    "Each macro cluster should receive one concise, human-readable field name that is "
    "similar in spirit to Web of Science categories: clear, distinct, and recognizable.\n\n"
    "Rules:\n"
    "1) Produce one name per macro_cluster id provided.\n"
    "2) Names must be distinct across macro clusters.\n"
    "3) Prefer stable field names (e.g., 'Condensed Matter Physics', 'Molecular Biology').\n"
    "4) Avoid generic labels like 'General Science' unless evidence is truly mixed.\n"
    "5) Use Title Case, no trailing punctuation, 2-7 words.\n"
    "6) Output only valid JSON matching the schema."
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
                        },
                        "required": ["macro_cluster", "name"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["items"],
            "additionalProperties": False,
        },
    },
}


def read_optional(path: str) -> pd.DataFrame:
    try:
        return wr.s3.read_parquet(path)
    except Exception as exc:
        print(f"[warn] optional read failed at {path}: {exc}")
        return pd.DataFrame()


def get_client():
    from openai import OpenAI

    with open(KEY_FILE) as f:
        api_key = f.read().strip()
    return OpenAI(api_key=api_key)


def prepare_inputs() -> pd.DataFrame:
    macro_kw = wr.s3.read_parquet(f"{KEYWORDS_DIR}macro/")[ ["cluster", "keywords"] ].copy()
    macro_kw["cluster"] = macro_kw["cluster"].astype("int64")

    macro_rep = wr.s3.read_parquet(MACRO_REPORT_PATH).copy()
    if "macro_cluster" not in macro_rep.columns:
        raise KeyError("cluster_report_macro must contain 'macro_cluster'")
    macro_rep["macro_cluster"] = macro_rep["macro_cluster"].astype("int64")

    macro = pd.DataFrame({"macro_cluster": sorted(macro_rep["macro_cluster"].unique())})
    macro = macro.merge(
        macro_kw.rename(columns={"cluster": "macro_cluster", "keywords": "macro_keywords"}),
        on="macro_cluster",
        how="left",
    )

    if "publications" in macro_rep.columns:
        pubs = (
            macro_rep[["macro_cluster", "publications"]]
            .drop_duplicates(subset=["macro_cluster"])
            .copy()
        )
        pubs["publications"] = pd.to_numeric(pubs["publications"], errors="coerce")
        macro = macro.merge(pubs, on="macro_cluster", how="left")
    else:
        macro["publications"] = pd.NA

    macro["macro_keywords"] = macro["macro_keywords"].fillna("")

    if not INCLUDE_MESO_CONTEXT:
        macro["meso_context"] = ""
        return macro

    meso_rep = read_optional(MESO_REPORT_PATH)
    meso_kw = read_optional(f"{KEYWORDS_DIR}meso/")
    if meso_rep.empty or meso_kw.empty:
        macro["meso_context"] = ""
        return macro

    needed = {"meso_cluster", "macro_cluster"}
    if not needed.issubset(set(meso_rep.columns)):
        print("[warn] cluster_report_meso missing required columns for context")
        macro["meso_context"] = ""
        return macro

    if not {"cluster", "keywords"}.issubset(set(meso_kw.columns)):
        print("[warn] bertopic/meso missing required keyword columns")
        macro["meso_context"] = ""
        return macro

    meso_rep = meso_rep.copy()
    meso_kw = meso_kw.copy()
    meso_rep["meso_cluster"] = meso_rep["meso_cluster"].astype("int64")
    meso_rep["macro_cluster"] = meso_rep["macro_cluster"].astype("int64")
    meso_kw["cluster"] = meso_kw["cluster"].astype("int64")

    if "publications" in meso_rep.columns:
        meso_rep["publications"] = pd.to_numeric(meso_rep["publications"], errors="coerce").fillna(0)
    else:
        meso_rep["publications"] = 0

    meso = meso_rep.merge(
        meso_kw.rename(columns={"cluster": "meso_cluster", "keywords": "meso_keywords"}),
        on="meso_cluster",
        how="left",
    )
    meso["meso_keywords"] = meso["meso_keywords"].fillna("")
    meso = meso.sort_values(["macro_cluster", "publications"], ascending=[True, False])

    ctx_map: dict[int, str] = {}
    for mid, grp in meso.groupby("macro_cluster"):
        kw_list = [k.strip() for k in grp["meso_keywords"].head(MAX_MESO_PER_MACRO).tolist() if isinstance(k, str) and k.strip()]
        ctx_map[int(mid)] = " | ".join(kw_list)

    macro["meso_context"] = macro["macro_cluster"].map(lambda x: ctx_map.get(int(x), ""))
    return macro


def build_user_payload(rows: pd.DataFrame) -> str:
    items = []
    for r in rows.itertuples(index=False):
        items.append(
            {
                "macro_cluster": int(r.macro_cluster),
                "publications": None if pd.isna(r.publications) else int(r.publications),
                "macro_keywords": str(r.macro_keywords),
                "meso_context": str(r.meso_context),
            }
        )

    return (
        "Assign one field/category name to each macro cluster below.\n"
        "Return JSON with items: [{macro_cluster, name}] and include every macro_cluster exactly once.\n\n"
        f"clusters:\n{json.dumps(items, ensure_ascii=True)}"
    )


def request_names(client, payload_text: str) -> pd.DataFrame:
    response = client.chat.completions.create(
        model=MODEL,
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

    if not {"macro_cluster", "name"}.issubset(set(out.columns)):
        raise RuntimeError("Model output missing required fields")

    out["macro_cluster"] = out["macro_cluster"].astype("int64")
    out["name"] = out["name"].astype(str).str.strip()
    return out


def enforce_coverage_and_uniqueness(result: pd.DataFrame, expected_ids: list[int], macro_kw_map: dict[int, str]) -> pd.DataFrame:
    expected = set(expected_ids)
    got = set(result["macro_cluster"].tolist())

    missing = sorted(expected - got)
    extra = sorted(got - expected)
    if extra:
        print(f"[warn] dropping unexpected macro ids from model output: {extra[:10]}{'...' if len(extra) > 10 else ''}")
        result = result[result["macro_cluster"].isin(expected)].copy()

    if missing:
        print(f"[warn] filling {len(missing)} missing macro ids with fallback names")
        fill_rows = []
        for mid in missing:
            first_term = str(macro_kw_map.get(mid, "")).split(",")[0].strip()
            fallback = first_term.title() if first_term else f"Macro Field {mid}"
            fill_rows.append({"macro_cluster": mid, "name": fallback})
        if fill_rows:
            result = pd.concat([result, pd.DataFrame(fill_rows)], ignore_index=True)

    # Force distinct names if duplicates remain.
    counts = Counter(result["name"].tolist())
    if any(v > 1 for v in counts.values()):
        print("[warn] duplicate names detected; applying deterministic suffixes")
        seen: dict[str, int] = {}
        new_names = []
        for r in result.itertuples(index=False):
            nm = r.name
            seen[nm] = seen.get(nm, 0) + 1
            if counts[nm] == 1:
                new_names.append(nm)
            else:
                new_names.append(f"{nm} ({int(r.macro_cluster)})")
        result = result.copy()
        result["name"] = new_names

    result = result[["macro_cluster", "name"]].drop_duplicates(subset=["macro_cluster"]).sort_values("macro_cluster")
    return result


def main() -> None:
    print(f"[config] INCLUDE_MESO_CONTEXT={INCLUDE_MESO_CONTEXT}")
    print("[load] reading macro inputs from S3")
    macro = prepare_inputs()
    meso_rows = int(macro["meso_context"].astype(str).str.strip().ne("").sum())
    meso_active = meso_rows > 0
    print(f"[config] meso context used in payload: {meso_active} ({meso_rows}/{len(macro)} macros)")
    macro_ids = sorted(macro["macro_cluster"].astype("int64").tolist())
    macro_kw_map = dict(zip(macro["macro_cluster"].astype("int64"), macro["macro_keywords"].fillna("")))
    print(f"[load] macro clusters: {len(macro_ids):,}")

    payload = build_user_payload(macro)
    client = get_client()

    print(f"[openai] requesting names with model={MODEL}")
    raw = request_names(client, payload)
    out = enforce_coverage_and_uniqueness(raw, macro_ids, macro_kw_map)

    print("\n=== macro cluster names ===")
    for r in out.itertuples(index=False):
        print(f"{int(r.macro_cluster):>4}  {r.name}")

    wr.s3.to_parquet(out, path=OUT_PATH, dataset=True, mode="overwrite")
    print(f"\n[done] {len(out):,} macro names -> {OUT_PATH}")


if __name__ == "__main__":
    main()
