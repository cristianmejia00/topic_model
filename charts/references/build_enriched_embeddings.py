"""Build enriched embeddings by prepending cluster/subcluster names to text.

Reads rcs_merged.csv at level 0 (and optionally at subcluster level) to obtain
global_name for each cluster, then prepends those names to the original TI+AB
text before encoding.
The result is a new embedding set saved to ``e01_enriched/`` alongside the
original ``e01/``.

Called from ``scripts/enriched_embeds_only.R`` via ``system2()``.

Usage
-----
    python pipelines/dataset/build_enriched_embeddings.py \\
        --config-analysis config_analysis.yml \\
        --config-dataset  config_dataset.yml \\
        [--level 1]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from sentence_transformers import SentenceTransformer

# Allow importing sibling module when invoked from repo root
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_embeddings import _as_str, _clean_text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_global_names(rcs_path: Path) -> dict[str, str]:
    """Return {cluster_code: global_name} from an rcs_merged.csv."""
    df = pd.read_csv(rcs_path, encoding="latin-1")
    if "global_name" not in df.columns or "cluster_code" not in df.columns:
        raise ValueError(f"rcs_merged.csv at {rcs_path} missing required columns")
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row["cluster_code"]).strip()
        gn = _as_str(row["global_name"]).strip()
        if gn and gn.lower() != "nan":
            out[code] = gn
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Enriched embeddings builder")
    p.add_argument("--config-analysis", required=True)
    p.add_argument("--config-dataset", required=True)
    p.add_argument("--level", type=int, default=1,
                   help="Subcluster level to enrich (0 = level0-only; default: 1)")
    p.add_argument("--force", action="store_true",
                   help="Recompute even if embeddings already exist")
    args = p.parse_args()

    # ── Load configs ──────────────────────────────────────────────────────
    with open(args.config_analysis, "r", encoding="utf-8") as f:
        cfg_a = yaml.safe_load(f)
    with open(args.config_dataset, "r", encoding="utf-8") as f:
        cfg_d = yaml.safe_load(f)

    meta_a = cfg_a["metadata"]
    bib_dir = Path(meta_a["bibliometrics_directory"])
    project = meta_a["project_folder"]
    analysis_id = meta_a["analysis_id"]
    filtered_folder = cfg_d["embeds"]["from_filtered_dataset"]

    # ── Check cache ───────────────────────────────────────────────────────
    out_dir = bib_dir / project / filtered_folder / "e01_enriched"
    cached_npy = out_dir / "embeddings.npy"
    if cached_npy.exists() and not args.force:
        print(f"Enriched embeddings already exist: {cached_npy}")
        print("Use --force to recompute.")
        return

    analysis_type = cfg_a.get("params", {}).get("type_of_analysis", "citation_network")

    # Citation-network analysis stores reports under <analysis_id>/<algorithm>/<threshold>
    # while topic-model analysis stores them under <analysis_id>.
    if analysis_type == "citation_network":
        cn = cfg_a.get("citation_network", {})
        algorithm = cn.get("clustering", {}).get("algorithm", "louvain")
        threshold = str(cn.get("thresholding", {}).get("threshold", "0.9"))
        analysis_root = bib_dir / project / analysis_id / algorithm / threshold
    else:
        analysis_root = bib_dir / project / analysis_id

    # ── Load global-name lookups ──────────────────────────────────────────
    rcs_level0 = analysis_root / "level0" / "rcs_merged.csv"
    rcs_leveln = analysis_root / f"level{args.level}" / "rcs_merged.csv"

    if not rcs_level0.exists():
        raise FileNotFoundError(f"rcs_merged.csv not found at level0: {rcs_level0}")

    names_l0 = _load_global_names(rcs_level0)
    names_ln: dict[str, str] = {}
    if args.level > 0:
        if not rcs_leveln.exists():
            raise FileNotFoundError(f"rcs_merged.csv not found at level{args.level}: {rcs_leveln}")
        names_ln = _load_global_names(rcs_leveln)

    if not names_l0:
        raise ValueError("No global_name values found in level0 rcs_merged.csv — run AI naming first")
    if args.level > 0 and not names_ln:
        raise ValueError(f"No global_name values found in level{args.level} rcs_merged.csv — run AI naming first")

    print(f"Level 0 names: {len(names_l0)} clusters")
    if args.level > 0:
        print(f"Level {args.level} names: {len(names_ln)} subclusters")
    else:
        print("Level 0-only enrichment mode")

    # ── Load datasets ─────────────────────────────────────────────────────
    minimal_path = analysis_root / "dataset_minimal.csv"
    if not minimal_path.exists():
        raise FileNotFoundError(f"dataset_minimal.csv not found: {minimal_path}")

    raw_path = bib_dir / project / filtered_folder / "dataset_raw_cleaned.csv"
    if not raw_path.exists():
        raise FileNotFoundError(f"dataset_raw_cleaned.csv not found: {raw_path}")

    dm = pd.read_csv(minimal_path, encoding="latin-1")
    dr = pd.read_csv(raw_path, encoding="latin-1")

    # Identify columns
    sub_col = None
    if args.level > 0:
        sub_col = f"subcluster_label{args.level}"
        if sub_col not in dm.columns:
            raise ValueError(f"Column '{sub_col}' not found in dataset_minimal.csv")
    if "level0" not in dm.columns:
        raise ValueError("Column 'level0' not found in dataset_minimal.csv")

    # Merge on UT
    select_cols = ["UT", "level0"] + ([sub_col] if sub_col else [])
    merged = dm[select_cols].merge(
        dr[["UT", "TI", "AB"]], on="UT", how="inner"
    )
    print(f"Documents after merge: {len(merged)}")

    # ── Build enriched text ───────────────────────────────────────────────
    embeds_cfg = cfg_d["embeds"]
    profile = embeds_cfg.get("e01", {})

    rows = []
    for _, row in merged.iterrows():
        parent_code = str(int(row["level0"])) if pd.notna(row["level0"]) else ""
        sub_code = _as_str(row[sub_col]).strip() if sub_col else ""

        parent_name = names_l0.get(parent_code, "")
        sub_name = names_ln.get(sub_code, "") if sub_col else ""

        # For subcluster enrichment, skip docs without valid subcluster mapping.
        if sub_col and (not sub_code or not sub_name):
            continue

        ti = _as_str(row["TI"])
        ab = _as_str(row["AB"])

        prefix_parts = [p for p in [parent_name, sub_name] if p]
        prefix = ". ".join(prefix_parts)
        raw_text = f"{prefix}. {ti} {ab}" if prefix else f"{ti} {ab}"
        text = _clean_text(raw_text, profile)

        rows.append({"UT": row["UT"], "text": text})

    if not rows:
        raise ValueError("No documents matched — check that global_name and subcluster assignments exist")

    corpus = pd.DataFrame(rows)
    print(f"Enriched documents: {len(corpus)}")

    # ── Encode ────────────────────────────────────────────────────────────
    model_name = profile.get("transformer_model", "all-MiniLM-L6-v2")
    print(f"Model: {model_name}")
    model = SentenceTransformer(model_name)
    embeddings = model.encode(corpus["text"].tolist(), show_progress_bar=True)

    # ── Save ──────────────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)

    np.save(out_dir / "embeddings.npy", embeddings)

    ids = corpus["UT"].astype(str).tolist()
    with (out_dir / "embeddings_ids.json").open("w", encoding="utf-8") as f:
        json.dump(ids, f)

    corpus.to_csv(out_dir / "corpus.csv", index=False)

    settings_out = {**profile, "enriched": True, "level": args.level}
    with (out_dir / "embeds_settings.json").open("w", encoding="utf-8") as f:
        json.dump(settings_out, f, indent=2)

    print(f"\nSaved enriched embeddings to: {out_dir}")
    print(f"  embeddings.npy       shape: {embeddings.shape}")
    print(f"  embeddings_ids.json  count: {len(ids)}")
    print(f"  corpus.csv           rows:  {len(corpus)}")


if __name__ == "__main__":
    main()
