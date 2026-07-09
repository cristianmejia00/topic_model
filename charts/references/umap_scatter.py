#!/usr/bin/env python3
"""UMAP scatter plot of document embeddings colored by cluster.

Each document is a dot in a 2D UMAP projection of its text embedding, colored
by its main (parent) cluster.  Cluster labels are placed on the left and right
margins with elbow connectors pointing to each cluster's centroid, using
anti-overlap vertical spreading.

Called from R via ``system2()`` during report generation, or standalone.

Usage
-----
    python pipelines/charts/umap_scatter.py \
        --embeddings-dir  path/to/e01/ \
        --doc-clusters    path/to/doc_clusters.csv \
        --rcs             path/to/rcs_merged.csv \
        --palette         assets/fukan_colors.json \
        --output          path/to/fig_umap_scatter.png \
        [--seed 100] [--label-min 20] [--title "…"] [--force]

Inputs
------
- ``embeddings.npy`` + ``embeddings_ids.json`` in *--embeddings-dir*
- ``doc_clusters.csv`` with columns ``UT, X_C`` (written by the R pipeline)
- ``rcs_merged.csv`` with cluster metadata (from ``02_rcs.R``)

UMAP coordinates are cached as ``umap_2d_coords.csv`` next to the embeddings
so that subsequent calls (e.g., level 1 after level 0) skip the expensive
projection.  Use ``--force`` to recompute.
"""
from __future__ import annotations

import argparse
import colorsys
import json
import re
import sys
from itertools import cycle
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np               # noqa: E402
import pandas as pd              # noqa: E402


# ---------------------------------------------------------------------------
# Palette & color helpers
# ---------------------------------------------------------------------------

def _load_palette(path: Path) -> list[str]:
    """Load fukan_colors.json -> flat list of 18 hex colors (base + extended)."""
    with path.open("r", encoding="utf-8") as f:
        pal = json.load(f)
    return pal["base"] + [c.lower() for c in pal["extended"]]


def _clean_code(code: str) -> str:
    """Strip trailing dashes and -0 suffixes: '1-2---' -> '1-2'."""
    return re.sub(r"---$", "", re.sub(r"-0$", "", str(code)))


def _extract_main(code: str) -> str:
    """Top-level cluster number: '1-2---' -> '1'."""
    return _clean_code(code).split("-")[0]


def _sort_key(x: str):
    """Numeric-aware sort key for cluster codes."""
    try:
        return (0, int(x))
    except ValueError:
        return (1, x)


def _assign_colors(main_clusters: pd.Series, palette: list[str]) -> dict[str, str]:
    """Map sorted unique main-clusters to palette colors.

    Replicates the R logic in chart_utils.R ``assign_cluster_colors()``:
    the last cluster (highest number, typically 99 = miscellaneous) always
    gets ``#d3d3d3`` grey.  If there are more clusters than palette entries,
    overflow clusters also get grey.
    """
    uniques = sorted(main_clusters.unique(), key=_sort_key)
    n = len(uniques)
    cmap: dict[str, str] = {}
    for i, mc in enumerate(uniques):
        if i == n - 1:
            cmap[mc] = "#d3d3d3"
        elif i < len(palette):
            cmap[mc] = palette[i]
        else:
            cmap[mc] = "#d3d3d3"
    return cmap


def _hex_to_hsl(hex_color: str) -> tuple[float, float, float]:
    """Convert hex color to (H, S, L) with all values in [0, 1]."""
    hex_color = hex_color.lstrip("#")
    r, g, b = (int(hex_color[i:i+2], 16) / 255.0 for i in (0, 2, 4))
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    return h, s, l


def _hsl_to_hex(h: float, s: float, l: float) -> str:
    """Convert (H, S, L) back to hex string."""
    r, g, b = colorsys.hls_to_rgb(h, l, s)
    return "#{:02x}{:02x}{:02x}".format(
        int(round(r * 255)), int(round(g * 255)), int(round(b * 255))
    )


def _assign_subcluster_colors(
    cluster_codes: pd.Series,
    parent_color_map: dict[str, str],
) -> dict[str, str]:
    """Generate lightness ramps within each parent color for subclusters.

    For each parent cluster, subclusters get shades that vary in lightness
    from 0.30 to 0.70, keeping the parent hue and saturation recognisable.
    Single-subcluster parents keep the original parent color.
    """
    codes = cluster_codes.unique()
    parent_to_subs: dict[str, list[str]] = {}
    code_to_clean: dict[str, str] = {}
    for raw in codes:
        clean = _clean_code(str(raw))
        code_to_clean[str(raw)] = clean
        parent = _extract_main(str(raw))
        parent_to_subs.setdefault(parent, []).append(str(raw))

    for parent in parent_to_subs:
        parent_to_subs[parent] = sorted(
            parent_to_subs[parent],
            key=lambda c: _sort_key(code_to_clean.get(c, c)),
        )

    sub_color_map: dict[str, str] = {}
    for parent, subs in parent_to_subs.items():
        base_hex = parent_color_map.get(parent, "#d3d3d3")
        if base_hex == "#d3d3d3" or len(subs) <= 1:
            for s in subs:
                sub_color_map[s] = base_hex
            continue

        h, s_val, _ = _hex_to_hsl(base_hex)
        n = len(subs)
        l_min, l_max = 0.30, 0.70
        for i, sub_code in enumerate(subs):
            l = l_min + (l_max - l_min) * i / max(n - 1, 1)
            sub_color_map[sub_code] = _hsl_to_hex(h, s_val, l)

    return sub_color_map


def _draw_hulls(ax, df, color_col="sub_color", cluster_col="X_C", alpha=0.10):
    """Draw convex hull outlines around each subcluster's points."""
    from scipy.spatial import ConvexHull

    for cluster_id, grp in df.groupby(cluster_col):
        pts = grp[["x", "y"]].values
        if len(pts) < 3:
            continue
        try:
            hull = ConvexHull(pts)
        except Exception:
            continue
        vertices = np.append(hull.vertices, hull.vertices[0])
        color = grp[color_col].iloc[0]
        ax.fill(
            pts[vertices, 0], pts[vertices, 1],
            fc=color, alpha=alpha, ec=color, linewidth=0.6,
            zorder=1,
        )


def _resolve_label(row: pd.Series) -> str:
    """Pick the best display label for a cluster.

    Priority: global_name > cluster_name > clean cluster_code.
    The cluster_code is always prepended when a name is available.
    """
    code = _clean_code(str(row.get("cluster_code", row.get("cluster", ""))))
    gn = str(row.get("global_name", "") or "").strip()
    if gn and gn.lower() != "nan":
        return f"{code}. {gn}"
    cn = str(row.get("cluster_name", "") or "").strip()
    if cn and cn.lower() != "nan":
        return f"{code}. {cn}"
    return code


# ---------------------------------------------------------------------------
# Label placement (ported from future_codebase/reporting.ipynb)
# ---------------------------------------------------------------------------

def _spread_targets(y_values, y_min, y_max, min_gap):
    """Spread y-positions to prevent label overlap."""
    if len(y_values) == 0:
        return np.array([], dtype=float)
    ys = np.clip(np.array(y_values, dtype=float), y_min, y_max)

    # Forward pass (top to bottom): enforce minimum separation
    for i in range(1, len(ys)):
        if ys[i - 1] - ys[i] < min_gap:
            ys[i] = ys[i - 1] - min_gap

    # Shift up if below lower bound
    if ys[-1] < y_min:
        ys += y_min - ys[-1]
        for i in range(len(ys) - 2, -1, -1):
            if ys[i] - ys[i + 1] < min_gap:
                ys[i] = ys[i + 1] + min_gap

    # Shift down if above upper bound
    if ys[0] > y_max:
        ys -= ys[0] - y_max

    return np.clip(ys, y_min, y_max)


def _pick_labels_for_side(sub, y_min, y_max, min_gap, max_labels=None):
    """Select a vertically spread subset of labels for one plot side."""
    if sub.empty:
        return sub

    capacity = max(1, int(np.floor((y_max - y_min) / min_gap)) + 1)
    target_n = capacity if max_labels is None else min(capacity, max_labels)
    target_n = min(target_n, len(sub))

    if len(sub) <= target_n:
        return sub.sort_values("cy", ascending=False).reset_index(drop=True)

    work = sub.copy()
    bins = np.linspace(y_min, y_max, target_n + 1)
    work["y_bin"] = np.clip(np.digitize(work["cy"], bins) - 1, 0, target_n - 1)
    primary = (
        work.sort_values(["y_bin", "n_docs"], ascending=[True, False])
        .groupby("y_bin", as_index=False)
        .head(1)
    )

    if len(primary) < target_n:
        used = set(primary.index.tolist())
        rest = (
            work.loc[~work.index.isin(used)]
            .sort_values("n_docs", ascending=False)
            .head(target_n - len(primary))
        )
        chosen = pd.concat([primary, rest], axis=0)
    else:
        chosen = primary

    return (
        chosen.sort_values("cy", ascending=False)
        .head(target_n)
        .reset_index(drop=True)
    )


def _add_side_labels(
    ax,
    labels_df,
    xlim,
    ylim,
    min_gap_frac=0.024,
    text_pad_frac=0.10,
    elbow_frac=0.04,
    fontsize=5.5,
    max_labels_per_side=None,
    max_chars=34,
):
    """Place labels at plot margins with elbow connectors."""
    if labels_df.empty:
        return

    x_min, x_max = xlim
    y_min, y_max = ylim
    x_span, y_span = x_max - x_min, y_max - y_min

    x_left = x_min - text_pad_frac * x_span
    x_right = x_max + text_pad_frac * x_span
    x_elbow_l = x_min - elbow_frac * x_span
    x_elbow_r = x_max + elbow_frac * x_span
    min_gap = min_gap_frac * y_span
    center_x = 0.5 * (x_min + x_max)

    labels = labels_df.copy()
    labels["side"] = np.where(labels["cx"] <= center_x, "left", "right")

    for side in ("left", "right"):
        sub = labels[labels["side"] == side].copy()
        if sub.empty:
            continue

        sub = _pick_labels_for_side(
            sub, y_min, y_max, min_gap, max_labels_per_side
        )
        sub = sub.sort_values("cy", ascending=False).reset_index(drop=True)
        sub["target_y"] = _spread_targets(
            sub["cy"].to_numpy(), y_min, y_max, min_gap
        )

        n_sub = len(sub)
        for i, (_, row) in enumerate(sub.iterrows()):
            if side == "left":
                x_txt = x_left
                x_elb = x_elbow_l - 0.004 * x_span * (i - (n_sub - 1) / 2)
                ha = "right"
            else:
                x_txt = x_right
                x_elb = x_elbow_r + 0.004 * x_span * (i - (n_sub - 1) / 2)
                ha = "left"

            ax.plot(
                [row["cx"], x_elb, x_txt],
                [row["cy"], row["target_y"], row["target_y"]],
                color="0.35",
                linewidth=0.45,
                alpha=0.65,
                zorder=3,
                clip_on=False,
            )
            ax.text(
                x_txt,
                row["target_y"],
                str(row["label"])[:max_chars],
                fontsize=fontsize,
                ha=ha,
                va="center",
                color="black",
                bbox=dict(
                    boxstyle="round,pad=0.12", fc="white", alpha=0.85, ec="none"
                ),
                zorder=4,
                clip_on=False,
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="UMAP scatter plot of document embeddings colored by cluster."
    )
    p.add_argument(
        "--embeddings-dir",
        required=True,
        help="Directory containing embeddings.npy + embeddings_ids.json",
    )
    p.add_argument(
        "--doc-clusters",
        required=True,
        help="CSV with UT and X_C columns (document -> cluster mapping)",
    )
    p.add_argument(
        "--rcs", required=True, help="Path to rcs_merged.csv"
    )
    p.add_argument(
        "--palette", required=True, help="Path to fukan_colors.json"
    )
    p.add_argument(
        "--output", required=True, help="Output file path (PNG recommended)"
    )
    p.add_argument("--seed", type=int, default=100, help="UMAP random seed")
    p.add_argument(
        "--label-min",
        type=int,
        default=20,
        help="Min documents per cluster to show a label",
    )
    p.add_argument("--title", default="", help="Chart title")
    p.add_argument(
        "--force",
        action="store_true",
        help="Force UMAP recomputation (ignore cached coordinates)",
    )
    args = p.parse_args()

    emb_dir = Path(args.embeddings_dir)
    emb_path = emb_dir / "embeddings.npy"
    ids_path = emb_dir / "embeddings_ids.json"
    cache_path = emb_dir / "umap_2d_coords.csv"

    if not emb_path.exists():
        sys.exit(f"Embeddings not found: {emb_path}")
    if not ids_path.exists():
        sys.exit(f"Embedding IDs not found: {ids_path}")

    # ── Load embeddings ──────────────────────────────────────────────────
    print(f"[umap_scatter] Loading embeddings: {emb_path}")
    embeddings = np.load(emb_path)
    with ids_path.open("r", encoding="utf-8") as f:
        emb_ids = json.load(f)
    assert len(emb_ids) == embeddings.shape[0], "Embeddings / IDs length mismatch"
    print(f"[umap_scatter] Embeddings shape: {embeddings.shape}")

    # ── UMAP 2D projection (cached) ─────────────────────────────────────
    coords_df = None
    if cache_path.exists() and not args.force:
        print(f"[umap_scatter] Loading cached coordinates: {cache_path}")
        coords_df = pd.read_csv(cache_path, dtype={"UT": str})
        if len(coords_df) != len(emb_ids):
            print("[umap_scatter] Cache size mismatch - recomputing")
            coords_df = None

    if coords_df is None:
        from umap import UMAP

        print(
            f"[umap_scatter] Computing UMAP "
            f"(n={len(embeddings)}, seed={args.seed}) ..."
        )
        xy = UMAP(
            n_components=2,
            n_neighbors=15,
            min_dist=0.1,
            metric="cosine",
            random_state=args.seed,
        ).fit_transform(embeddings)
        coords_df = pd.DataFrame(
            {"UT": emb_ids, "x": xy[:, 0], "y": xy[:, 1]}
        )
        coords_df.to_csv(cache_path, index=False)
        print(f"[umap_scatter] Cached coordinates: {cache_path}")

    # ── Load cluster data ────────────────────────────────────────────────
    doc_cl = pd.read_csv(args.doc_clusters, dtype={"UT": str, "X_C": str})
    rcs = pd.read_csv(args.rcs, dtype=str)

    # ── Merge: docs <-> coords <-> cluster metadata ─────────────────────
    df = doc_cl.merge(coords_df, on="UT", how="inner")

    # Build cluster -> metadata lookup from rcs
    rcs_key = "cluster" if "cluster" in rcs.columns else "cluster_code"
    rcs_dedup = rcs.drop_duplicates(subset=[rcs_key])
    rcs_map = rcs_dedup.set_index(rcs_key)

    for col in ("cluster_code", "cluster_name", "global_name"):
        if col in rcs_map.columns:
            df[col] = df["X_C"].map(rcs_map[col])

    if "cluster_code" not in df.columns or df["cluster_code"].isna().all():
        df["cluster_code"] = df["X_C"]
    df["cluster_code"] = df["cluster_code"].fillna(df["X_C"])

    # ── Colors (fukan palette) ───────────────────────────────────────────
    df["main_cluster"] = df["cluster_code"].apply(_extract_main)
    palette = _load_palette(Path(args.palette))

    matched = df["X_C"].isin(set(rcs_dedup[rcs_key].astype(str)))
    color_map = _assign_colors(df.loc[matched, "main_cluster"], palette)
    df["color"] = df["main_cluster"].map(color_map).fillna("#d3d3d3")
    df.loc[~matched, "color"] = "#d3d3d3"

    # Detect subclusters: codes like "1-1", "1-2", "2-1" etc.
    clean_codes = df.loc[matched, "cluster_code"].apply(_clean_code)
    is_subcluster = clean_codes.str.contains("-", na=False).any()

    if is_subcluster:
        sub_color_map = _assign_subcluster_colors(
            df.loc[matched, "cluster_code"], color_map
        )
        df["sub_color"] = df["cluster_code"].map(sub_color_map).fillna(df["color"])
        df.loc[~matched, "sub_color"] = "#d3d3d3"
        print(f"[umap_scatter] Subcluster mode: {len(sub_color_map)} sub-shades")
    else:
        df["sub_color"] = df["color"]

    n_total = len(df)
    n_grey = (~matched).sum()
    print(
        f"[umap_scatter] Documents: {n_total:,} "
        f"(matched: {n_total - n_grey:,}, unmatched: {n_grey:,})"
    )

    # ── Label anchors (one per cluster) ──────────────────────────────────
    label_map = {
        str(row[rcs_key]): _resolve_label(row)
        for _, row in rcs_dedup.iterrows()
    }
    df["label"] = df["X_C"].map(label_map)

    labels_df = (
        df[matched]
        .groupby("X_C")
        .agg(
            cx=("x", "median"),
            cy=("y", "median"),
            n_docs=("x", "size"),
            label=("label", "first"),
        )
        .reset_index()
    )
    labels_df = labels_df[labels_df["n_docs"] >= args.label_min]
    print(
        f"[umap_scatter] Labels: {len(labels_df)} clusters "
        f"(>= {args.label_min} docs)"
    )

    # ── Scatter plot ─────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 12), dpi=150)

    # Layer 1: grey / unmatched underneath
    grey = df[df["color"] == "#d3d3d3"]
    if not grey.empty:
        ax.scatter(
            grey["x"], grey["y"],
            c="#d3d3d3", s=2, alpha=0.15, edgecolors="none",
        )

    ## Layer 2: convex hull outlines per subcluster (behind dots)
    #if is_subcluster:
    #    _draw_hulls(ax, df[matched].copy(), color_col="sub_color", cluster_col="X_C")

    # Layer 3: colored clusters on top (sub-shaded when subclusters exist)
    colored = df[df["color"] != "#d3d3d3"]
    if not colored.empty:
        ax.scatter(
            colored["x"], colored["y"],
            c=colored["sub_color"], s=3, alpha=0.5, edgecolors="none",
        )

    pad = 1.5
    xlim = (df["x"].min() - pad, df["x"].max() + pad)
    ylim = (df["y"].min() - pad, df["y"].max() + pad)
    ax.set_xlim(xlim)
    ax.set_ylim(ylim)

    _add_side_labels(
        ax, labels_df, xlim, ylim,
        fontsize=5.5, max_labels_per_side=36,
    )

    ax.set_title(args.title or "Document Embeddings - Cluster Map", fontsize=14)
    ax.axis("off")
    fig.subplots_adjust(left=0.18, right=0.82)

    # ── Save ─────────────────────────────────────────────────────────────
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"[umap_scatter] Saved: {out}")


if __name__ == "__main__":
    main()
