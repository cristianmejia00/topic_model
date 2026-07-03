"""
plot_clusters.py
================
Render the hierarchical cluster map from the BERTopic pipeline outputs:

    * micro clusters as a point cloud, colored by their macro cluster
    * meso centroids as medium markers
    * macro centroids as large markers, labeled with their top keywords

All three levels share one 2D coordinate system (the pipeline fits UMAP on micro
and transforms meso/macro into the same space), so they overlay directly.

Inputs (from cluster_bertopic.py):
    {IN_DIR}micro/  meso/  macro/   -> columns: cluster, keywords, x_coords, y_coords
    MICRO_REPORT                     -> micro_cluster, meso_cluster, macro_cluster
                                        (supplies the micro->macro coloring + sizes)

Outputs: a high-res PNG (rasterized points) and a PDF (vector labels).

Requires: matplotlib, pandas, numpy, awswrangler, pyarrow
"""

from __future__ import annotations
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from macro_palette import color_for_macro, load_macro_color_map

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
IN_DIR       = "s3://openalex-outputs/classification/q20260629/bertopic/images/"
MICRO_REPORT = "s3://openalex-outputs/classification/q20260629/cluster_report_micro/"

OUT_PNG = "cluster_map.png"
OUT_PDF = "cluster_map.pdf"
DPI     = 220

# micro point cloud
POINT_SIZE  = 2.0
POINT_ALPHA = 0.35

# labels
LABEL_KEYWORDS_N = 3          # terms shown per centroid label
LABEL_MACRO      = True
LABEL_MESO_TOP   = 0          # label the N largest meso clusters (0 = none)
LEGEND_MAX       = 25         # show a macro legend only if there are <= this many

# marker sizing (by number of micro clusters underneath)
MESO_SIZE_RANGE  = (10, 90)
MACRO_SIZE_RANGE = (90, 520)

FIG_SIZE = (16, 12)
TITLE    = "Publication cluster map — micro coloured by macro"


# ----------------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------------
def load_palette(keys):
    palette = {}
    color_map = load_macro_color_map()
    for k in keys:
        palette[k] = color_for_macro(int(k), color_map)
    return palette


def short_keywords(s, n=LABEL_KEYWORDS_N):
    if not isinstance(s, str) or not s:
        return ""
    return ", ".join(s.split(", ")[:n])


def scale_sizes(counts, lo, hi):
    """sqrt-scale raw counts into a [lo, hi] marker-area range."""
    c = np.sqrt(np.asarray(counts, dtype=float))
    if c.max() == c.min():
        return np.full_like(c, (lo + hi) / 2)
    return lo + (hi - lo) * (c - c.min()) / (c.max() - c.min())


# ----------------------------------------------------------------------------
# PLOT  (pure function of prepared dataframes -> easy to test)
# ----------------------------------------------------------------------------
def build_plot(micro, meso, macro, palette, out_png=OUT_PNG, out_pdf=OUT_PDF):
    """
    micro : columns [x, y, macro]
    meso  : columns [x, y, macro, size, keywords]
    macro : columns [x, y, macro, size, keywords]
    """
    macro_ids = sorted(macro["macro"].dropna().unique())

    fig, ax = plt.subplots(figsize=FIG_SIZE)

    # --- micro point cloud (rasterized so the file stays small) -------------
    m = micro.dropna(subset=["macro"])
    micro_colors = np.array([palette[k] for k in m["macro"]])
    ax.scatter(m["x"], m["y"], s=POINT_SIZE, c=micro_colors,
               alpha=POINT_ALPHA, linewidths=0, rasterized=True, zorder=1)

    # --- meso centroids -----------------------------------------------------
    if len(meso):
        meso_colors = np.array([palette.get(k, (0.5, 0.5, 0.5)) for k in meso["macro"]])
        meso_sz = scale_sizes(meso["size"], *MESO_SIZE_RANGE)
        ax.scatter(meso["x"], meso["y"], s=meso_sz, c=meso_colors,
                   edgecolors="white", linewidths=0.4, alpha=0.9, zorder=3)

    # --- macro centroids (labeled) -----------------------------------------
    macro_colors = np.array([palette[k] for k in macro["macro"]])
    macro_sz = scale_sizes(macro["size"], *MACRO_SIZE_RANGE)
    ax.scatter(macro["x"], macro["y"], s=macro_sz, c=macro_colors,
               edgecolors="black", linewidths=1.0, zorder=5)

    # --- labels -------------------------------------------------------------
    bbox = dict(boxstyle="round,pad=0.22", fc="white", ec="none", alpha=0.72)
    if LABEL_MACRO:
        for _, r in macro.iterrows():
            label = short_keywords(r["keywords"])
            if label:
                ax.annotate(label, (r["x"], r["y"]), fontsize=8.5, weight="bold",
                            ha="center", va="center", zorder=6, bbox=bbox)

    if LABEL_MESO_TOP > 0 and len(meso):
        for _, r in meso.nlargest(LABEL_MESO_TOP, "size").iterrows():
            label = short_keywords(r["keywords"])
            if label:
                ax.annotate(label, (r["x"], r["y"]), fontsize=6.5, color="#333333",
                            ha="center", va="center", zorder=4,
                            bbox=dict(boxstyle="round,pad=0.15", fc="white",
                                      ec="none", alpha=0.55))

    # --- legend (only when macro count is manageable) ----------------------
    if len(macro_ids) <= LEGEND_MAX:
        kw_by_macro = dict(zip(macro["macro"], macro["keywords"]))
        handles = [
            Line2D([0], [0], marker="o", linestyle="", markersize=8,
                   markerfacecolor=palette[k], markeredgecolor="black",
                   label=f"{k}: {short_keywords(kw_by_macro.get(k, ''), 2)}")
            for k in macro_ids
        ]
        ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.01, 0.5),
                  fontsize=8, frameon=False, title="Macro clusters")

    # --- cosmetics ----------------------------------------------------------
    ax.set_aspect("equal")
    ax.set_xticks([]); ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_title(TITLE, fontsize=15, pad=12)
    caption = (f"{len(micro):,} micro · {len(meso):,} meso · {len(macro):,} macro clusters")
    ax.text(0.5, -0.02, caption, transform=ax.transAxes, ha="center",
            va="top", fontsize=9, color="#666666")

    fig.savefig(out_png, dpi=DPI, bbox_inches="tight")
    fig.savefig(out_pdf, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] wrote {out_png} and {out_pdf}")


# ----------------------------------------------------------------------------
# DATA PREP  (join coords to the hierarchy, derive sizes)
# ----------------------------------------------------------------------------
def prepare_frames():
    import awswrangler as wr

    def coords(level):
        df = wr.s3.read_parquet(f"{IN_DIR}{level}/")
        return df.rename(columns={"x_coords": "x", "y_coords": "y"})

    micro_c = coords("micro")   # cluster = micro id
    meso_c  = coords("meso")    # cluster = meso id
    macro_c = coords("macro")   # cluster = macro id

    rep = wr.s3.read_parquet(MICRO_REPORT)[["micro_cluster", "meso_cluster", "macro_cluster"]]
    for col in rep.columns:
        rep[col] = rep[col].astype("Int64")

    # micro -> macro (coloring)
    micro = micro_c.merge(
        rep[["micro_cluster", "macro_cluster"]],
        left_on="cluster", right_on="micro_cluster", how="left",
    )
    micro_plot = micro[["x", "y"]].assign(macro=micro["macro_cluster"])

    # meso -> macro (color) + size (micros per meso)
    meso_macro = rep[["meso_cluster", "macro_cluster"]].drop_duplicates()
    meso_size  = rep.groupby("meso_cluster")["micro_cluster"].nunique().rename("size")
    meso = (meso_c.merge(meso_macro, left_on="cluster", right_on="meso_cluster", how="left")
                  .merge(meso_size, left_on="cluster", right_index=True, how="left"))
    meso_plot = meso[["x", "y", "keywords"]].assign(
        macro=meso["macro_cluster"], size=meso["size"].fillna(1))

    # macro size (micros per macro)
    macro_size = rep.groupby("macro_cluster")["micro_cluster"].nunique().rename("size")
    macro = macro_c.merge(macro_size, left_on="cluster", right_index=True, how="left")
    macro_plot = macro[["x", "y", "keywords"]].assign(
        macro=macro["cluster"].astype("Int64"), size=macro["size"].fillna(1))

    return micro_plot, meso_plot, macro_plot


def main():
    micro, meso, macro = prepare_frames()
    macro_ids = sorted(macro["macro"].dropna().unique())
    palette = load_palette(macro_ids)
    build_plot(micro, meso, macro, palette)


if __name__ == "__main__":
    main()