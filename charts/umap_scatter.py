"""Render macro-level UMAP scatter from enriched embedding outputs.

Input:
- s3://.../subqueries/{SUBQUERY}/charts/enriched_embeds/embeddings.npy
- s3://.../subqueries/{SUBQUERY}/charts/enriched_embeds/embeddings_ids.json
- s3://.../subqueries/{SUBQUERY}/charts/enriched_embeds/sampled_records/

Output:
- s3://.../subqueries/{SUBQUERY}/charts/fig_umap_scatter.png

UMAP coordinates are cached at:
- s3://.../subqueries/{SUBQUERY}/charts/enriched_embeds/umap_2d_coords.csv
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from dataclasses import dataclass

import awswrangler as wr
import boto3
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from umap import UMAP  # noqa: E402


SNAPSHOT_ENV_VAR = "TOPIC_MODEL_SNAPSHOT"
QUERY_ENV_VAR = "TOPIC_MODEL_QUERY"
SUBQUERY_ENV_VAR = "TOPIC_MODEL_SUBQUERY"
LEGACY_SUBQUERY_ENV_VAR = "TOPIC_MODEL_QUERY_FOLDER"

DEFAULT_SEED = 100
DEFAULT_LABEL_MIN = 20
DEFAULT_MAX_LABELS = 70


@dataclass(frozen=True)
class ChartPaths:
    snapshot: str
    query: str
    subquery: str

    @property
    def results_root(self) -> str:
        return f"s3://openalex-results/snapshot_{self.snapshot}/queries/{self.query}/"

    @property
    def clustering_root(self) -> str:
        return f"{self.results_root}network/clustering/"

    @property
    def subquery_root(self) -> str:
        return f"{self.clustering_root}subqueries/{self.subquery}/"

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
    parser = argparse.ArgumentParser(description="Generate macro-level UMAP scatter for one subquery.")
    parser.add_argument("--snapshot", default=None, help="Snapshot token, e.g. 2026-06-26.")
    parser.add_argument("--query", default=None, help="Query token, e.g. q20260629.")
    parser.add_argument("--subquery", default=None, help="Subquery folder token.")
    parser.add_argument("--query-folder", default=None, help="Deprecated alias for --subquery.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="UMAP random seed.")
    parser.add_argument("--label-min", type=int, default=DEFAULT_LABEL_MIN, help="Minimum docs per macro to display side labels.")
    parser.add_argument("--max-labels", type=int, default=DEFAULT_MAX_LABELS, help="Hard cap for macro labels shown in the chart.")
    parser.add_argument("--title", default="", help="Optional chart title override.")
    parser.add_argument("--force", action="store_true", help="Recompute UMAP coordinates even when cache exists.")
    return parser.parse_args()


def parse_s3_uri(path: str) -> tuple[str, str]:
    if not path.startswith("s3://"):
        raise ValueError(f"Expected s3:// path, got: {path}")
    body = path[5:]
    parts = body.split("/", 1)
    bucket = parts[0]
    key = "" if len(parts) == 1 else parts[1]
    return bucket, key


def read_json_s3(path: str) -> object:
    bucket, key = parse_s3_uri(path)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))


def load_npy_s3(path: str) -> np.ndarray:
    bucket, key = parse_s3_uri(path)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return np.load(io.BytesIO(body))


def put_png_s3(path: str, fig: plt.Figure) -> None:
    bucket, key = parse_s3_uri(path)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="image/png")


def load_build_settings(path: str) -> dict[str, object]:
    if not wr.s3.does_object_exist(path):
        return {}
    payload = read_json_s3(path)
    if isinstance(payload, dict):
        return payload
    return {}


def load_subquery_counts(paths: ChartPaths) -> tuple[int | None, int | None]:
    micro_path = f"{paths.subquery_root}cluster_report_micro/"
    macro_path = f"{paths.subquery_root}cluster_report_macro/"

    micro_count: int | None = None
    macro_count: int | None = None

    try:
        micro_df = wr.s3.read_parquet(micro_path)
        if "micro_cluster" in micro_df.columns:
            micro_count = int(pd.to_numeric(micro_df["micro_cluster"], errors="coerce").dropna().astype("int64").nunique())
    except Exception:
        micro_count = None

    try:
        macro_df = wr.s3.read_parquet(macro_path)
        if "macro_cluster" in macro_df.columns:
            macro_count = int(pd.to_numeric(macro_df["macro_cluster"], errors="coerce").dropna().astype("int64").nunique())
    except Exception:
        macro_count = None

    return macro_count, micro_count


def _spread_targets(y_values: np.ndarray, y_min: float, y_max: float, min_gap: float) -> np.ndarray:
    if len(y_values) == 0:
        return np.array([], dtype=float)
    ys = np.clip(np.array(y_values, dtype=float), y_min, y_max)

    for idx in range(1, len(ys)):
        if ys[idx - 1] - ys[idx] < min_gap:
            ys[idx] = ys[idx - 1] - min_gap

    if ys[-1] < y_min:
        ys += y_min - ys[-1]
        for idx in range(len(ys) - 2, -1, -1):
            if ys[idx] - ys[idx + 1] < min_gap:
                ys[idx] = ys[idx + 1] + min_gap

    if ys[0] > y_max:
        ys -= ys[0] - y_max

    return np.clip(ys, y_min, y_max)


def _pick_labels_for_side(sub: pd.DataFrame, y_min: float, y_max: float, min_gap: float, max_labels: int | None) -> pd.DataFrame:
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
        rest = work.loc[~work.index.isin(used)].sort_values("n_docs", ascending=False).head(target_n - len(primary))
        chosen = pd.concat([primary, rest], axis=0)
    else:
        chosen = primary

    return chosen.sort_values("cy", ascending=False).head(target_n).reset_index(drop=True)


def _add_side_labels(
    ax: plt.Axes,
    labels_df: pd.DataFrame,
    xlim: tuple[float, float],
    ylim: tuple[float, float],
    *,
    min_gap_frac: float = 0.024,
    text_pad_frac: float = 0.10,
    elbow_frac: float = 0.04,
    fontsize: float = 5.5,
    max_labels_per_side: int | None = 36,
    max_chars: int = 40,
) -> int:
    if labels_df.empty:
        return 0

    x_min, x_max = xlim
    y_min, y_max = ylim
    x_span = x_max - x_min
    y_span = y_max - y_min

    x_left = x_min - text_pad_frac * x_span
    x_right = x_max + text_pad_frac * x_span
    x_elbow_l = x_min - elbow_frac * x_span
    x_elbow_r = x_max + elbow_frac * x_span
    min_gap = min_gap_frac * y_span
    center_x = 0.5 * (x_min + x_max)

    # Keep label targets away from the exact plot bounds so text boxes are not
    # pushed outside the printable area at the bottom/top edges.
    y_pad = 0.03 * y_span
    label_y_min = y_min + y_pad
    label_y_max = y_max - y_pad
    if label_y_min >= label_y_max:
        label_y_min, label_y_max = y_min, y_max

    labels = labels_df.copy()
    labels["side"] = np.where(labels["cx"] <= center_x, "left", "right")
    drawn = 0

    for side in ("left", "right"):
        sub = labels[labels["side"] == side].copy()
        if sub.empty:
            continue

        # Dynamically relax vertical spacing to avoid dropping labels on dense sides.
        # This keeps the global label cap deterministic while maximizing visible labels.
        desired_n = len(sub) if max_labels_per_side is None else min(len(sub), max_labels_per_side)
        if desired_n <= 1:
            side_gap = min_gap
        else:
            side_gap = min(min_gap, (label_y_max - label_y_min) / float(desired_n - 1) * 0.9)

        sub = _pick_labels_for_side(sub, label_y_min, label_y_max, side_gap, max_labels_per_side)
        sub = sub.sort_values("cy", ascending=False).reset_index(drop=True)
        sub["target_y"] = _spread_targets(sub["cy"].to_numpy(), label_y_min, label_y_max, side_gap)
        drawn += len(sub)

        n_sub = len(sub)
        for pos, (_, row) in enumerate(sub.iterrows()):
            if side == "left":
                x_txt = x_left
                x_elb = x_elbow_l - 0.004 * x_span * (pos - (n_sub - 1) / 2)
                align = "right"
            else:
                x_txt = x_right
                x_elb = x_elbow_r + 0.004 * x_span * (pos - (n_sub - 1) / 2)
                align = "left"

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
                ha=align,
                va="center",
                color="black",
                bbox={"boxstyle": "round,pad=0.12", "fc": "white", "alpha": 0.85, "ec": "none"},
                zorder=4,
                clip_on=False,
            )

    return drawn


def ensure_display_ids(df: pd.DataFrame) -> pd.DataFrame:
    if "display_id" in df.columns and df["display_id"].notna().any():
        df["display_id"] = pd.to_numeric(df["display_id"], errors="coerce")
        return df

    counts = (
        df.groupby("macro_cluster", as_index=False)
        .size()
        .rename(columns={"size": "n_docs"})
        .sort_values(["n_docs", "macro_cluster"], ascending=[False, True])
        .reset_index(drop=True)
    )
    counts["display_id"] = np.arange(1, len(counts) + 1, dtype=np.int64)
    return df.merge(counts[["macro_cluster", "display_id"]], on="macro_cluster", how="left")


def main() -> None:
    args = parse_args()

    snapshot = resolve_snapshot(args.snapshot)
    query = resolve_query(args.query)
    subquery = resolve_subquery(args.subquery, args.query_folder)
    paths = ChartPaths(snapshot=snapshot, query=query, subquery=subquery)

    emb_path = f"{paths.enriched_root}embeddings.npy"
    ids_path = f"{paths.enriched_root}embeddings_ids.json"
    sampled_path = f"{paths.enriched_root}sampled_records/"
    settings_path = f"{paths.enriched_root}build_settings.json"
    cache_path = f"{paths.enriched_root}umap_2d_coords.csv"
    output_path = f"{paths.charts_root}fig_umap_scatter.png"

    print("[config] snapshot:", snapshot)
    print("[config] query:", query)
    print("[config] subquery:", subquery)
    print("[config] embeddings:", emb_path)
    print("[config] sampled_records:", sampled_path)
    print("[config] output:", output_path)

    if not wr.s3.does_object_exist(emb_path):
        raise RuntimeError(f"Missing embeddings file: {emb_path}")
    if not wr.s3.does_object_exist(ids_path):
        raise RuntimeError(f"Missing embeddings IDs file: {ids_path}")

    embeddings = load_npy_s3(emb_path)
    emb_ids = read_json_s3(ids_path)
    if not isinstance(emb_ids, list):
        raise RuntimeError("embeddings_ids.json must be a JSON list.")

    print(f"[load] embeddings shape: {embeddings.shape}")

    sampled = wr.s3.read_parquet(sampled_path)
    if sampled.empty:
        raise RuntimeError("sampled_records dataset is empty.")
    if "paper_id" not in sampled.columns or "macro_cluster" not in sampled.columns:
        raise RuntimeError("sampled_records must include paper_id and macro_cluster columns.")

    sampled["paper_id"] = sampled["paper_id"].astype(str)
    sampled["macro_cluster"] = pd.to_numeric(sampled["macro_cluster"], errors="coerce")
    sampled = sampled.dropna(subset=["macro_cluster"]).copy()
    sampled["macro_cluster"] = sampled["macro_cluster"].astype("int64")

    sampled = ensure_display_ids(sampled)
    sampled["display_id"] = pd.to_numeric(sampled["display_id"], errors="coerce").fillna(0).astype("int64")

    if "macro_name" not in sampled.columns:
        sampled["macro_name"] = sampled["macro_cluster"].map(lambda x: f"Macro {int(x)}")
    sampled["macro_name"] = sampled["macro_name"].astype(str).str.strip()

    if "color_hex" not in sampled.columns:
        sampled["color_hex"] = "#6c757d"
    sampled["color_hex"] = sampled["color_hex"].astype(str).str.strip().replace("", "#6c757d")

    if len(emb_ids) != embeddings.shape[0]:
        raise RuntimeError("embeddings.npy row count does not match embeddings_ids.json length.")

    settings = load_build_settings(settings_path)
    macro_count_subquery, micro_count_subquery = load_subquery_counts(paths)

    doc_count_pre_sample = int(settings.get("pre_sample_docs") or settings.get("candidate_docs") or len(sampled))
    macro_count_for_subtitle = int(
        settings.get("candidate_macro_clusters")
        or (macro_count_subquery if macro_count_subquery is not None else sampled["macro_cluster"].nunique())
    )
    micro_count_for_subtitle = int(
        settings.get("candidate_micro_clusters")
        or settings.get("matched_micro_clusters")
        or (micro_count_subquery if micro_count_subquery is not None else 0)
    )

    coords_df: pd.DataFrame | None = None
    if wr.s3.does_object_exist(cache_path) and not args.force:
        cache_df = wr.s3.read_csv(cache_path, dtype={"paper_id": str})
        if len(cache_df) == len(emb_ids):
            coords_df = cache_df
            print(f"[load] using cached UMAP coordinates: {cache_path}")
        else:
            print("[warn] cached UMAP size mismatch, recomputing.")

    if coords_df is None:
        print(f"[umap] computing UMAP for {len(emb_ids):,} records (seed={args.seed})")
        xy = UMAP(
            n_components=2,
            n_neighbors=15,
            min_dist=0.1,
            metric="cosine",
            random_state=args.seed,
        ).fit_transform(embeddings)

        coords_df = pd.DataFrame({"paper_id": emb_ids, "x": xy[:, 0], "y": xy[:, 1]})
        wr.s3.to_csv(coords_df, path=cache_path, index=False)
        print(f"[save] cached UMAP coordinates: {cache_path}")

    df = sampled.merge(coords_df, on="paper_id", how="inner")
    if df.empty:
        raise RuntimeError("No overlap between sampled_records and UMAP coordinates.")

    labels_df = (
        df.groupby(["macro_cluster", "display_id", "macro_name", "color_hex"], as_index=False)
        .agg(
            cx=("x", "median"),
            cy=("y", "median"),
            n_docs=("paper_id", "size"),
        )
        .sort_values(["n_docs", "display_id"], ascending=[False, True])
        .reset_index(drop=True)
    )
    labels_df["label"] = labels_df.apply(lambda r: f"{int(r['display_id'])}. {r['macro_name']}", axis=1)
    labels_df = labels_df[labels_df["n_docs"] >= int(args.label_min)].copy()
    labels_df = labels_df.sort_values(["n_docs", "display_id"], ascending=[False, True]).head(int(args.max_labels)).copy()

    fig, ax = plt.subplots(figsize=(12, 12), dpi=150)
    ax.scatter(
        df["x"],
        df["y"],
        c=df["color_hex"],
        s=3,
        alpha=0.45,
        edgecolors="none",
        zorder=2,
    )

    pad = 1.5
    xlim = (float(df["x"].min()) - pad, float(df["x"].max()) + pad)
    ylim = (float(df["y"].min()) - pad, float(df["y"].max()) + pad)
    ax.set_xlim(xlim)
    ax.set_ylim(ylim)

    labels_drawn = _add_side_labels(ax, labels_df, xlim, ylim, fontsize=5.5, max_labels_per_side=None)

    title_text = args.title or "Document Embeddings - Macro Cluster Map"
    fig.suptitle(title_text, fontsize=15, y=0.992)
    subtitle = (
        f"{snapshot} data; {doc_count_pre_sample:,} documents; "
        f"{macro_count_for_subtitle:,} macro clusters; {micro_count_for_subtitle:,} micro clusters"
    )
    fig.text(0.5, 0.965, subtitle, ha="center", va="center", fontsize=10, color="0.35")
    ax.axis("off")
    fig.subplots_adjust(left=0.18, right=0.82, top=0.90)

    put_png_s3(output_path, fig)
    plt.close(fig)

    print(f"[done] plotted records: {len(df):,}")
    print(
        f"[done] labeled macros: {labels_drawn:,} "
        f"(selected={len(labels_df):,}, label_min={args.label_min}, max_labels={args.max_labels})"
    )
    print(f"[done] saved: {output_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
