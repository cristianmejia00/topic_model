"""
recompute_viz_coords.py
=======================
VISUALIZATION-ONLY recolouring fix.

Problem: cluster positions come from MiniLM *title* semantics, but the colours
(micro/meso/macro) come from *citation-graph* clustering. The two don't align,
so a single macro is sprayed across the map.

Trick: append each document's meso + macro keywords to its title, re-embed that
augmented text with MiniLM, and recompute ONLY the 2D coordinates. Documents that
share a macro now share text, so they cluster together and colour regions become
coherent.

IMPORTANT:
    * This does NOT touch cluster_bertopic.py or the original embeddings.
    * These augmented vectors are for viewing THIS dataset only. They are NOT
      comparable across datasets and must never be used for the heatmap.
    * Outputs go to a separate prefix: {IN_DIR}images/  so nothing is overwritten.

Output schema mirrors the pipeline (cluster, keywords, x_coords, y_coords), so
main_plots/plot_images.py can render it with a single change: point IN_DIR at .../images/.

Requires: sentence-transformers, umap-learn, numpy, pandas, awswrangler, pyarrow
"""

from __future__ import annotations
import os
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# CONFIG  --  must match the values used in cluster_bertopic.py
# ----------------------------------------------------------------------------
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

TABLE    = "article_report"
TOP_PCT  = 0.01                  # same adaptive selection as the pipeline
MIN_REPS = 5

# how strongly the cluster keywords pull documents together: repeat the appended
# keyword text this many times. 1 is usually enough; raise to 2-3 for tighter
# colour regions (at the cost of squashing within-cluster title structure).
KEYWORD_REPEAT = 1

BATCH_SIZE = 512
DEVICE     = None

# 2D reduction (shared space: fit on micro, transform meso/macro)
UMAP_NEIGHBORS = 15
UMAP_MIN_DIST  = 0.1
UMAP_SEED      = 42

# I/O
ATHENA_DATABASE = "q20260629"
S3_STAGING      = "s3://openalex-outputs/athena-staging/"
IN_DIR          = "s3://openalex-outputs/classification/q20260629/bertopic/"          # existing keywords
OUT_DIR         = "s3://openalex-outputs/classification/q20260629/bertopic/images/"   # viz-only assets
LOCAL_CACHE     = "./_viz_cache"

LEVEL_COL = {"micro": "micro_cluster", "meso": "meso_cluster", "macro": "macro_cluster"}


# ----------------------------------------------------------------------------
# extraction (identical to the pipeline so the representative set matches)
# ----------------------------------------------------------------------------
EXTRACT_SQL = f"""
SELECT id, micro_cluster, meso_cluster, macro_cluster, title
FROM (
    SELECT id, micro_cluster, meso_cluster, macro_cluster, title,
           ROW_NUMBER() OVER (
               PARTITION BY micro_cluster
               ORDER BY citations DESC, id
           ) AS rn,
           COUNT(*) OVER (PARTITION BY micro_cluster) AS cluster_size
    FROM {TABLE}
    WHERE micro_cluster IS NOT NULL AND title IS NOT NULL
)
WHERE rn <= GREATEST({MIN_REPS}, CAST(CEIL({TOP_PCT} * cluster_size) AS integer))
"""


def load_representatives() -> pd.DataFrame:
    import uuid
    import awswrangler as wr
    print(f"[extract] representatives (top {TOP_PCT:.0%}, floor {MIN_REPS}) ...")
    s3_output = S3_STAGING.rstrip("/") + "/" + uuid.uuid4().hex + "/"
    df = wr.athena.read_sql_query(
        EXTRACT_SQL, database=ATHENA_DATABASE, s3_output=s3_output,
        ctas_approach=False, unload_approach=True,
    )
    for col in ("micro_cluster", "meso_cluster", "macro_cluster"):
        df[col] = df[col].astype("Int64")
    print(f"[extract] {len(df):,} papers across "
          f"{df['micro_cluster'].nunique():,} micro clusters")
    return df


def load_level_keywords(level: str) -> pd.DataFrame:
    """Existing per-level keywords from the pipeline (cluster, keywords)."""
    import awswrangler as wr
    df = wr.s3.read_parquet(f"{IN_DIR}{level}/")[["cluster", "keywords"]].copy()
    df["cluster"] = df["cluster"].astype("Int64")
    df["keywords"] = df["keywords"].fillna("")
    return df


# ----------------------------------------------------------------------------
# augmentation + embedding
# ----------------------------------------------------------------------------
def build_augmented_text(reps: pd.DataFrame,
                         meso_kw: pd.DataFrame,
                         macro_kw: pd.DataFrame) -> pd.Series:
    """title + (meso keywords + macro keywords) * KEYWORD_REPEAT"""
    df = (reps.merge(meso_kw.rename(columns={"cluster": "meso_cluster",
                                             "keywords": "meso_kw"}),
                     on="meso_cluster", how="left")
              .merge(macro_kw.rename(columns={"cluster": "macro_cluster",
                                              "keywords": "macro_kw"}),
                     on="macro_cluster", how="left"))
    title    = df["title"].fillna("")
    meso_kw_ = df["meso_kw"].fillna("")
    macro_kw_ = df["macro_kw"].fillna("")
    tail = (meso_kw_ + ". " + macro_kw_ + ". ") * KEYWORD_REPEAT
    return (title + ". " + tail).str.strip()


def embed(texts):
    from sentence_transformers import SentenceTransformer
    print(f"[embed] {MODEL_NAME} on {len(texts):,} augmented documents")
    model = SentenceTransformer(MODEL_NAME, device=DEVICE)
    return model.encode(texts, batch_size=BATCH_SIZE, normalize_embeddings=True,
                        show_progress_bar=True, convert_to_numpy=True).astype(np.float32)


# ----------------------------------------------------------------------------
# pooling + reduction
# ----------------------------------------------------------------------------
def _l2norm(mat):
    n = np.linalg.norm(mat, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return (mat / n).astype(np.float32)


def group_mean(emb, labels):
    """Mean of augmented doc vectors per label (NA labels dropped). Returns
    (unique_ids:int64, centroids). Order of ids is sorted ascending."""
    s = pd.Series(list(labels))
    mask = s.notna().to_numpy()
    e = emb[mask]
    codes, uniques = pd.factorize(s[mask].to_numpy(), sort=True)
    acc = np.zeros((len(uniques), e.shape[1])); cnt = np.zeros(len(uniques))
    np.add.at(acc, codes, e); np.add.at(cnt, codes, 1)
    return np.array([int(u) for u in uniques], dtype=np.int64), _l2norm(acc / cnt[:, None])


def build_reducer(fit_vecs):
    import umap
    n = len(fit_vecs)
    print(f"[umap] fitting on {n:,} micro centroids")
    reducer = umap.UMAP(n_components=2, n_neighbors=min(UMAP_NEIGHBORS, max(2, n - 1)),
                        min_dist=UMAP_MIN_DIST, metric="cosine", random_state=UMAP_SEED)
    reducer.fit(fit_vecs)
    return reducer


# ----------------------------------------------------------------------------
# save
# ----------------------------------------------------------------------------
def save_level(level, ids, keywords_df, xs, ys, vecs):
    import awswrangler as wr
    os.makedirs(LOCAL_CACHE, exist_ok=True)

    coords = pd.DataFrame({"cluster": ids, "x_coords": xs, "y_coords": ys})
    coords = coords.merge(keywords_df, on="cluster", how="left")
    coords["keywords"] = coords["keywords"].fillna("")
    coords = coords[["cluster", "keywords", "x_coords", "y_coords"]]      # pipeline schema
    wr.s3.to_parquet(coords, path=f"{OUT_DIR}{level}/", dataset=True, mode="overwrite")

    emb_df = pd.DataFrame({"cluster": ids, "embedding": list(vecs)})
    wr.s3.to_parquet(emb_df, path=f"{OUT_DIR}{level}_embeddings/", dataset=True, mode="overwrite")
    np.save(os.path.join(LOCAL_CACHE, f"{level}_vecs.npy"), vecs)

    print(f"[save] {level}: {len(ids):,} clusters -> {OUT_DIR}{level}/ (viz-only)")


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main():
    reps = load_representatives()
    meso_kw  = load_level_keywords("meso")
    macro_kw = load_level_keywords("macro")

    texts = build_augmented_text(reps, meso_kw, macro_kw).tolist()
    doc_emb = embed(texts)

    # centroids per level, straight from the augmented doc vectors
    centroids = {lvl: group_mean(doc_emb, reps[LEVEL_COL[lvl]])
                 for lvl in ("micro", "meso", "macro")}

    # shared 2D space: fit on micro, transform meso/macro
    reducer = build_reducer(centroids["micro"][1])

    keywords = {"micro": load_level_keywords("micro"), "meso": meso_kw, "macro": macro_kw}
    for level in ("micro", "meso", "macro"):
        ids, vecs = centroids[level]
        coords = reducer.transform(vecs)
        save_level(level, ids, keywords[level], coords[:, 0], coords[:, 1], vecs)

    print(f"\n[done] viz-only coordinates written under {OUT_DIR}")
    print(f"       to render: set IN_DIR = \"{OUT_DIR}\" in main_plots/plot_images.py "
          f"(keep MICRO_REPORT unchanged) and re-run it.")


if __name__ == "__main__":
    main()