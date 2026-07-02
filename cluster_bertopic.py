"""
cluster_bertopic.py
===================
Retrofit a BERTopic model onto pre-existing cluster assignments (micro/meso/macro)
to extract, per cluster level:

    * c-TF-IDF keywords          (distinctive terms, level-relative)
    * cluster embeddings         (BERTopic topic embeddings = mean of member doc vecs)
    * x/y 2D coordinates         (UMAP reduction of the cluster embeddings, for plotting)

and, for the documents:

    * the MiniLM title embeddings

Design decisions (see the accompanying notes):
    - Embed the top-5 titles per micro cluster ONCE with all-MiniLM-L6-v2.
    - Retrofit BERTopic THREE times (micro, meso, macro) reusing that one embedding
      matrix. Keywords are level-relative and cannot be aggregated, but the expensive
      step (embedding) is shared, so three manual fits are cheap.
    - 2D coords are fit on the micro cluster embeddings and meso/macro are transformed
      into the same space, so all levels share one coordinate system.

Requires: bertopic, sentence-transformers, umap-learn, scikit-learn,
          numpy, pandas, pyarrow, awswrangler
"""

from __future__ import annotations
import os
import uuid
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"   # single model, by design

TABLE   = "article_report"
LEVELS  = ["micro", "meso", "macro"]
LEVEL_COL = {"micro": "micro_cluster", "meso": "meso_cluster", "macro": "macro_cluster"}

# representative selection: top TOP_PCT of a micro cluster by citations,
# floored at MIN_REPS, capped at the cluster size (take all if smaller).
#   reps = min(cluster_size, max(MIN_REPS, ceil(TOP_PCT * cluster_size)))
TOP_PCT  = 0.01                  # 1%
MIN_REPS = 5

# meso/macro embedding source:
#   True  -> publications-weighted mean of the micro centroids (explicit weight)
#   False -> BERTopic-native (mean of that level's raw docs; implicitly rep-count weighted)
# Keywords are ALWAYS from the per-level BERTopic retrofit -- unaffected either way.
WEIGHT_BY_PUBLICATIONS = True

# embedding
BATCH_SIZE = 512
DEVICE     = None                # None = auto (GPU if present)
# reuse document embeddings already saved under {OUT_DIR}documents/ instead of
# recomputing them. Set True to force a fresh embedding pass.
FORCE_EMBEDS = False

# keywords (c-TF-IDF)
TOP_N_WORDS = 10
NGRAM_RANGE = (1, 2)
MIN_DF      = 5
MAX_FEATURES = 100_000

# 2D reduction
SHARED_2D_SPACE = True           # fit on micro, transform meso/macro into same space
UMAP_NEIGHBORS  = 15
UMAP_MIN_DIST   = 0.1
UMAP_SEED       = 42             # set None for a faster (parallel) non-reproducible fit

# I/O
ATHENA_DATABASE = "q20260629"
S3_STAGING      = "s3://openalex-outputs/athena-staging/"
MICRO_REPORT    = "s3://openalex-outputs/classification/q20260629/cluster_report_micro/"  # publication weights
OUT_DIR         = "s3://openalex-outputs/classification/q20260629/bertopic/"
LOCAL_CACHE     = "./_bertopic_cache"


# ----------------------------------------------------------------------------
# 1. EXTRACT  --  adaptive top-1% (floor MIN_REPS) cited papers per micro cluster
# ----------------------------------------------------------------------------
# rn is bounded by cluster_size, so small clusters return all their rows without
# a special case. `id` breaks citation ties deterministically across reruns.
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
    import awswrangler as wr
    print(f"[extract] top {TOP_PCT:.0%} cited papers per micro cluster "
          f"(floor {MIN_REPS}, capped at cluster size) ...")
    # unload_approach requires an empty/non-existent S3 path; use a unique sub-prefix
    s3_output = S3_STAGING.rstrip("/") + "/" + uuid.uuid4().hex + "/"
    df = wr.athena.read_sql_query(
        EXTRACT_SQL,
        database=ATHENA_DATABASE,
        s3_output=s3_output,
        ctas_approach=False,
        unload_approach=True,
    )
    # normalize cluster ids to nullable ints (meso/macro are stored as double)
    for col in ("micro_cluster", "meso_cluster", "macro_cluster"):
        df[col] = df[col].astype("Int64")
    print(f"[extract] {len(df):,} papers | "
          f"{df['micro_cluster'].nunique():,} micro / "
          f"{df['meso_cluster'].nunique():,} meso / "
          f"{df['macro_cluster'].nunique():,} macro clusters")
    return df


# ----------------------------------------------------------------------------
# 2. EMBED  --  titles once, reused for all three levels
# ----------------------------------------------------------------------------
def embed_titles(titles: list[str]):
    from sentence_transformers import SentenceTransformer
    print(f"[embed] loading {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME, device=DEVICE)
    print(f"[embed] encoding {len(titles):,} titles")
    emb = model.encode(
        titles,
        batch_size=BATCH_SIZE,
        normalize_embeddings=True,
        show_progress_bar=True,
        convert_to_numpy=True,
    ).astype(np.float32)
    return model, emb


def save_document_embeddings(df: pd.DataFrame, emb: np.ndarray):
    import awswrangler as wr
    os.makedirs(LOCAL_CACHE, exist_ok=True)
    np.save(os.path.join(LOCAL_CACHE, "doc_embeddings.npy"), emb)
    np.save(os.path.join(LOCAL_CACHE, "doc_ids.npy"), df["id"].to_numpy())
    out = pd.DataFrame({"id": df["id"].values,
                        "micro_cluster": df["micro_cluster"].values,
                        "embedding": list(emb)})
    wr.s3.to_parquet(out, path=f"{OUT_DIR}documents/", dataset=True, mode="overwrite")
    print(f"[save] {len(out):,} document embeddings -> {OUT_DIR}documents/")


def load_or_compute_doc_embeddings(reps: pd.DataFrame, docs: list[str]) -> np.ndarray:
    """Reuse saved document embeddings for this OUT_DIR when present (and aligned to
    the current reps by id); otherwise embed the titles and save. FORCE_EMBEDS
    bypasses the cache. Alignment is by id, not row order, since the extraction has
    no global ORDER BY and row order can differ between runs."""
    import awswrangler as wr
    docs_path = f"{OUT_DIR}documents/"

    if not FORCE_EMBEDS:
        try:
            existing = wr.s3.list_objects(docs_path)
        except Exception:
            existing = []
        if existing:
            saved = wr.s3.read_parquet(docs_path)[["id", "embedding"]]
            emb_by_id = saved.drop_duplicates("id").set_index("id")["embedding"]
            rep_ids = reps["id"]
            if rep_ids.isin(emb_by_id.index).all():
                doc_emb = np.vstack(emb_by_id.loc[rep_ids].to_numpy()).astype(np.float32)
                print(f"[embed] reused {len(doc_emb):,} saved embeddings from {docs_path} "
                      f"(FORCE_EMBEDS=False)")
                return doc_emb
            missing = int((~rep_ids.isin(emb_by_id.index)).sum())
            print(f"[embed] saved set is missing {missing:,} of {len(rep_ids):,} current ids "
                  f"-> recomputing all")

    _, doc_emb = embed_titles(docs)
    save_document_embeddings(reps, doc_emb)
    return doc_emb


# ----------------------------------------------------------------------------
# 3. RETROFIT BERTOPIC PER LEVEL  --  keywords + cluster embeddings
# ----------------------------------------------------------------------------
def retrofit_level(docs, embeddings, labels):
    """
    Manual BERTopic: predefined labels (`labels`) become the topics. UMAP/HDBSCAN
    are bypassed; only c-TF-IDF (keywords) and topic-embedding averaging run.

    Returns: cluster_ids (np.ndarray), keywords (list[str]), cluster_vecs (np.ndarray).
    """
    from bertopic import BERTopic
    from bertopic.dimensionality import BaseDimensionalityReduction
    from bertopic.cluster import BaseCluster
    from bertopic.vectorizers import ClassTfidfTransformer
    from sklearn.feature_extraction.text import CountVectorizer

    # map real cluster ids -> contiguous codes 0..K-1 (BERTopic wants clean labels)
    codes, uniques = pd.factorize(labels, sort=True)      # no NaN -> no -1 codes

    topic_model = BERTopic(
        embedding_model=None,                             # embeddings supplied directly
        umap_model=BaseDimensionalityReduction(),         # passthrough
        hdbscan_model=BaseCluster(),                      # passthrough (uses y)
        vectorizer_model=CountVectorizer(
            stop_words="english",
            ngram_range=NGRAM_RANGE,
            min_df=MIN_DF,
            max_features=MAX_FEATURES,
        ),
        ctfidf_model=ClassTfidfTransformer(reduce_frequent_words=True),
        top_n_words=TOP_N_WORDS,
        calculate_probabilities=False,
        verbose=False,
    )
    topics, _ = topic_model.fit_transform(docs, embeddings=embeddings, y=codes)

    # CRITICAL: do NOT assume BERTopic's topic id equals our factorize code.
    # BERTopic renumbers topics internally (by frequency), so row/topic `i` is
    # generally NOT cluster uniques[i]. Recover the true real_id -> topic_id map
    # from the fit result: every doc carries its real label (`labels`) and the
    # topic BERTopic assigned it (`topics`), and that mapping is constant within
    # a cluster. Everything (keywords, vectors, ids) is then aligned by real id.
    return _align_topics_to_clusters(
        uniques, labels, topics,
        topic_model.topic_embeddings_, topic_model.get_topic,
    )


def _align_topics_to_clusters(uniques, labels, topics, topic_embeddings, get_topic_fn):
    """Map each real cluster id to its BERTopic topic, then pull that topic's
    keywords and embedding. Robust to BERTopic's internal topic renumbering."""
    topics = np.asarray(topics)
    real_to_topic = dict(zip(*pd.DataFrame({"r": list(labels), "t": topics})
                             .drop_duplicates("r").pipe(lambda d: (d["r"], d["t"]))))
    # topic_embeddings_ rows are ordered by ascending topic id (−1 first if present)
    sorted_topics = sorted(set(topics.tolist()))
    topic_to_row = {t: i for i, t in enumerate(sorted_topics)}
    emb = _l2norm(np.asarray(topic_embeddings, dtype=np.float32))

    ids, keywords, vecs = [], [], []
    for real in uniques:
        t = real_to_topic[real]
        terms = get_topic_fn(t)                           # [(word, score), ...] or False
        keywords.append(", ".join(w for w, _ in terms) if terms else "")
        vecs.append(emb[topic_to_row[t]])
        ids.append(real)
    return np.asarray(ids), keywords, np.vstack(vecs).astype(np.float32)


def _l2norm(mat: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(mat, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return (mat / n).astype(np.float32)


def weighted_pool(vecs: np.ndarray, group_keys, weights):
    """Group `vecs` by `group_keys`, take the weighted mean per group, renormalize.
    Returns (unique_keys, centroids). Each input vec is a unit micro centroid, so the
    result depends only on the weights -- rep-count variation cannot leak in."""
    codes, uniques = pd.factorize(np.asarray(group_keys), sort=True)
    weights = np.asarray(weights, dtype=np.float64)
    acc  = np.zeros((len(uniques), vecs.shape[1]), dtype=np.float64)
    wsum = np.zeros(len(uniques), dtype=np.float64)
    np.add.at(acc,  codes, vecs * weights[:, None])
    np.add.at(wsum, codes, weights)
    return uniques, _l2norm(acc / wsum[:, None])


def load_micro_weights() -> pd.DataFrame:
    """Publications per micro cluster, from the micro report, for weighted rollup."""
    import awswrangler as wr
    rep = wr.s3.read_parquet(MICRO_REPORT)
    w = rep[["micro_cluster", "publications"]].copy()
    w["micro_cluster"] = w["micro_cluster"].astype("Int64")
    return w


# ----------------------------------------------------------------------------
# 4. 2D COORDS  --  UMAP of cluster embeddings (shared space)
# ----------------------------------------------------------------------------
def build_reducer(fit_vecs):
    import umap
    n = len(fit_vecs)
    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=min(UMAP_NEIGHBORS, max(2, n - 1)),
        min_dist=UMAP_MIN_DIST,
        metric="cosine",
        random_state=UMAP_SEED,
    )
    print(f"[umap] fitting 2D reducer on {n:,} vectors")
    reducer.fit(fit_vecs)
    return reducer


def reduce_2d(reducer, vecs):
    coords = reducer.transform(vecs)
    return coords[:, 0], coords[:, 1]


# ----------------------------------------------------------------------------
# 5. SAVE
# ----------------------------------------------------------------------------
def save_level(level, cluster_ids, keywords, xs, ys, cluster_vecs):
    import awswrangler as wr
    os.makedirs(LOCAL_CACHE, exist_ok=True)

    # main deliverable: cluster, keywords, x_coords, y_coords
    df = pd.DataFrame({
        "cluster": cluster_ids,
        "keywords": keywords,
        "x_coords": xs,
        "y_coords": ys,
    })
    wr.s3.to_parquet(df, path=f"{OUT_DIR}{level}/", dataset=True, mode="overwrite")

    # cluster-level embeddings (parquet + fast npy)
    emb_df = pd.DataFrame({"cluster": cluster_ids, "embedding": list(cluster_vecs)})
    wr.s3.to_parquet(emb_df, path=f"{OUT_DIR}{level}_embeddings/", dataset=True, mode="overwrite")
    np.save(os.path.join(LOCAL_CACHE, f"{level}_vecs.npy"), cluster_vecs)
    np.save(os.path.join(LOCAL_CACHE, f"{level}_ids.npy"),  cluster_ids)

    print(f"[save] {level}: {len(cluster_ids):,} clusters -> {OUT_DIR}{level}/ (+ embeddings)")


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
def main():
    reps = load_representatives()
    docs = reps["title"].tolist()

    doc_emb = load_or_compute_doc_embeddings(reps, docs)

    # retrofit each level against the shared embedding matrix
    # (this is what produces the keywords -- always per-level, never aggregated)
    results = {}   # level -> (ids, keywords, vecs)
    for level in LEVELS:
        print(f"\n[bertopic] retrofitting level: {level}")
        ids, kws, vecs = retrofit_level(docs, doc_emb, reps[LEVEL_COL[level]].to_numpy())
        results[level] = (ids, kws, vecs)

    # optionally REPLACE the meso/macro *vectors* with a publications-weighted
    # rollup of the micro centroids. Keywords (results[level][1]) are untouched;
    # coordinates inherit the new vectors automatically since UMAP runs on them.
    if WEIGHT_BY_PUBLICATIONS:
        print("\n[rollup] rebuilding meso/macro vectors: publications-weighted micro centroids")
        micro_ids, _, micro_vecs = results["micro"]

        # align publications + hierarchy to the micro centroid order
        hierarchy = (reps.drop_duplicates("micro_cluster")
                         .set_index("micro_cluster")[["meso_cluster", "macro_cluster"]])
        rep_count = reps.groupby("micro_cluster").size().rename("rep_count")
        micro_meta = pd.DataFrame({"micro_cluster": micro_ids}).join(hierarchy, on="micro_cluster")
        micro_meta = micro_meta.merge(load_micro_weights(), on="micro_cluster", how="left")
        micro_meta = micro_meta.merge(rep_count, on="micro_cluster", how="left")
        # missing publications -> fall back to rep_count so no cluster gets zero weight
        micro_meta["publications"] = micro_meta["publications"].fillna(micro_meta["rep_count"])
        w = micro_meta["publications"].to_numpy()

        # each higher level = weighted mean of ITS micro centroids, reordered to
        # match that level's keyword id order so keywords and vectors stay aligned
        for level in ("meso", "macro"):
            ids_lvl = results[level][0]
            uids, vecs_lvl = weighted_pool(micro_vecs, micro_meta[LEVEL_COL[level]].values, w)
            vec_by_id = dict(zip(uids, vecs_lvl))
            new_vecs = np.stack([vec_by_id[c] for c in ids_lvl]).astype(np.float32)
            results[level] = (ids_lvl, results[level][1], new_vecs)

    # build the 2D reducer: shared space fit on micro, else per-level
    reducer = build_reducer(results["micro"][2]) if SHARED_2D_SPACE else None

    for level in LEVELS:
        ids, kws, vecs = results[level]
        r = reducer if SHARED_2D_SPACE else build_reducer(vecs)
        xs, ys = reduce_2d(r, vecs)
        save_level(level, ids, kws, xs, ys, vecs)

    mode = "publications-weighted" if WEIGHT_BY_PUBLICATIONS else "BERTopic-native"
    print(f"\n[done] documents + micro/meso/macro keywords, embeddings ({mode} at meso/macro), "
          f"and 2D coords written.")


if __name__ == "__main__":
    main()