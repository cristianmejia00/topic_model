"""
subquery_search.py
==================
Find the micro clusters that match a free-text query (a word, phrase, or a whole
paragraph), then export focused subsets of the dataset to S3 for that query.

Matching:
    query --MiniLM--> vector, cosine against the ORIGINAL title-based micro
    centroids (bertopic/micro_embeddings/), keep those >= THRESHOLD, then keep
    only clusters with >= MIN_SIZE papers.

    NB: search uses the title embeddings, NOT the images/ augmented vectors --
    those encode cluster identity and would match on the wrong signal.

Outputs (under subqueries/{QUERY_FOLDER}/):
    matches/                 matched micro clusters + similarity + size (summary)
    article_top10/           top-10 cited papers per matched micro cluster
    cluster_report_micro/    micro report rows for the matched clusters
    cluster_report_meso/     meso report rows for the parent mesos
    cluster_report_macro/    macro report rows for the parent macros
    top_countries/           top-20 countries per matched micro cluster
    top_institutions/        top-20 institutions per matched micro cluster

Requires: sentence-transformers, numpy, pandas, awswrangler, pyarrow
"""

from __future__ import annotations
import sys
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# QUERY PARAMETERS
# ----------------------------------------------------------------------------
QUERY_FOLDER = "quantum_computing"          # S3 subfolder name for this query
QUERY_TEXT   = "quantum_computing: Quantum computing architectures, superconducting transmons and trapped ion qubits, fault-tolerant quantum error correction (QEC) surface codes, NISQ algorithms like VQE and QAOA, quantum superposition, entanglement, and software SDKs like Qiskit and Cirq."          # may be a full paragraph
THRESHOLD    = 0.50                         # min cosine similarity to keep
MIN_SIZE     = 30                           # min papers per micro cluster

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"   # must match the pipeline

DATABASE  = "q20260629"
S3_STAGING = "s3://openalex-outputs/athena-staging/"
MICRO_EMBEDDINGS = "s3://openalex-outputs/classification/q20260629/bertopic/micro_embeddings/"
OUT_ROOT   = "s3://openalex-outputs/classification/q20260629/subqueries/"

TOP_PAPERS   = 10        # top cited papers per cluster
TOP_ENTITIES = 20        # top countries / institutions per cluster

OUT_BASE = f"{OUT_ROOT}{QUERY_FOLDER}/"


# ----------------------------------------------------------------------------
# 1. embed the query and score the micro centroids
# ----------------------------------------------------------------------------
def embed_query(text: str) -> np.ndarray:
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(MODEL_NAME)
    v = model.encode([text], normalize_embeddings=True, convert_to_numpy=True)[0]
    return v.astype(np.float32)


def load_micro_centroids():
    import awswrangler as wr
    print("[load] micro centroids (title-based) ...")
    df = wr.s3.read_parquet(MICRO_EMBEDDINGS)
    ids = df["cluster"].astype("int64").to_numpy()
    mat = np.vstack(df["embedding"].to_numpy()).astype(np.float32)   # already unit-norm
    return ids, mat


def score_and_filter(qvec, ids, mat) -> pd.DataFrame:
    sims = mat @ qvec                                               # cosine (unit vectors)
    order = np.argsort(-sims)
    # always show the top of the distribution so THRESHOLD can be calibrated
    print("\n[calibrate] highest similarities to the query:")
    for i in order[:10]:
        print(f"    micro {int(ids[i]):>10}   cos = {sims[i]:.3f}")

    keep = sims >= THRESHOLD
    print(f"\n[filter] {keep.sum():,} micro clusters with cosine >= {THRESHOLD}")
    return pd.DataFrame({"micro_cluster": ids[keep].astype("int64"),
                         "similarity": sims[keep]}).sort_values("similarity", ascending=False)


# ----------------------------------------------------------------------------
# helpers for pushing an id set into Athena
# ----------------------------------------------------------------------------
def _in_clause(ids) -> str:
    return ", ".join(str(int(x)) for x in ids)


def run_sql(sql: str) -> pd.DataFrame:
    import awswrangler as wr
    return wr.athena.read_sql_query(sql, database=DATABASE, s3_output=S3_STAGING,
                                    ctas_approach=False)


def write(df: pd.DataFrame, name: str):
    import awswrangler as wr
    wr.s3.to_parquet(df, path=f"{OUT_BASE}{name}/", dataset=True, mode="overwrite")
    print(f"[save] {name}: {len(df):,} rows -> {OUT_BASE}{name}/")


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main():
    qvec = embed_query(QUERY_TEXT)
    ids, mat = load_micro_centroids()
    matches = score_and_filter(qvec, ids, mat)

    if matches.empty:
        print(f"\n[stop] no micro clusters reached cosine {THRESHOLD}. "
              f"MiniLM similarities are often well below 0.9 -- try a lower THRESHOLD "
              f"(the calibration list above shows the achievable range).")
        sys.exit(0)

    candidate_ids = matches["micro_cluster"].tolist()

    # --- micro report for candidates, then apply MIN_SIZE -------------------
    micro_rep = run_sql(
        f"SELECT * FROM cluster_report_micro WHERE micro_cluster IN ({_in_clause(candidate_ids)})"
    )
    micro_rep = micro_rep[micro_rep["publications"] >= MIN_SIZE]
    if micro_rep.empty:
        print(f"\n[stop] matches found, but none has >= {MIN_SIZE} papers.")
        sys.exit(0)

    final_micro = micro_rep["micro_cluster"].astype("int64").tolist()
    meso_ids    = micro_rep["meso_cluster"].dropna().astype("int64").unique().tolist()
    macro_ids   = micro_rep["macro_cluster"].dropna().astype("int64").unique().tolist()
    print(f"\n[final] {len(final_micro):,} micro / {len(meso_ids):,} meso / "
          f"{len(macro_ids):,} macro clusters after MIN_SIZE={MIN_SIZE}")

    micro_in = _in_clause(final_micro)

    # --- (summary) matched clusters with similarity + size ------------------
    summary = (matches.merge(micro_rep[["micro_cluster", "publications"]],
                             on="micro_cluster", how="inner")
                      .sort_values("similarity", ascending=False))
    write(summary, "matches")

    # --- article_report: top-10 cited papers per matched micro -------------
    write(run_sql(f"""
        SELECT id, title, citations, countries, institutions,
               micro_cluster, meso_cluster, macro_cluster, publication_year
        FROM (
            SELECT id, title, citations, countries, institutions,
                   micro_cluster, meso_cluster, macro_cluster, publication_year,
                   ROW_NUMBER() OVER (PARTITION BY micro_cluster
                                      ORDER BY citations DESC, id) AS rn
            FROM article_report
            WHERE micro_cluster IN ({micro_in})
        )
        WHERE rn <= {TOP_PAPERS}
    """), "article_top10")

    # --- report subsets: micro (local), meso, macro ------------------------
    write(micro_rep, "cluster_report_micro")
    write(run_sql(
        f"SELECT * FROM cluster_report_meso WHERE meso_cluster IN ({_in_clause(meso_ids)})"
    ), "cluster_report_meso")
    write(run_sql(
        f"SELECT * FROM cluster_report_macro WHERE macro_cluster IN ({_in_clause(macro_ids)})"
    ), "cluster_report_macro")

    # --- top-20 countries per micro ----------------------------------------
    write(run_sql(f"""
        SELECT micro_cluster, country, freq FROM (
            SELECT micro_cluster, country, COUNT(*) AS freq,
                   ROW_NUMBER() OVER (PARTITION BY micro_cluster
                                      ORDER BY COUNT(*) DESC, country) AS rn
            FROM article_report
            CROSS JOIN UNNEST(countries) AS t(country)
            WHERE micro_cluster IN ({micro_in})
            GROUP BY micro_cluster, country
        )
        WHERE rn <= {TOP_ENTITIES}
    """), "top_countries")

    # --- top-20 institutions per micro -------------------------------------
    write(run_sql(f"""
        SELECT micro_cluster, institution, freq FROM (
            SELECT micro_cluster, institution, COUNT(*) AS freq,
                   ROW_NUMBER() OVER (PARTITION BY micro_cluster
                                      ORDER BY COUNT(*) DESC, institution) AS rn
            FROM article_report
            CROSS JOIN UNNEST(institutions) AS t(institution)
            WHERE micro_cluster IN ({micro_in})
            GROUP BY micro_cluster, institution
        )
        WHERE rn <= {TOP_ENTITIES}
    """), "top_institutions")

    print(f"\n[done] all subsets written under {OUT_BASE}")


if __name__ == "__main__":
    main()