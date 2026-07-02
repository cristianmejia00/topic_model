# Topic Model — BERTopic Cluster Pipeline

End-to-end pipeline that retrofits BERTopic onto pre-existing hierarchical cluster
assignments (micro / meso / macro) to extract keywords, cluster embeddings, and 2D
coordinates, then renders a publication-cluster map.

---

## What each script does

### 1. `cluster_bertopic.py`

Extracts topic keywords and 2D coordinates for a three-level cluster hierarchy
(micro → meso → macro) stored in AWS Athena / S3.

**Steps performed internally:**

| Step | What happens |
|------|-------------|
| **Extract** | Queries Athena for the top-1 % most-cited papers per micro cluster (floor 5), pulling `id`, `title`, and all three cluster columns. |
| **Embed** | Encodes every representative title once with `all-MiniLM-L6-v2` (sentence-transformers). The embedding matrix is shared across all three levels. |
| **Retrofit BERTopic** | Runs a manual BERTopic fit at each level (micro, meso, macro) with predefined labels, bypassing UMAP/HDBSCAN to produce level-relative c-TF-IDF keywords and cluster-mean embeddings. |
| **Weighted rollup** *(optional)* | Re-derives meso/macro cluster vectors as a publications-weighted mean of their micro centroids so larger clusters carry proportionally more weight. |
| **2D reduction** | Fits a single UMAP reducer on micro cluster embeddings and transforms meso/macro into the same 2D space. |
| **Save** | Writes Parquet files to S3 (`{OUT_DIR}{level}/`) with columns `cluster`, `keywords`, `x_coords`, `y_coords`, plus raw `.npy` caches locally under `_bertopic_cache/`. |

**Outputs (S3):**

```
{OUT_DIR}
  documents/            # per-paper embeddings
  micro/                # cluster, keywords, x_coords, y_coords
  meso/
  macro/
  micro_embeddings/     # cluster-level embedding vectors
  meso_embeddings/
  macro_embeddings/
```

---

### 2. `topic_plot.py`

Reads the S3 outputs from `bertopic.py` and the micro cluster report, then renders a
hierarchical cluster map as a high-resolution PNG and a vector PDF.

**What is plotted:**

| Layer | What it shows |
|-------|--------------|
| **Point cloud** | Every micro cluster centroid, coloured by its macro cluster. |
| **Meso centroids** | Medium markers, sized by number of micro clusters within, same colour as their macro parent. |
| **Macro centroids** | Large markers with bold keyword labels, outlined in black. |
| **Legend** | Shown when there are ≤ 25 macro clusters. |

**Outputs (local):**

```
cluster_map.png   # rasterised at 220 DPI
cluster_map.pdf   # vector (labels remain crisp at any zoom)
```

---

## Order of execution

```
1. cluster_bertopic.py  →  produces S3 Parquet outputs (keywords + 2D coords)
2. topic_plot.py        →  reads those outputs and writes cluster_map.png / .pdf
```

---

## Setup

### Prerequisites

- Python 3.9+
- AWS credentials configured (environment variables, `~/.aws/credentials`, or IAM role)
  with read access to the source Athena database and read/write access to the S3 output
  bucket.

### Create the virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Both scripts have a `CONFIG` section near the top. At minimum, update these values in
`cluster_bertopic.py` before running:

| Variable | Description |
|----------|-------------|
| `ATHENA_DATABASE` | Athena database containing the `article_report` table. |
| `S3_STAGING` | S3 path used by Athena for query staging output. |
| `MICRO_REPORT` | S3 path to the micro cluster report Parquet (supplies publication counts). |
| `OUT_DIR` | S3 prefix where all pipeline outputs are written. |

`topic_plot.py` reads from `IN_DIR` and `MICRO_REPORT` which should match the values
set in `bertopic.py`.

---

## Running

```bash
# Activate the environment first
source .venv/bin/activate

# Step 1 — extract keywords and 2D coordinates (writes to S3)
python cluster_bertopic.py

# Step 2 — render the cluster map (writes cluster_map.png and cluster_map.pdf locally)
python topic_plot.py
```

---

## Dependencies

Key libraries (pinned versions in `requirements.txt`):

| Library | Purpose |
|---------|---------|
| `bertopic` | c-TF-IDF keyword extraction and topic modelling |
| `sentence-transformers` | MiniLM title embeddings |
| `umap-learn` | 2D dimensionality reduction |
| `scikit-learn` | `CountVectorizer` and supporting utilities |
| `awswrangler` | Athena queries and S3 Parquet I/O |
| `pandas` / `numpy` / `pyarrow` | Data wrangling and serialisation |
| `matplotlib` | Cluster map rendering |
