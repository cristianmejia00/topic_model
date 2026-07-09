# Charts Pipeline (Subquery Macro UMAP)

This folder contains the two scripts that generate the macro-level UMAP chart for a subquery.

## What Each Script Does

### 1) `build_enriched_embeddings.py`

Builds enriched text embeddings for papers matched by a subquery.

Main steps:
- Reads matched micro clusters from:
  - `.../subqueries/{SUBQUERY}/cluster_report_micro/`
- Pulls candidate papers from query-level `article_report` for those micro clusters.
- Joins macro metadata from:
  - `cluster_name_macro/`
  - `cluster_color_macro/`
  - `cluster_report_macro/`
- Builds enriched text by prepending macro name to paper text:
  - `"{macro_name}. {title} {abstract}"`
  - Falls back to title-only if abstract is unavailable.
- Assigns macro `display_id` where `1` is the largest macro in the matched set.
- Applies proportional macro-aware sampling with guaranteed macro representation,
  capped by `--max-docs` (default: `100000`).
- Encodes texts with SentenceTransformers and writes outputs under:
  - `.../subqueries/{SUBQUERY}/charts/enriched_embeds/`

Outputs:
- `embeddings.npy`
- `embeddings_ids.json`
- `sampled_records/` (parquet)
- `macro_display/` (parquet)
- `build_settings.json`

### 2) `umap_scatter.py`

Creates a macro-level UMAP scatter image from the enriched embeddings.

Main steps:
- Reads embedding artifacts from `charts/enriched_embeds/`.
- Computes (or reuses cached) UMAP 2D coordinates.
- Plots each paper as a point colored by macro color.
- Places side labels using macro centroids and anti-overlap spacing.
  - Labels are deterministic with a hard cap of `70` (`--max-labels`).
- Writes chart image to:
  - `.../subqueries/{SUBQUERY}/charts/fig_umap_scatter.png`

Subtitle in figure:
- `{SNAPSHOT} data; x documents; y macro clusters; z micro clusters`
- `x` is read from `enriched_embeds/build_settings.json` as pre-sample docs.
- `y` and `z` prefer pre-sample counts from `build_settings.json`, with
  fallback to subquery report tables if needed.

Cache written by this script:
- `.../subqueries/{SUBQUERY}/charts/enriched_embeds/umap_2d_coords.csv`

## Prerequisites

- Python environment with dependencies from `requirements.txt`.
- AWS credentials with access to the relevant S3 prefixes, Athena, and Glue catalog.
- Existing query-level and subquery-level datasets in the expected paths.

## How To Run

Run from repository root:

```bash
/home/ubuntu/topic_model/.venv/bin/python charts/build_enriched_embeddings.py \
  --snapshot 2026-06-26 \
  --query planetary-health \
  --subquery everything

/home/ubuntu/topic_model/.venv/bin/python charts/umap_scatter.py \
  --snapshot 2026-06-26 \
  --query planetary-health \
  --subquery everything
```

Or run from inside `charts/`:

```bash
python build_enriched_embeddings.py --snapshot 2026-06-26 --query planetary-health --subquery everything
python umap_scatter.py --snapshot 2026-06-26 --query planetary-health --subquery everything
```

## Useful Options

`build_enriched_embeddings.py`:
- `--max-docs`: cap records to embed (default `100000`)
- `--seed`: deterministic sampling seed
- `--model`: SentenceTransformer model name
- `--force`: overwrite existing `enriched_embeds/` outputs
- `--micro-batch-size`: number of micro IDs per Athena chunk

`umap_scatter.py`:
- `--seed`: UMAP random seed
- `--label-min`: minimum docs per macro to show labels
- `--max-labels`: hard cap for macro labels (default `70`)
- `--title`: custom chart title
- `--force`: recompute UMAP coordinates even if cache exists

## Common Pitfalls

- Use a normal double hyphen in CLI flags, for example `--subquery`.
  A Unicode dash (for example `-–subquery`) will fail argument parsing.
- If embeddings already exist and you want to rebuild, pass `--force` to
  `build_enriched_embeddings.py`.
- If the chart should be recomputed from scratch, pass `--force` to
  `umap_scatter.py` to ignore cached coordinates.
