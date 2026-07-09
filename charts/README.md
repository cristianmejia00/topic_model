# Charts Pipeline (Subquery Visuals)

This folder contains three scripts:
- two Python scripts for macro-level UMAP
- one R script for micro-level scatter plots

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

### 3) `micro_scatterplots.R`

Creates two micro-level scatter plots using `ggplot2` + `ggrepel`.

Main steps:
- Reads subquery micro report from:
  - `.../subqueries/{SUBQUERY}/cluster_report_micro/`
- Reads macro colors from:
  - `.../cluster_color_macro/`
- Ensures labels come from `cluster_code`.
  - If `cluster_code` is absent or empty, computes fallback labels with the
    same deterministic logic as step-06 subquery writers.
- Uses point color inherited from parent macro (`color_hex`).
- Uses point size from `publications`.
- Applies a minimum publication threshold per micro cluster (`MIN_SIZE = 50`).
- Builds two charts:
  - x=`ave_py`, y=`ave_citations`
  - x=`ave_py`, y=ranked normalized citations, with priority:
    `yearly_rank_citations` -> `ranked_citation_score` -> `ranked_citation`

Outputs:
- `.../subqueries/{SUBQUERY}/charts/fig_scatter_micro_PY_x_Z9.png`
- `.../subqueries/{SUBQUERY}/charts/fig_scatter_micro_PY_x_Z9_rank.png`
- If `--min_x` and/or `--min_y` are provided, output filenames include suffixes,
  for example:
  - `fig_scatter_micro_PY_x_Z9_minx2020_miny0p6.png`
  - `fig_scatter_micro_PY_x_Z9_rank_minx2020_miny0p6.png`

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

Rscript charts/micro_scatterplots.R \
  --snapshot 2026-06-26 \
  --query planetary-health \
  --subquery everything \
  --min_x 2020 \
  --min_y 0.6
```

Or run from inside `charts/`:

```bash
python build_enriched_embeddings.py --snapshot 2026-06-26 --query planetary-health --subquery everything
python umap_scatter.py --snapshot 2026-06-26 --query planetary-health --subquery everything
Rscript micro_scatterplots.R --snapshot 2026-06-26 --query planetary-health --subquery everything
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

`micro_scatterplots.R`:
- `--snapshot`: snapshot token
- `--query`: query token
- `--subquery`: subquery folder
- `--aws-region`: AWS region for S3 operations (default: `AWS_REGION`/`AWS_DEFAULT_REGION`, fallback `ap-northeast-1`)
- `--min_x` (or `--min-x`): optional lower bound for x-axis (`ave_py`)
- `--min_y` (or `--min-y`): optional lower bound for y-axis (applied to each plot's y metric)
- `--width`: output width in inches (default `12`)
- `--height`: output height in inches (default `7`)
- `--dpi`: output DPI (default `240`)

## Common Pitfalls

- Use a normal double hyphen in CLI flags, for example `--subquery`.
  A Unicode dash (for example `-–subquery`) will fail argument parsing.
- If embeddings already exist and you want to rebuild, pass `--force` to
  `build_enriched_embeddings.py`.
- If the chart should be recomputed from scratch, pass `--force` to
  `umap_scatter.py` to ignore cached coordinates.
- `micro_scatterplots.R` requires R packages: `aws.s3`, `arrow`, `dplyr`,
  `ggplot2`, `ggrepel`.
- If you see `PermanentRedirect` / `Moved Permanently (HTTP 301)` from S3,
  run with `--aws-region ap-northeast-1` (or set `AWS_REGION` accordingly).
