# Charts Pipeline (Subquery Visuals)

This folder contains four scripts:
- two Python scripts for macro-level UMAP
- two R scripts for levelized bar/scatter visualizations (macro, meso, micro)

## What Each Script Does

### 1) build_enriched_embeddings.py

Builds enriched text embeddings for papers matched by a subquery.

Main steps:
- Reads matched micro clusters from subquery cluster reports.
- Pulls candidate papers from query-level article_report for those micro clusters.
- Joins macro metadata from cluster_name_macro, cluster_color_macro, and cluster_report_macro.
- Builds enriched text by prepending macro name to paper text.
  - Default text composition: macro name + title + abstract.
  - Optional title-only mode: macro name + title via `--exclude-abstract-in-text`.
  - Optional abstract backfill (off by default): `--use-abstract-fallback` to query
    `nodes_query` and then `nodes_snapshot` by paper id when article_report abstracts are missing.
- Assigns macro display order where 1 is the largest macro in the matched set.
- Applies proportional macro-aware sampling with guaranteed macro representation.
- Encodes texts with SentenceTransformers and writes outputs under charts/enriched_embeds.

### 2) umap_scatter.py

Creates a macro-level UMAP scatter image from enriched embeddings.

Main steps:
- Reads embedding artifacts from charts/enriched_embeds.
- Computes (or reuses cached) UMAP 2D coordinates.
- Colors points by macro color and adds deterministic side labels.
- Writes fig_umap_scatter.png under the subquery charts folder.

### 3) micro_scatterplots.R

Creates levelized scatter plots for macro, meso, and micro clusters.

Main steps:
- Reads subquery cluster report datasets:
  - cluster_report_macro
  - cluster_report_meso
  - cluster_report_micro
- Reads macro colors from cluster_color_macro.
- Uses ave_py (x), ave_citations (y), publications (size), and macro parent color.
- Also generates PY vs Z9 rank scatters when rank metrics are available.
  - Rank metric priority: yearly_rank_citations -> ranked_citation_score -> ranked_citation.
- Builds stable labels from cluster_code and short_name.
  - For micro, cluster_code is reconstructed when missing or invalid.
  - For macro and meso, deterministic rank-based numeric cluster codes are used.
- Applies minimum publications filtering (default 50).
- Writes outputs to both S3 and local step-06 level folders.

Outputs per level:
- charts/{level}/fig_scatter_{level}_PY_x_Z9.png (S3)
- charts/{level}/fig_scatter_{level}_PY_x_Z9_rank.png (S3, when rank metric exists)
- 06-subquery_reports/excel/snapshot_{SNAPSHOT}_{QUERY}/{SUBQUERY}/{level}/fig_scatter_{level}_PY_x_Z9.png (local)
- 06-subquery_reports/excel/snapshot_{SNAPSHOT}_{QUERY}/{SUBQUERY}/{level}/fig_scatter_{level}_PY_x_Z9_rank.png (local, when rank metric exists)

Additional micro-only output:
- fig_scatter_micro_PY_x_Z9_minx2020_miny0p6.png
- fig_scatter_micro_PY_x_Z9_rank_minx2020_miny0p6.png (when rank metric exists)

### 4) micro_cluster_bars.R

Creates levelized multipanel bar charts for macro, meso, and micro clusters.

Main steps:
- Reads subquery cluster report datasets for each requested level.
- Reads macro color palette from cluster_color_macro.
- Reads optional cluster_names (used to improve micro labels where available).
- Uses publications as bar length, with square-root x-axis scaling.
- Groups bars into panels by macro display order (default 6 macros per panel).
- For meso and micro, keeps top N clusters per macro-parent (default 10).
- Writes outputs to both S3 and local step-06 level folders.

Output per level:
- charts/{level}/fig_bars_{level}_min{MIN}_top{TOP}_mpp{MPP}.png (S3)
- 06-subquery_reports/excel/snapshot_{SNAPSHOT}_{QUERY}/{SUBQUERY}/{level}/fig_bars_{level}_min{MIN}_top{TOP}_mpp{MPP}.png (local)

## Output Contract

Canonical S3 root:
- s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/clustering/subqueries/{SUBQUERY}/

Chart outputs:
- charts/macro/
- charts/meso/
- charts/micro/

Local mirror root:
- 06-subquery_reports/excel/snapshot_{SNAPSHOT}_{QUERY}/{SUBQUERY}/macro/
- 06-subquery_reports/excel/snapshot_{SNAPSHOT}_{QUERY}/{SUBQUERY}/meso/
- 06-subquery_reports/excel/snapshot_{SNAPSHOT}_{QUERY}/{SUBQUERY}/micro/

## Prerequisites

- Python environment with dependencies from requirements.txt.
- R packages installed: aws.s3, arrow, dplyr, ggplot2, ggrepel.
- AWS credentials with access to the relevant S3 prefixes.

## How To Run

Run from repository root:

```bash
/home/ubuntu/topic_model/.venv/bin/python charts/build_enriched_embeddings.py \
  --snapshot 2026-06-26 \
  --query quantum \
  --subquery everything

/home/ubuntu/topic_model/.venv/bin/python charts/umap_scatter.py \
  --snapshot 2026-06-26 \
  --query quantum \
  --subquery everything

Rscript charts/micro_scatterplots.R \
  --snapshot 2026-06-26 \
  --query quantum \
  --subquery everything

Rscript charts/micro_cluster_bars.R \
  --snapshot 2026-06-26 \
  --query quantum \
  --subquery everything
```

## Useful Options

build_enriched_embeddings.py:
- --max-docs
- --seed
- --model
- --force
- --micro-batch-size
- --exclude-abstract-in-text
- --use-abstract-fallback
- --abstract-batch-size

umap_scatter.py:
- --seed
- --label-min
- --max-labels
- --title
- --force

micro_scatterplots.R:
- --snapshot
- --query
- --subquery (or --query-folder)
- --level macro,meso,micro (default: all)
- --aws-region (default from AWS_REGION/AWS_DEFAULT_REGION, fallback ap-northeast-1)
- --min-size (default 50)
- --width (default 12)
- --height (default 7)
- --dpi (default 240)

micro_cluster_bars.R:
- --snapshot
- --query
- --subquery (or --query-folder)
- --level macro,meso,micro (default: all)
- --aws-region (default from AWS_REGION/AWS_DEFAULT_REGION, fallback ap-northeast-1)
- --min-size (default 50)
- --top-per-parent (aliases: --top-per-macro, --top_per_macro; default 10)
- --macros-per-panel (default 6)
- --panel-width (default 8)
- --panel-height (default 6)
- --dpi (default 240)

## Common Pitfalls

- Use a normal double hyphen in CLI flags (for example: --subquery).
- If S3 responds with PermanentRedirect (HTTP 301), set --aws-region to the bucket region.
- For large subqueries, bar charts can become very wide/tall. Tune panel-width, panel-height, and macros-per-panel.
