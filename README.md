# Topic Model — Pipeline Guide

This repository contains the full workflow for hierarchical clustering, BERTopic
retrofit, plotting, and subquery report generation.

## Main Scripts

- `annex/parallel_louvain.ipynb`
  - Computes robust node-level hierarchy assignments (`node_id`, `micro_cluster`,
    `meso_cluster`, `macro_cluster`) from citation graphs.
- `create_athena_reports.py`
  - Builds Athena tables in order: `article_report`, `cluster_report_micro`,
    `cluster_report_meso`, `cluster_report_macro`.
- `audit_athena_hierarchy.py`
  - Audits parent coverage and one-to-one hierarchy consistency directly in Athena.
- `cluster_bertopic.py`
  - Generates BERTopic keywords, embeddings, and shared 2D coordinates for
    micro/meso/macro levels.

## Plot Scripts

- `main_plots/plot_images.py`
  - Renders `main_plots/cluster_map.png` and `main_plots/cluster_map.pdf`.
- `main_plots/plot_embeds.py`
  - Optional visualization-only embedding pass for tighter color coherence.
- `main_plots/check_macro_plot.py`
  - Optional diagnostic macro highlight plot (`main_plots/macro_check.png`).

## Subquery Scripts

- `subqueries/subquery_search_by_topic.py`
  - Topic-query retrieval over micro centroids.
- `subqueries/subquery_search_by_filters.py`
  - Numeric-filter retrieval (supports multiple AND filters).
- `subqueries/subquery_search_passthrough.py`
  - No-filter export over all micro clusters.
- `subqueries/name_clusters.py`
  - Names micro clusters in a selected subquery.
- `subqueries/generate_subquery_report.py`
  - Builds/publishes HTML report for one subquery.
- `subqueries/explore_subquery.py`
  - Terminal explorer for generated subquery outputs.

## Order of Code Execution

Root entrypoint behavior:

- Use `run_root_pipeline.py` for root-level scripts.
- `--database` is required (no default).
- If output targets already exist, execution fails unless `--force` is provided.

### A) Core Pipeline

1. `annex/parallel_louvain.ipynb`
2. `create_athena_reports.py --overwrite`
3. `audit_athena_hierarchy.py`
4. `cluster_bertopic.py`
5. `main_plots/plot_images.py`

Optional diagnostics after step 5:

1. `main_plots/check_macro_plot.py`
2. `main_plots/plot_embeds.py` (then rerun `main_plots/plot_images.py` against images output)

### B) Subquery Pipeline

1. Run one selector:
   - `subqueries/subquery_search_by_topic.py`
   - or `subqueries/subquery_search_by_filters.py`
   - or `subqueries/subquery_search_passthrough.py`
2. `subqueries/name_clusters.py`
3. `subqueries/generate_subquery_report.py`
4. `subqueries/explore_subquery.py`

## Setup

### Prerequisites

- Python 3.9+
- AWS credentials configured with Athena read access and S3 read/write access.

### Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running (Quick Start)

```bash
source .venv/bin/activate

# Core pipeline
python run_root_pipeline.py --database q20260629 --step athena_reports --force
python run_root_pipeline.py --database q20260629 --step audit_hierarchy
python run_root_pipeline.py --database q20260629 --step bertopic --force
python run_root_pipeline.py --database q20260629 --step plot_images --force

# Optional root steps
python run_root_pipeline.py --database q20260629 --step macro_colors --force
python run_root_pipeline.py --database q20260629 --step macro_names --force
python run_root_pipeline.py --database q20260629 --step plot_embeds --force
python run_root_pipeline.py --database q20260629 --step check_macro_plot --force

# Subquery example (topic-based)
python subqueries/subquery_search_by_topic.py
python subqueries/name_clusters.py
python subqueries/generate_subquery_report.py
python subqueries/explore_subquery.py
```

## Dependencies

Key libraries (pinned in `requirements.txt`):

- bertopic
- sentence-transformers
- umap-learn
- scikit-learn
- awswrangler
- pandas
- numpy
- pyarrow
- matplotlib
