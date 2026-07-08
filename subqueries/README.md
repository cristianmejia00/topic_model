# Subqueries Pipeline Guide

This folder contains the end-to-end workflow for building one subquery dataset,
naming its micro clusters, generating a static HTML report, and exploring results.

## Scripts in This Folder

- `run_subquery_search.py`
  - Single entrypoint for search mode selection (`topic`, `filters`, `passthrough`).
- `subquery_search_by_topic.py`
  - Topic-embedding search over micro centroids.
- `subquery_search_by_filters.py`
  - Numeric filter search (supports repeated `--filter` clauses combined with `AND`).
- `subquery_search_passthrough.py`
  - No-filter export of all micro clusters.
- `name_clusters.py`
  - LLM-based naming for micro clusters in a selected query folder.
- `generate_subquery_html_report.py`
  - Builds a static HTML report and uploads it to S3.
- `explore_subquery.py`
  - Terminal explorer for the generated subsets.
- `common_config.py`
  - Shared defaults and path resolvers used by all scripts.

## Configuration Precedence

All subquery scripts resolve `database` and `query_folder` in this order:

1. CLI argument (`--database`, `--query-folder`)
2. Environment variable (`TOPIC_MODEL_DATABASE`, `TOPIC_MODEL_QUERY_FOLDER`)
3. Default in `common_config.py`

### Optional environment setup

```bash
export TOPIC_MODEL_DATABASE=q20260629
export TOPIC_MODEL_QUERY_FOLDER=quantum_computing
```

## Code Execution Order

Use this order after the core pipeline has already created Athena + BERTopic inputs.

### Recommended path (single entrypoint)

1. Run one search mode via `run_subquery_search.py`.
2. Name clusters for that query folder.
3. Generate the HTML report.
4. Explore in terminal (optional, for QC).

```bash
source .venv/bin/activate
DB=q20260629
QUERY_FOLDER=quantum_computing

# 1) choose one search mode
python subqueries/run_subquery_search.py --search topic --database "$DB" --query-folder "$QUERY_FOLDER"
# or
# QUERY_FOLDER=filters_ave_py_ge_2022_and_recency_py_ge_0_4
python subqueries/run_subquery_search.py --search filters --database "$DB" --query-folder "$QUERY_FOLDER" --filter 'ave_py>=2022' --filter 'recency_py>=0.4'
# or
# QUERY_FOLDER=everything
python subqueries/run_subquery_search.py --search passthrough --database "$DB" --query-folder "$QUERY_FOLDER"

# 2) name matched clusters
python subqueries/name_clusters.py --database "$DB" --query-folder "$QUERY_FOLDER"

# 3) build and publish report
python subqueries/generate_subquery_html_report.py --database "$DB" --query-folder "$QUERY_FOLDER"

# 3b) build local Excel report pack (4 files)
python subqueries/generate_subquery_excel_report.py --database "$DB" --query-folder "$QUERY_FOLDER"

# 3c) build UTokyo-focused workbook (2 sheets)
python subqueries/generate_utokyo_subquery_excel_report.py --database "$DB" --query-folder "$QUERY_FOLDER"

# 4) inspect in terminal
python subqueries/explore_subquery.py --database "$DB" --query-folder "$QUERY_FOLDER"
```

### Direct script path (advanced)

Use this only if you intentionally want to bypass the dispatcher.

```bash
# topic search
python subqueries/subquery_search_by_topic.py --database q20260629 --query-folder quantum_computing

# filters search
python subqueries/subquery_search_by_filters.py --database q20260629 --query-folder filters_ave_py_ge_2022_and_recency_py_ge_0_4 --filter 'ave_py>=2022' --filter 'recency_py>=0.4'

# passthrough search
python subqueries/subquery_search_passthrough.py --database q20260629 --query-folder everything
```

## Default Query Folders

- Topic search default: `quantum_computing`
- Filter search default: `filters_ave_py_ge_2022_and_recency_py_ge_0_4`
- Passthrough default: `everything`

## Output Layout

For a given query folder, outputs are written under:

`s3://openalex-outputs/classification/{database}/subqueries/{query_folder}/`

Expected datasets:

- `matches/`
- `article_top10/`
- `cluster_report_micro/`
- `cluster_report_meso/`
- `cluster_report_macro/`
- `top_countries/`
- `top_institutions/`
- `cluster_names/` (after naming)
- `report/` (after report generation)

Local report output:

- `docs/{query_folder}/report/index.html`

Local Excel output:

- `excel/{database}/{query_folder}/article_report_top10.xlsx`
- `excel/{database}/{query_folder}/cluster_profiles.xlsx`
- `excel/{database}/{query_folder}/countries_summary.xlsx`
- `excel/{database}/{query_folder}/institutions_summary.xlsx`

Local UTokyo output:

- `utokyo/{database}/{query_folder}/utokyo_cluster_and_articles.xlsx`

## Quick Validation

After editing subquery scripts, run:

```bash
.venv/bin/python -m py_compile \
	subqueries/common_config.py \
	subqueries/run_subquery_search.py \
	subqueries/subquery_search_by_topic.py \
	subqueries/subquery_search_by_filters.py \
	subqueries/subquery_search_passthrough.py \
	subqueries/name_clusters.py \
	subqueries/generate_subquery_html_report.py \
	subqueries/explore_subquery.py
```