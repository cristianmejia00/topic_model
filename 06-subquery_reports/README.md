# Subqueries Pipeline Guide

This folder contains the step-06 workflow for building one subquery dataset,
naming its micro clusters, generating HTML/Excel outputs, and exploring results.

## Required Context Contract

All step-06 scripts now resolve context from:

1. CLI arguments
2. Environment variables

Required context keys:

- `snapshot`: `--snapshot` or `TOPIC_MODEL_SNAPSHOT`
- `query`: `--query` or `TOPIC_MODEL_QUERY`
- `subquery`: `--subquery` or `TOPIC_MODEL_SUBQUERY`

Compatibility alias:

- `--query-folder` and `TOPIC_MODEL_QUERY_FOLDER` are supported as aliases for `subquery`.

Derived database name:

- `snapshot_{SNAPSHOT}-{QUERY}`

Only these defaults remain:

- Athena staging: `s3://openalex-outputs/athena-staging/`
- Athena workgroup: `primary`

## Scripts in This Folder

- `run_subquery_search.py`
	- Entry point for `topic`, `filters`, or `passthrough` search.
- `subquery_search_by_topic.py`
	- Topic-embedding search over micro centroids.
- `subquery_search_by_filters.py`
	- Numeric filter search (repeated `--filter` combined with `AND`).
- `subquery_search_passthrough.py`
	- No-filter export of all micro clusters.
- `name_clusters.py`
	- LLM naming for matched micro clusters.
- `generate_subquery_html_report.py`
	- Builds static HTML and uploads `report/` to S3.
- `generate_subquery_excel_report.py`
	- Builds local 4-file Excel report pack with top-20 articles per micro,
	  including `authors` and `publication_source` enrichment from query-level
	  `nodes_query/`.
- `generate_utokyo_subquery_excel_report.py`
	- Builds local UTokyo-focused workbook.
- `explore_subquery.py`
	- Terminal explorer for generated subsets.
- `explore_names.py`
	- Prints generated cluster names from S3.
- `common_config.py`
	- Shared context/path resolution for step-06.

## Execution Order

Recommended run sequence after step-05 Athena + BERTopic inputs are available:

1. Run one search mode.
2. Name clusters.
3. Generate HTML and/or Excel outputs.
4. Explore outputs for QC (optional).

```bash
source .venv/bin/activate

SNAPSHOT=2026-06-26
QUERY=q20260629
SUBQUERY=quantum_computing

# 1) choose one search mode
python 06-subquery_reports/run_subquery_search.py \
	--search topic \
	--snapshot "$SNAPSHOT" \
	--query "$QUERY" \
	--subquery "$SUBQUERY"

# alternate search modes:
# python 06-subquery_reports/run_subquery_search.py --search filters --snapshot "$SNAPSHOT" --query "$QUERY" --subquery filters_ave_py_ge_2023_and_recency_py_ge_0_5_and_size_50 --filter 'ave_py>=2023' --filter 'recency_py>=0.5' --filter 'publications>=50'
# python 06-subquery_reports/run_subquery_search.py --search passthrough --snapshot "$SNAPSHOT" --query "$QUERY" --subquery everything

# 2) name matched clusters
python 06-subquery_reports/name_clusters.py \
	--snapshot "$SNAPSHOT" \
	--query "$QUERY" \
	--subquery "$SUBQUERY"

# 3) build reports
python 06-subquery_reports/generate_subquery_html_report.py \
	--snapshot "$SNAPSHOT" \
	--query "$QUERY" \
	--subquery "$SUBQUERY"

python 06-subquery_reports/generate_subquery_excel_report.py \
	--snapshot "$SNAPSHOT" \
	--query "$QUERY" \
	--subquery "$SUBQUERY"

python 06-subquery_reports/generate_utokyo_subquery_excel_report.py \
	--snapshot "$SNAPSHOT" \
	--query "$QUERY" \
	--subquery "$SUBQUERY"

# 4) optional QC
python 06-subquery_reports/explore_subquery.py \
	--snapshot "$SNAPSHOT" \
	--query "$QUERY" \
	--subquery "$SUBQUERY"
```

## Canonical Output Layout

All S3 outputs for step-06 land under:

`s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/clustering/subqueries/{SUBQUERY}/`

Expected datasets:

- `matches/`
- `article_top10/`
- `cluster_report_micro/`
- `cluster_report_meso/`
- `cluster_report_macro/`
- `top_countries/`
- `top_institutions/`
- `cluster_names/` (after naming)
- `report/` (after HTML generation)

Local outputs:

- HTML: `docs/{database}/{subquery}/report/index.html`
- Excel pack:
	- `excel/{database}/{subquery}/article_report_top20.xlsx`
	- `excel/{database}/{subquery}/cluster_profiles.xlsx`
	- `excel/{database}/{subquery}/countries_summary.xlsx`
	- `excel/{database}/{subquery}/institutions_summary.xlsx`
- UTokyo workbook:
	- `utokyo/{database}/{subquery}/utokyo_cluster_and_articles.xlsx`

Excel report notes:

- The S3 subset `article_top10/` is still produced by step-06 search scripts.
  The Excel exporter independently queries `article_report` to build top-20
  article rows per micro cluster.
- `article_report_top20.xlsx` includes `authors` and `publication_source` using
  query-level `nodes_query/` (`s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/nodes_query/`).
- In `cluster_profiles.xlsx` micro sheet, the exporter includes `macro_cluster_id`
  (mapped to macro sheet `display_id`) and `cluster_code`.
- If `cluster_code` is missing/blank/invalid in `cluster_report_micro`, the
  exporter recomputes it with deterministic publication-based ranking.

## Quick Validation

```bash
.venv/bin/python -m py_compile \
	06-subquery_reports/common_config.py \
	06-subquery_reports/run_subquery_search.py \
	06-subquery_reports/subquery_search_by_topic.py \
	06-subquery_reports/subquery_search_by_filters.py \
	06-subquery_reports/subquery_search_passthrough.py \
	06-subquery_reports/name_clusters.py \
	06-subquery_reports/generate_subquery_html_report.py \
	06-subquery_reports/generate_subquery_excel_report.py \
	06-subquery_reports/generate_utokyo_subquery_excel_report.py \
	06-subquery_reports/explore_subquery.py \
	06-subquery_reports/explore_names.py
```