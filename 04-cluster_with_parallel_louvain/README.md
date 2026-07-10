# Step 04 - Parallel Louvain (Notebook Workflow)

This folder runs the core hierarchical Louvain clustering using NetworKit.

## Scope

The supported workflow is notebook-first:

1. `download_input.ipynb`
2. `parallel_louvain.ipynb`
3. `upload_outputs.ipynb`
4. `clear_input_output_subfolders.ipynb` (optional maintenance utility)

Step 04 now consumes the step-03 network exports directly and uses `SNAPSHOT` + `QUERY` as the configuration contract.

## Input and Output Contracts

S3 inputs:

- `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/edges.txt/`
- `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/nodes_index.txt/`
- `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/nodes.txt/`

S3 outputs:

- `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/clustering/louvain_clusters.txt/`
- `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/clustering/louvain_clusters.md`

Local staging layout used by notebooks:

- `input/snapshot_{SNAPSHOT}/{QUERY}/edges.txt`
- `input/snapshot_{SNAPSHOT}/{QUERY}/nodes_index.txt`
- `input/snapshot_{SNAPSHOT}/{QUERY}/nodes.txt`
- `output/snapshot_{SNAPSHOT}/{QUERY}/louvain_clusters.txt`

## Run

From this folder:

```bash
jupyter notebook download_input.ipynb
jupyter notebook parallel_louvain.ipynb
jupyter notebook upload_outputs.ipynb
```

For local cleanup between runs:

```bash
jupyter notebook clear_input_output_subfolders.ipynb
```

Inside the cleanup notebook, run a dry-run first (`EXECUTE = False`), review the listed files/folders, then set `EXECUTE = True` only when you are ready to delete local contents under `input/` and/or `output/`.

## Notes on Performance

- For very large multipart downloads, AWS CLI (`aws s3 cp --recursive`) is typically faster than sequential notebook streaming.
- The upload notebook uses boto3 multipart upload and is usually fast enough, while also performing schema checks and writing provenance metadata.
- Edge deduplication is no longer a required step because step 03 already publishes canonical network text assets for step 04 consumption.
