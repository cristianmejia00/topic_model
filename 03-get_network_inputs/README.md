# 03-get_network_inputs

This step converts `edges_query` parquet into network input text files for downstream Louvain processing.

## Source Of Truth

- Production ETL script: `get_network_inputs_etl.py`
- Deployment utility: `deploy_glue_network_inputs_job.py`
- Run utility: `run_glue_network_inputs_job.py`
- Runtime config: `config.yaml`

This folder intentionally does not keep a production notebook.

## Required Runtime Config

Edit `config.yaml` and provide non-empty values:

- `SNAPSHOT`
- `QUERY`

Example:

```yaml
SNAPSHOT: "2026-06-26"
QUERY: "q20260629"
```

## Input And Output Paths

Resolved from `SNAPSHOT` and `QUERY`:

- Input parquet prefix: `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/edges_query/`
- Output prefix: `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/`

Outputs written as tab-delimited text (no header):

- `edges.txt/`
- `nodes_index.txt/`
- `nodes.txt/`

## Important Behavior

- Edges are kept directed only.
- Bidirectional duplication is intentionally removed.

## Glue Job Settings Parity (Notebook -> Script Job)

Deploy script applies the notebook-equivalent settings:

- Glue version: `5.0`
- Worker type: `G.2X`
- Number of workers: `10`
- Timeout: `240` minutes
- Auto-scaling: enabled
- Write shuffle files to S3: enabled
- Spark shuffle storage path: `s3://openalex-outputs/cwts/spark/`

## IAM Role

To match the role used by the old notebook job:

- By default, deploy tries to copy role from existing job `edges_to_cwts_format_v3`.
- Or pass `--role <role-arn-or-name>` explicitly.

## Deploy

```bash
python 03-get_network_inputs/deploy_glue_network_inputs_job.py \
  --job-name openalex_get_network_inputs \
  --script-s3-prefix s3://aws-glue-assets-702228044494-ap-northeast-1/scripts/03-get-network-inputs/
```

If role auto-lookup is unavailable:

```bash
python 03-get_network_inputs/deploy_glue_network_inputs_job.py \
  --job-name openalex_get_network_inputs \
  --role arn:aws:iam::<account-id>:role/<glue-role> \
  --script-s3-prefix s3://aws-glue-assets-702228044494-ap-northeast-1/scripts/03-get-network-inputs/
```

## Run

```bash
python 03-get_network_inputs/run_glue_network_inputs_job.py \
  --config 03-get_network_inputs/config.yaml \
  --job-name openalex_get_network_inputs \
  --wait
```
