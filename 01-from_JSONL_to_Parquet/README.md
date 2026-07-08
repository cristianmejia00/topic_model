# 01-from_JSONL_to_Parquet

This step converts OpenAlex works snapshot JSONL into Parquet datasets for downstream Athena and analytics workflows.

## Source of Truth

- Production ETL script: `glue_jsonl_to_parquet_etl.py`
- This folder intentionally does not track a notebook to avoid drift.
- Production execution should use the script-based Glue job flow below.

## Files

- `glue_jsonl_to_parquet_etl.py`: Glue Spark ETL logic (JSONL -> nodes and edges parquet).
- `deploy_glue_jsonl_to_parquet_job.py`: uploads script to S3 and creates/updates Glue job.
- `run_glue_jsonl_to_parquet_job.py`: starts job run and optionally waits for completion.

## Default Data Paths

- Input JSONL snapshot: `s3://openalex-works/snapshot/data/works`
- Output base template: `s3://openalex-results/snapshot_{SNAPSHOT_DATE}`
- `SNAPSHOT_DATE` is read from `s3://openalex-works/snapshot/data/works/manifest.json` field `date`
- Example resolved output base: `s3://openalex-results/snapshot_2026-06-26`
- Nodes output: `s3://openalex-results/snapshot_2026-06-26/nodes_partitioned/`
- Edges output: `s3://openalex-results/snapshot_2026-06-26/edges/`

## Deployment (Create or Update Glue Job)

Set role and run deploy:

```bash
export GLUE_JOB_ROLE="arn:aws:iam::702228044494:role/AWSGlueServiceRole_S3FullAccess"

python 01-from_JSONL_to_Parquet/deploy_glue_jsonl_to_parquet_job.py \
  --job-name openalex_jsonl_to_parquet \
  --script-s3-prefix s3://aws-glue-assets-702228044494-ap-northeast-1/scripts/01-from-jsonl-to-parquet/
```

The deploy script uploads `glue_jsonl_to_parquet_etl.py` to S3 and creates or updates the Glue job definition.

## Run Job

Launch and wait:

```bash
python 01-from_JSONL_to_Parquet/run_glue_jsonl_to_parquet_job.py \
  --job-name openalex_jsonl_to_parquet \
  --wait
```

Run with path overrides:

```bash
python 01-from_JSONL_to_Parquet/run_glue_jsonl_to_parquet_job.py \
  --job-name openalex_jsonl_to_parquet \
  --input-path s3://openalex-works/snapshot/data/works \
  --output-path s3://openalex-results/snapshot_{SNAPSHOT_DATE} \
  --write-mode overwrite \
  --wait
```

## IAM Requirements

Glue job role needs:

- Read access to input snapshot bucket/prefix.
- Write/delete access to output parquet prefix.
- CloudWatch Logs permissions for job logging.
- Access to temporary directories if `--TempDir` is configured.

Caller identity running deploy/run scripts needs:

- `glue:GetJob`, `glue:CreateJob`, `glue:UpdateJob`, `glue:StartJobRun`, `glue:GetJobRun`
- `s3:PutObject` for script upload prefix

## Suggested Operator Sequence

1. Sync latest snapshot into input prefix.
2. Run deploy script to publish latest ETL code and job settings.
3. Run job script with `--wait`.
4. Validate outputs in S3.
5. Refresh Glue catalog/Athena metadata if table definitions need updates.
