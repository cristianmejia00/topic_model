# 00-update_openalex_snapshot

This step refreshes the OpenAlex works snapshot used by downstream pipelines in this repository.

Run this a few times per year (for example quarterly, or whenever you want to pull a newer upstream snapshot).

## What It Does

It synchronizes:

- Source: `s3://openalex/data/jsonl/works`
- Destination: `s3://openalex-works/snapshot/data/works`

with `--delete`, so files removed upstream are also removed in the destination.

## Script

Use the provided script:

- `sync_openalex_works_snapshot.sh`

## How To Execute

From the repository root:

```bash
cd 00-update_openalex_snapshot
./sync_openalex_works_snapshot.sh
```

Or execute from anywhere:

```bash
bash 00-update_openalex_snapshot/sync_openalex_works_snapshot.sh
```

## Prerequisites

- AWS CLI installed and configured
- IAM permissions for read on `s3://openalex/data/jsonl/works`
- IAM permissions for write/delete on `s3://openalex-works/snapshot/data/works`
