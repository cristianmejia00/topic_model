# 02-get_query_subset

This step extracts a query-specific subset from a snapshot dataset already published to:

- `s3://openalex-results/snapshot_{SNAPSHOT}/`

## Required Inputs

- A snapshot folder named exactly as `SNAPSHOT`, and under it a query folder named exactly as `QUERY` with both SQL files:
  - `nodes_query.sql`
  - `edges_query.sql`

Example structure:

```text
02-get_query_subset/
  config.yaml
  run_query_subset.py
  2026-06-26/
    q20260629/
      nodes_query.sql
      edges_query.sql
```

## Configuration (No Defaults for SNAPSHOT/QUERY)

Edit `config.yaml` and set:

- `SNAPSHOT` (required, non-empty)
- `QUERY` (required, non-empty)
- `ATHENA_STAGING` (required, non-empty)
- `ATHENA_WORKGROUP` (required, non-empty)

Example:

```yaml
SNAPSHOT: "2026-06-26"
QUERY: "q20260629"
ATHENA_STAGING: "s3://openalex-outputs/athena-staging/"
ATHENA_WORKGROUP: "primary"
```

The runner resolves:

- Database: `snapshot_{SNAPSHOT}`
- Snapshot root: `s3://openalex-results/snapshot_{SNAPSHOT}/`
- Source tables locations:
  - `.../nodes_snapshot/`
  - `.../edges_snapshot/`

## What The Runner Does

1. Validates config and required SQL files.
2. Validates snapshot root exists and is non-empty in S3.
3. Validates `nodes_snapshot/` and `edges_snapshot/` prefixes are non-empty.
4. Ensures Glue/Athena database `snapshot_{SNAPSHOT}` exists.
5. Ensures source external tables exist if missing:
   - `nodes_snapshot`
   - `edges_snapshot`
6. Repairs `nodes_snapshot` partitions.
7. Executes SQL in strict order:
   - first `nodes_query.sql`
   - then `edges_query.sql`

SQL supports interpolation placeholders and is rendered before execution:

- `{SNAPSHOT}` -> config `SNAPSHOT`
- `{QUERY}` -> config `QUERY`
- `{DATABASE}` -> `snapshot_{SNAPSHOT}`

## SQL Contract Notes

- `nodes_query.sql` must read from `nodes_snapshot`.
- `edges_query.sql` must read from `edges_snapshot`.
- `edges_query.sql` must depend on `nodes_query` (not `nodes_subset`).
- Use SQL files under `02-get_query_subset/{SNAPSHOT}/{QUERY}/`.

## Usage

Validation only:

```bash
python 02-get_query_subset/run_query_subset.py --config 02-get_query_subset/config.yaml --validate-only
```

Full run:

```bash
python 02-get_query_subset/run_query_subset.py --config 02-get_query_subset/config.yaml
```

Full run with overwrite (drop existing `nodes_query`/`edges_query` and clear their output prefixes):

```bash
python 02-get_query_subset/run_query_subset.py --config 02-get_query_subset/config.yaml --overwrite
```

## Important Note About Athena Console SQL

Your SQL can stay the same if it is valid Athena SQL and references the correct source/dependency table names for this scripted flow.
The biggest differences when running from repo are:

- explicit config-driven database/workgroup/staging
- preflight checks before execution
- guaranteed order (`nodes_query.sql` then `edges_query.sql`)
