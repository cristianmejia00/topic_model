#!/usr/bin/env bash
set -euo pipefail

aws s3 sync "s3://openalex/data/jsonl/works" "s3://openalex-works/snapshot/data/works" --delete
