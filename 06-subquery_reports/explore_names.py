import awswrangler as wr
import pandas as pd
import argparse

from common_config import resolve_paths


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Print cluster names for one step-06 subquery.")
	parser.add_argument("--snapshot", default=None, help="Snapshot token, e.g. 2026-06-26.")
	parser.add_argument("--query", default=None, help="Query token, e.g. q20260629.")
	parser.add_argument("--subquery", default=None, help="Subquery folder name.")
	parser.add_argument("--query-folder", default=None, help="Deprecated alias for --subquery.")
	return parser.parse_args()


args = parse_args()
paths = resolve_paths(
	snapshot=args.snapshot,
	query=args.query,
	subquery=args.subquery,
	query_folder=args.query_folder,
)
path = f"{paths.subquery_base}cluster_names/"

print("[config] database:", paths.database)
print("[config] source:", path)

df = wr.s3.read_parquet(path)

pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 0)

print(df.to_string(index=False))
