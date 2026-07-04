"""
create_macro_color_palette.py
=============================
Create a stable macro-cluster color palette and save it to S3.

Output:
  s3://openalex-outputs/classification/q20260629/cluster_color_macro/
  columns: macro_cluster, color_hex
"""

from __future__ import annotations

import awswrangler as wr
import pandas as pd

from macro_palette import stable_color_hex
from root_common_config import get_root_paths

ROOT_PATHS = get_root_paths()
MACRO_REPORT_PATH = ROOT_PATHS.macro_report
OUT_PATH = ROOT_PATHS.cluster_color_macro


def main() -> None:
    rep = wr.s3.read_parquet(MACRO_REPORT_PATH)
    if "macro_cluster" not in rep.columns:
        raise KeyError("cluster_report_macro is missing 'macro_cluster'")

    ids = sorted(pd.Series(rep["macro_cluster"]).dropna().astype("int64").unique().tolist())
    out = pd.DataFrame(
        {
            "macro_cluster": ids,
            "color_hex": [stable_color_hex(mid) for mid in ids],
        }
    )

    wr.s3.to_parquet(out, path=OUT_PATH, dataset=True, mode="overwrite")

    print(f"[done] wrote {len(out):,} macro colors -> {OUT_PATH}")
    print(out.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
