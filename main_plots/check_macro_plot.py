import awswrangler as wr
import matplotlib.pyplot as plt
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from macro_palette import color_for_macro, load_macro_color_map

# same values as in plot_clusters.py
IN_DIR       = "s3://openalex-outputs/classification/q20260629/bertopic/images/"
MICRO_REPORT = "s3://openalex-outputs/classification/q20260629/cluster_report_micro/"

micro = wr.s3.read_parquet(f"{IN_DIR}micro/").rename(
    columns={"x_coords": "x", "y_coords": "y"})
rep = wr.s3.read_parquet(MICRO_REPORT)[["micro_cluster", "macro_cluster"]]
micro = micro.merge(rep, left_on="cluster", right_on="micro_cluster", how="left")

target = micro["macro_cluster"].value_counts().idxmax()   # largest macro
hit = micro["macro_cluster"] == target
macro_color_map = load_macro_color_map()
target_color = color_for_macro(target, macro_color_map)

plt.figure(figsize=(10, 10))
plt.scatter(micro.x[~hit], micro.y[~hit], s=1, c="lightgray", alpha=0.3, rasterized=True)
plt.scatter(micro.x[hit],  micro.y[hit],  s=4, c=target_color, alpha=0.8, rasterized=True)
plt.gca().set_aspect("equal")
plt.axis("off")
plt.title(f"macro {target}")
out_png = Path(__file__).resolve().parent / "macro_check.png"
plt.savefig(out_png, dpi=200, bbox_inches="tight")
print(f"highlighted macro {target} ({target_color}): {hit.sum():,} of {len(micro):,} micro clusters")