import awswrangler as wr
import pandas as pd

PATH = "s3://openalex-outputs/classification/q20260629/subqueries/quantum_computing/cluster_names/"

df = wr.s3.read_parquet(PATH)

pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 0)

print(df.to_string(index=False))
