#!/usr/bin/env python3
"""
Glue ETL job: build network input files from edges_query parquet output.

Outputs (tab-delimited, no headers):
- {OUTPUT_PATH}edges.txt/
- {OUTPUT_PATH}nodes_index.txt/
- {OUTPUT_PATH}nodes.txt/

Notes:
- This job intentionally keeps directed edges only (no bidirectional expansion).
"""

from __future__ import annotations

import argparse
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window


DEFAULT_INPUT_PATH_TEMPLATE = "s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/edges_query/"
DEFAULT_OUTPUT_PATH_TEMPLATE = "s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/"


def render_template(template: str, snapshot: str, query: str) -> str:
    return template.replace("{SNAPSHOT}", snapshot).replace("{QUERY}", query)


def normalize_s3_prefix(path: str) -> str:
    return path if path.endswith("/") else f"{path}/"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--JOB_NAME", required=True)
    parser.add_argument("--SNAPSHOT", required=True)
    parser.add_argument("--QUERY", required=True)
    parser.add_argument("--INPUT_PATH_TEMPLATE", default=DEFAULT_INPUT_PATH_TEMPLATE)
    parser.add_argument("--OUTPUT_PATH_TEMPLATE", default=DEFAULT_OUTPUT_PATH_TEMPLATE)

    args, _ = parser.parse_known_args(argv[1:])

    args.SNAPSHOT = str(args.SNAPSHOT).strip()
    args.QUERY = str(args.QUERY).strip()
    if not args.SNAPSHOT:
        raise ValueError("--SNAPSHOT cannot be empty")
    if not args.QUERY:
        raise ValueError("--QUERY cannot be empty")

    return args


def main(argv: list[str]) -> None:
    args = parse_args(argv)

    source_path = normalize_s3_prefix(
        render_template(args.INPUT_PATH_TEMPLATE, args.SNAPSHOT, args.QUERY)
    )
    output_path = normalize_s3_prefix(
        render_template(args.OUTPUT_PATH_TEMPLATE, args.SNAPSHOT, args.QUERY)
    )

    print("[config] JOB_NAME=", args.JOB_NAME)
    print("[config] SNAPSHOT=", args.SNAPSHOT)
    print("[config] QUERY=", args.QUERY)
    print("[config] SOURCE_PATH=", source_path)
    print("[config] OUTPUT_PATH=", output_path)

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args.JOB_NAME, {"JOB_NAME": args.JOB_NAME})

    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [source_path]},
        format="parquet",
    )
    source_df = source_dyf.toDF()

    from_nodes = source_df.select(col("from").alias("node_str"))
    to_nodes = source_df.select(col("to").alias("node_str"))
    unique_nodes = from_nodes.union(to_nodes).distinct()

    window_spec = Window.orderBy("node_str")
    nodes_index_df = unique_nodes.withColumn("id", row_number().over(window_spec) - 1)

    nodes_index_output_df = nodes_index_df.select("id", "node_str")
    nodes_output_df = nodes_index_df.select(col("id"), lit(1).alias("value"))

    intermediate_df = source_df.join(
        nodes_index_df,
        source_df["from"] == nodes_index_df["node_str"],
        "inner",
    ).select(
        col("id").alias("from_id"),
        col("to"),
        col("weight"),
    )

    transformed_edges_df = intermediate_df.join(
        nodes_index_df,
        intermediate_df["to"] == nodes_index_df["node_str"],
        "inner",
    ).select(
        col("from_id").alias("from"),
        col("id").alias("to"),
        col("weight"),
    ).orderBy("from", "to")

    edges_dyf = DynamicFrame.fromDF(transformed_edges_df, glue_context, "edges_dyf")
    glue_context.write_dynamic_frame.from_options(
        frame=edges_dyf,
        connection_type="s3",
        connection_options={"path": f"{output_path}edges.txt/"},
        format="csv",
        format_options={"separator": "\t", "writeHeader": False},
    )

    nodes_index_dyf = DynamicFrame.fromDF(nodes_index_output_df, glue_context, "nodes_index_dyf")
    glue_context.write_dynamic_frame.from_options(
        frame=nodes_index_dyf,
        connection_type="s3",
        connection_options={"path": f"{output_path}nodes_index.txt/"},
        format="csv",
        format_options={"separator": "\t", "writeHeader": False},
    )

    nodes_dyf = DynamicFrame.fromDF(nodes_output_df, glue_context, "nodes_dyf")
    glue_context.write_dynamic_frame.from_options(
        frame=nodes_dyf,
        connection_type="s3",
        connection_options={"path": f"{output_path}nodes.txt/"},
        format="csv",
        format_options={"separator": "\t", "writeHeader": False},
    )

    job.commit()
    print("[success] network input files written")


if __name__ == "__main__":
    main(sys.argv)
