#!/usr/bin/env python3
"""
Glue ETL job: parse OpenAlex works JSONL snapshot to curated Parquet outputs.

Outputs:
- {OUTPUT_PATH}/nodes_snapshot/  (partitioned by publication_year)
- {OUTPUT_PATH}/edges_snapshot/
"""

from __future__ import annotations

import argparse
import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    array_distinct,
    col,
    collect_list,
    expr,
    explode,
    flatten,
    from_json,
    lit,
    monotonically_increasing_id,
    substring,
    transform,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


DEFAULT_INPUT_PATH = "s3://openalex-works/snapshot/data/works"
DEFAULT_OUTPUT_PATH = "s3://openalex-results/snapshot_{SNAPSHOT_DATE}"
DEFAULT_OPENALEX_PREFIX = "https://openalex.org/"
DEFAULT_SUBSTRING_LEN = 500
DEFAULT_WRITE_MODE = "overwrite"
VALID_WRITE_MODES = {"append", "overwrite", "ignore", "error", "errorifexists"}


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {uri}")
    remainder = uri[5:]
    bucket, sep, key = remainder.partition("/")
    if not bucket or not sep:
        raise ValueError(f"Expected S3 URI with bucket and key, got: {uri}")
    return bucket, key


def load_snapshot_date(input_path: str) -> tuple[str, str]:
    manifest_uri = f"{input_path.rstrip('/')}/manifest.json"
    bucket, key = parse_s3_uri(manifest_uri)
    s3 = boto3.client("s3")
    payload = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    manifest = json.loads(payload.decode("utf-8"))

    snapshot_date = str(manifest.get("date", "")).strip()
    if not snapshot_date:
        raise ValueError(f"Missing or empty 'date' field in manifest: {manifest_uri}")

    return snapshot_date, manifest_uri


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--JOB_NAME", required=True)
    parser.add_argument("--INPUT_PATH", default=DEFAULT_INPUT_PATH)
    parser.add_argument("--OUTPUT_PATH", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--OPENALEX_PREFIX", default=DEFAULT_OPENALEX_PREFIX)
    parser.add_argument("--SUBSTRING_LEN", type=int, default=DEFAULT_SUBSTRING_LEN)
    parser.add_argument("--WRITE_MODE", default=DEFAULT_WRITE_MODE)

    args, _ = parser.parse_known_args(argv[1:])

    write_mode_normalized = str(args.WRITE_MODE).strip().lower()
    if write_mode_normalized not in VALID_WRITE_MODES:
        allowed = ", ".join(sorted(VALID_WRITE_MODES))
        raise ValueError(f"Unsupported --WRITE_MODE={args.WRITE_MODE!r}. Allowed: {allowed}")
    args.WRITE_MODE = write_mode_normalized

    return args


def build_work_schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType()),
            StructField("doi", StringType()),
            StructField("title", StringType()),
            StructField("abstract_inverted_index", MapType(StringType(), ArrayType(IntegerType()))),
            StructField("language", StringType()),
            StructField("type", StringType()),
            StructField("type_crossref", StringType()),
            StructField("publication_year", IntegerType()),
            StructField("cited_by_count", LongType()),
            StructField("referenced_works", ArrayType(StringType())),
            StructField(
                "primary_location",
                StructType(
                    [
                        StructField(
                            "source",
                            StructType(
                                [
                                    StructField("display_name", StringType()),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
            StructField(
                "authorships",
                ArrayType(
                    StructType(
                        [
                            StructField(
                                "author",
                                StructType(
                                    [
                                        StructField("display_name", StringType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "institutions",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField("display_name", StringType()),
                                            StructField("country_code", StringType()),
                                            StructField("ror", StringType()),
                                            StructField("type", StringType()),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    )
                ),
            ),
        ]
    )


def main(argv: list[str]) -> None:
    args = parse_args(argv)
    prefix_len = len(args.OPENALEX_PREFIX) + 1
    snapshot_date, manifest_uri = load_snapshot_date(args.INPUT_PATH)
    resolved_output_path = args.OUTPUT_PATH.replace("{SNAPSHOT_DATE}", snapshot_date)

    print("[config] JOB_NAME=", args.JOB_NAME)
    print("[config] INPUT_PATH=", args.INPUT_PATH)
    print("[config] MANIFEST_URI=", manifest_uri)
    print("[config] SNAPSHOT_DATE=", snapshot_date)
    print("[config] OUTPUT_PATH_TEMPLATE=", args.OUTPUT_PATH)
    print("[config] OUTPUT_PATH_RESOLVED=", resolved_output_path)
    print("[config] OPENALEX_PREFIX=", args.OPENALEX_PREFIX)
    print("[config] SUBSTRING_LEN=", args.SUBSTRING_LEN)
    print("[config] WRITE_MODE=", args.WRITE_MODE)

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args.JOB_NAME, {"JOB_NAME": args.JOB_NAME})

    work_schema = build_work_schema()

    print("Reading raw text data from S3...")
    raw_df = spark.read.text(args.INPUT_PATH)

    print("Parsing JSONL to structured dataframe...")
    parsed_df = (
        raw_df.select(from_json(col("value"), work_schema).alias("work"))
        .select("work.*")
        .filter(col("id").isNotNull())
    )
    parsed_df.cache()

    print("Creating nodes dataset...")
    base_nodes_df = (
        parsed_df.withColumn("temp_id", monotonically_increasing_id())
        .withColumn("id", substring(col("id"), prefix_len, args.SUBSTRING_LEN))
        # OpenAlex stores abstract text as an inverted index map (token -> positions).
        # Reconstruct a plain-text abstract by sorting tokens by position and joining with spaces.
        .withColumn(
            "abstract",
            expr(
                """
                CASE
                    WHEN abstract_inverted_index IS NULL THEN NULL
                    ELSE array_join(
                        transform(
                            array_sort(
                                flatten(
                                    transform(
                                        map_entries(abstract_inverted_index),
                                        kv -> transform(kv.value, pos -> named_struct('pos', pos, 'token', kv.key))
                                    )
                                )
                            ),
                            x -> x.token
                        ),
                        ' '
                    )
                END
                """
            ),
        )
        .withColumn("publication_source", col("primary_location.source.display_name"))
        .withColumn(
            "institutions",
            array_distinct(flatten(transform(col("authorships"), lambda x: x.institutions.display_name))),
        )
        .withColumn(
            "countries",
            array_distinct(flatten(transform(col("authorships"), lambda x: x.institutions.country_code))),
        )
        .withColumn(
            "institutions_ror",
            array_distinct(flatten(transform(col("authorships"), lambda x: x.institutions.ror))),
        )
        .withColumn(
            "institutions_type",
            array_distinct(flatten(transform(col("authorships"), lambda x: x.institutions.type))),
        )
    )

    exploded_authors_df = base_nodes_df.select("temp_id", "authorships").withColumn(
        "authorship_exploded", explode(col("authorships"))
    )

    author_ids_df = (
        exploded_authors_df.select(
            "temp_id",
            col("authorship_exploded.author.display_name").alias("author_id"),
        )
        .groupBy("temp_id")
        .agg(collect_list("author_id").alias("authors"))
    )

    nodes_df = base_nodes_df.join(author_ids_df, "temp_id", "left").select(
        "id",
        "doi",
        "title",
        "abstract",
        "language",
        col("type").alias("type_openalex"),
        "type_crossref",
        "publication_year",
        col("cited_by_count").alias("citations"),
        "publication_source",
        "countries",
        "institutions",
        "institutions_ror",
        "institutions_type",
        "authors",
    )

    print("Creating edges dataset...")
    edges_df = (
        parsed_df.select(
            substring(col("id"), prefix_len, args.SUBSTRING_LEN).alias("from_id"),
            explode(col("referenced_works")).alias("to_raw"),
        )
        .select(
            col("from_id").alias("from"),
            substring(col("to_raw"), prefix_len, args.SUBSTRING_LEN).alias("to"),
            lit(1).alias("weight"),
        )
        .filter(col("from").isNotNull() & col("to").isNotNull() & (col("to") != ""))
        .distinct()
    )

    nodes_count = nodes_df.count()
    edges_count = edges_df.count()
    print(f"Total nodes to write: {nodes_count}")
    print(f"Total edges to write: {edges_count}")

    print("Writing parquet outputs...")
    nodes_df.write.mode(args.WRITE_MODE).partitionBy("publication_year").parquet(
        f"{resolved_output_path}/nodes_snapshot/"
    )
    edges_df.write.mode(args.WRITE_MODE).parquet(f"{resolved_output_path}/edges_snapshot/")

    parsed_df.unpersist()
    job.commit()
    print("Data processing complete.")


if __name__ == "__main__":
    main(sys.argv)
