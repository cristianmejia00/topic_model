"""Shared defaults/helpers for subqueries scripts."""

from __future__ import annotations

import os

DEFAULT_DATABASE = "q20260629"
DEFAULT_QUERY_FOLDER_TOPIC = "quantum_computing"
DEFAULT_QUERY_FOLDER_FILTERS = "filters_ave_py_ge_2022_and_recency_py_ge_0_4"
DEFAULT_QUERY_FOLDER_PASSTHROUGH = "everything"


def _clean(value: str | None, default: str) -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def resolve_database(cli_value: str | None) -> str:
    return _clean(cli_value, _clean(os.getenv("TOPIC_MODEL_DATABASE"), DEFAULT_DATABASE))


def resolve_query_folder(cli_value: str | None, default_query_folder: str) -> str:
    return _clean(cli_value, _clean(os.getenv("TOPIC_MODEL_QUERY_FOLDER"), default_query_folder))


def classification_root(database: str) -> str:
    return f"s3://openalex-outputs/classification/{database}/"


def subqueries_root(database: str) -> str:
    return f"{classification_root(database)}subqueries/"


def keywords_dir(database: str) -> str:
    return f"{classification_root(database)}bertopic/"


def macro_name_path(database: str) -> str:
    return f"{classification_root(database)}cluster_name_macro/"
