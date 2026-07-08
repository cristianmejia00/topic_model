"""Shared context and path helpers for step-06 subquery scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass


SNAPSHOT_ENV_VAR = "TOPIC_MODEL_SNAPSHOT"
QUERY_ENV_VAR = "TOPIC_MODEL_QUERY"
SUBQUERY_ENV_VAR = "TOPIC_MODEL_SUBQUERY"
LEGACY_SUBQUERY_ENV_VAR = "TOPIC_MODEL_QUERY_FOLDER"

DEFAULT_STAGING = "s3://openalex-outputs/athena-staging/"
DEFAULT_WORKGROUP = "primary"


def _clean(value: str | None) -> str:
    return "" if value is None else str(value).strip()


def _require(value: str, *, cli_hint: str) -> str:
    if value:
        return value
    raise RuntimeError(f"Missing required value. Provide {cli_hint} or matching environment variable.")


def resolve_snapshot(cli_value: str | None) -> str:
    return _require(
        _clean(cli_value) or _clean(os.getenv(SNAPSHOT_ENV_VAR)),
        cli_hint="--snapshot / TOPIC_MODEL_SNAPSHOT",
    )


def resolve_query(cli_value: str | None) -> str:
    return _require(
        _clean(cli_value) or _clean(os.getenv(QUERY_ENV_VAR)),
        cli_hint="--query / TOPIC_MODEL_QUERY",
    )


def resolve_subquery(*, cli_subquery: str | None, cli_query_folder: str | None) -> str:
    canonical = _clean(cli_subquery)
    alias_cli = _clean(cli_query_folder)
    env_canonical = _clean(os.getenv(SUBQUERY_ENV_VAR))
    env_alias = _clean(os.getenv(LEGACY_SUBQUERY_ENV_VAR))

    value = canonical or alias_cli or env_canonical or env_alias
    return _require(value, cli_hint="--subquery / TOPIC_MODEL_SUBQUERY")


def build_database_name(snapshot: str, query: str) -> str:
    snap = _clean(snapshot)
    qry = _clean(query)
    if not snap:
        raise RuntimeError("Snapshot is required to derive database name.")
    if not qry:
        raise RuntimeError("Query is required to derive database name.")
    return f"snapshot_{snap}-{qry}"


@dataclass(frozen=True)
class SubqueryPaths:
    snapshot: str
    query: str
    subquery: str

    @property
    def database(self) -> str:
        return build_database_name(self.snapshot, self.query)

    @property
    def results_root(self) -> str:
        return f"s3://openalex-results/snapshot_{self.snapshot}/queries/{self.query}/"

    @property
    def network_root(self) -> str:
        return f"{self.results_root}network/"

    @property
    def clustering_root(self) -> str:
        return f"{self.network_root}clustering/"

    @property
    def bertopic_root(self) -> str:
        return f"{self.clustering_root}bertopic/"

    @property
    def macro_name_path(self) -> str:
        return f"{self.clustering_root}cluster_name_macro/"

    @property
    def subqueries_root(self) -> str:
        return f"{self.clustering_root}subqueries/"

    @property
    def subquery_base(self) -> str:
        return f"{self.subqueries_root}{self.subquery}/"


def resolve_paths(
    *,
    snapshot: str | None,
    query: str | None,
    subquery: str | None,
    query_folder: str | None = None,
) -> SubqueryPaths:
    snap = resolve_snapshot(snapshot)
    qry = resolve_query(query)
    sub = resolve_subquery(cli_subquery=subquery, cli_query_folder=query_folder)
    return SubqueryPaths(snapshot=snap, query=qry, subquery=sub)
