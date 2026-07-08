"""Shared configuration and guards for root-level pipeline scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import awswrangler as wr


DB_ENV_VAR = "TOPIC_MODEL_DATABASE"
SNAPSHOT_ENV_VAR = "TOPIC_MODEL_SNAPSHOT"
QUERY_ENV_VAR = "TOPIC_MODEL_QUERY"


@dataclass(frozen=True)
class RootPaths:
    snapshot: str
    query: str

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
    def classification_root(self) -> str:
        # Backward-compatible alias used by existing step-05 scripts.
        return self.clustering_root

    @property
    def bertopic_root(self) -> str:
        return f"{self.classification_root}bertopic/"

    @property
    def bertopic_images_root(self) -> str:
        return f"{self.classification_root}bertopic/images/"

    @property
    def micro_report(self) -> str:
        return f"{self.classification_root}cluster_report_micro/"

    @property
    def macro_report(self) -> str:
        return f"{self.classification_root}cluster_report_macro/"

    @property
    def meso_report(self) -> str:
        return f"{self.classification_root}cluster_report_meso/"

    @property
    def cluster_color_macro(self) -> str:
        return f"{self.classification_root}cluster_color_macro/"

    @property
    def cluster_name_macro(self) -> str:
        return f"{self.classification_root}cluster_name_macro/"

    @property
    def article_report(self) -> str:
        return f"{self.classification_root}article_report/"


def build_database_name(snapshot: str, query: str) -> str:
    snap = str(snapshot or "").strip()
    qry = str(query or "").strip()
    if not snap:
        raise RuntimeError("Snapshot is required to derive database name.")
    if not qry:
        raise RuntimeError("Query is required to derive database name.")
    return f"snapshot_{snap}-{qry}"


def resolve_snapshot_from_env() -> str:
    value = os.getenv(SNAPSHOT_ENV_VAR, "").strip()
    if not value:
        raise RuntimeError(
            f"Missing required environment variable {SNAPSHOT_ENV_VAR}. "
            "Run scripts through run_root_pipeline.py with --snapshot."
        )
    return value


def resolve_query_from_env() -> str:
    value = os.getenv(QUERY_ENV_VAR, "").strip()
    if not value:
        raise RuntimeError(
            f"Missing required environment variable {QUERY_ENV_VAR}. "
            "Run scripts through run_root_pipeline.py with --query."
        )
    return value


def get_root_paths() -> RootPaths:
    return RootPaths(
        snapshot=resolve_snapshot_from_env(),
        query=resolve_query_from_env(),
    )


def s3_prefix_has_objects(prefix: str) -> bool:
    try:
        objs = wr.s3.list_objects(prefix)
    except Exception:
        return False
    return bool(objs)


def ensure_outputs_writable(*, s3_prefixes: list[str], local_paths: list[Path], force: bool) -> None:
    """Fail if outputs already exist unless force=True."""
    conflicts: list[str] = []

    for prefix in s3_prefixes:
        if s3_prefix_has_objects(prefix):
            conflicts.append(f"S3 prefix exists: {prefix}")

    for path in local_paths:
        if path.exists():
            conflicts.append(f"Local path exists: {path}")

    if conflicts and not force:
        details = "\n".join(f"  - {item}" for item in conflicts)
        raise RuntimeError(
            "Refusing to overwrite existing outputs. Re-run with --force to allow overwrite.\n"
            f"Conflicts:\n{details}"
        )
