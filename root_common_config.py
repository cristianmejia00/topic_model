"""Shared configuration and guards for root-level pipeline scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import awswrangler as wr


DB_ENV_VAR = "TOPIC_MODEL_DATABASE"


@dataclass(frozen=True)
class RootPaths:
    database: str

    @property
    def classification_root(self) -> str:
        return f"s3://openalex-outputs/classification/{self.database}/"

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


def resolve_database_from_env() -> str:
    """Require database to be provided via entrypoint environment."""
    value = os.getenv(DB_ENV_VAR, "").strip()
    if not value:
        raise RuntimeError(
            f"Missing required environment variable {DB_ENV_VAR}. "
            "Run scripts through run_root_pipeline.py with --database."
        )
    return value


def get_root_paths() -> RootPaths:
    return RootPaths(database=resolve_database_from_env())


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
