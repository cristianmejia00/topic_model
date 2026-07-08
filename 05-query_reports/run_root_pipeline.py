"""
Single entrypoint for root-level pipeline scripts.

Design:
- Database is required (no default).
- Snapshot and query are required for S3 layout resolution.
- All root scripts receive context via TOPIC_MODEL_DATABASE/TOPIC_MODEL_SNAPSHOT/TOPIC_MODEL_QUERY.
- Existing outputs are blocked unless --force is provided.

Examples:
    .venv/bin/python run_root_pipeline.py --database q20260629 --snapshot 2026-06-26 --query q20260629 --step bertopic
    .venv/bin/python run_root_pipeline.py --database q20260629 --snapshot 2026-06-26 --query q20260629 --step macro_colors --step macro_names
    .venv/bin/python run_root_pipeline.py --database q20260629 --snapshot 2026-06-26 --query q20260629 --step athena_reports --force
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from root_common_config import (
    DB_ENV_VAR,
    QUERY_ENV_VAR,
    RootPaths,
    SNAPSHOT_ENV_VAR,
    ensure_outputs_writable,
)


ALL_STEPS = [
    "meso_as_micro",
    "athena_reports",
    "audit_hierarchy",
    "bertopic",
    "macro_colors",
    "macro_names",
    "plot_embeds",
    "plot_images",
    "check_macro_plot",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run root-level pipeline scripts with required database and overwrite guards."
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Glue/Athena database id, e.g. q20260629 (required).",
    )
    parser.add_argument(
        "--snapshot",
        required=True,
        help="Snapshot token used in S3 paths, e.g. 2026-06-26 (required).",
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Query token used in S3 paths, e.g. q20260629 (required).",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Optional version tag required for meso_as_micro step, e.g. version3.",
    )
    parser.add_argument(
        "--step",
        action="append",
        choices=ALL_STEPS,
        required=True,
        help="Pipeline step to run. Repeat to run multiple steps in order.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow overwriting existing outputs. Without this, existing outputs raise an error.",
    )
    parser.add_argument(
        "--staging",
        default="s3://openalex-outputs/athena-staging/",
        help="Athena staging path (used by athena_reports/audit_hierarchy steps).",
    )
    parser.add_argument(
        "--workgroup",
        default="primary",
        help="Athena workgroup (used by athena_reports step).",
    )
    parser.add_argument(
        "--show-limit",
        type=int,
        default=25,
        help="Problem row sample size (used by audit_hierarchy step).",
    )
    return parser.parse_args()


def step_outputs(step: str, paths: RootPaths, repo_root: Path) -> tuple[list[str], list[Path]]:
    s3: list[str] = []
    local: list[Path] = []

    if step == "meso_as_micro":
        # Migration step enforces idempotency/lineage at table level.
        pass

    elif step == "athena_reports":
        s3.extend([
            paths.article_report,
            paths.micro_report,
            paths.meso_report,
            paths.macro_report,
        ])

    elif step == "bertopic":
        s3.extend([
            f"{paths.bertopic_root}micro/",
            f"{paths.bertopic_root}meso/",
            f"{paths.bertopic_root}macro/",
            f"{paths.bertopic_root}micro_embeddings/",
            f"{paths.bertopic_root}meso_embeddings/",
            f"{paths.bertopic_root}macro_embeddings/",
            f"{paths.bertopic_root}documents/",
        ])
        local.extend([
            repo_root / "_bertopic_cache" / "doc_embeddings.npy",
            repo_root / "_bertopic_cache" / "doc_ids.npy",
            repo_root / "_bertopic_cache" / "micro_ids.npy",
            repo_root / "_bertopic_cache" / "micro_vecs.npy",
            repo_root / "_bertopic_cache" / "meso_ids.npy",
            repo_root / "_bertopic_cache" / "meso_vecs.npy",
            repo_root / "_bertopic_cache" / "macro_ids.npy",
            repo_root / "_bertopic_cache" / "macro_vecs.npy",
        ])

    elif step == "macro_colors":
        s3.append(paths.cluster_color_macro)

    elif step == "macro_names":
        s3.append(paths.cluster_name_macro)

    elif step == "plot_embeds":
        s3.extend([
            f"{paths.bertopic_images_root}micro/",
            f"{paths.bertopic_images_root}meso/",
            f"{paths.bertopic_images_root}macro/",
            f"{paths.bertopic_images_root}micro_embeddings/",
            f"{paths.bertopic_images_root}meso_embeddings/",
            f"{paths.bertopic_images_root}macro_embeddings/",
        ])
        local.extend([
            repo_root / "_viz_cache" / "micro_vecs.npy",
            repo_root / "_viz_cache" / "meso_vecs.npy",
            repo_root / "_viz_cache" / "macro_vecs.npy",
        ])

    elif step == "plot_images":
        local.extend([
            repo_root / "main_plots" / "cluster_map.png",
            repo_root / "main_plots" / "cluster_map.pdf",
        ])

    elif step == "check_macro_plot":
        local.append(repo_root / "main_plots" / "macro_check.png")

    elif step == "audit_hierarchy":
        # Read-only step.
        pass

    return s3, local


def step_command(step: str, repo_root: Path, args: argparse.Namespace) -> list[str]:
    py = sys.executable
    if step == "meso_as_micro":
        cmd = [
            py,
            str(repo_root / "migrate_meso_as_micro.py"),
            "--database",
            args.database,
            "--snapshot",
            args.snapshot,
            "--query",
            args.query,
            "--version",
            str(args.version),
            "--staging",
            args.staging,
            "--workgroup",
            args.workgroup,
        ]
        if args.force:
            cmd.append("--force")
        return cmd

    if step == "athena_reports":
        cmd = [
            py,
            str(repo_root / "create_athena_reports.py"),
            "--database",
            args.database,
            "--snapshot",
            args.snapshot,
            "--query",
            args.query,
            "--staging",
            args.staging,
            "--workgroup",
            args.workgroup,
        ]
        if args.force:
            cmd.append("--overwrite")
        return cmd

    if step == "audit_hierarchy":
        return [
            py,
            str(repo_root / "audit_athena_hierarchy.py"),
            "--database",
            args.database,
            "--staging",
            args.staging,
            "--show-limit",
            str(args.show_limit),
        ]

    script_map = {
        "bertopic": repo_root / "cluster_bertopic.py",
        "macro_colors": repo_root / "create_macro_color_palette.py",
        "macro_names": repo_root / "name_macro_clusters.py",
        "plot_embeds": repo_root / "main_plots" / "plot_embeds.py",
        "plot_images": repo_root / "main_plots" / "plot_images.py",
        "check_macro_plot": repo_root / "main_plots" / "check_macro_plot.py",
    }
    return [py, str(script_map[step])]


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent
    paths = RootPaths(database=args.database, snapshot=args.snapshot, query=args.query)

    if "meso_as_micro" in args.step and not str(args.version or "").strip():
        raise RuntimeError("--version is required when running --step meso_as_micro")

    env = os.environ.copy()
    env[DB_ENV_VAR] = args.database
    env[SNAPSHOT_ENV_VAR] = args.snapshot
    env[QUERY_ENV_VAR] = args.query

    print(f"[entrypoint] database={args.database}")
    print(f"[entrypoint] snapshot={args.snapshot}")
    print(f"[entrypoint] query={args.query}")
    print(f"[entrypoint] force={args.force}")

    for step in args.step:
        s3_prefixes, local_paths = step_outputs(step, paths, repo_root)
        ensure_outputs_writable(
            s3_prefixes=s3_prefixes,
            local_paths=local_paths,
            force=args.force,
        )

        cmd = step_command(step, repo_root, args)
        print(f"\n[step] {step}")
        print("[run]", " ".join(cmd))
        subprocess.run(cmd, check=True, env=env)

    print("\n[success] selected root pipeline steps finished")


if __name__ == "__main__":
    main()
