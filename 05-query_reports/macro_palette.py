"""
Shared helpers for stable macro-cluster colors.
"""

from __future__ import annotations

import colorsys

import awswrangler as wr

from root_common_config import get_root_paths


def _default_macro_color_path() -> str:
    try:
        return get_root_paths().cluster_color_macro
    except Exception:
        return ""


MACRO_COLOR_PATH = _default_macro_color_path()


def stable_color_hex(macro_id: int) -> str:
    """Deterministic color assignment that is stable for each macro id."""
    hue = (int(macro_id) * 0.6180339887498949) % 1.0
    r, g, b = colorsys.hsv_to_rgb(hue, 0.62, 0.85)
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"


def load_macro_color_map(path: str = MACRO_COLOR_PATH) -> dict[int, str]:
    """Return {macro_cluster: color_hex}. Empty dict on read/schema issues."""
    if not path:
        print("[warn] macro color path is not configured; using deterministic fallback")
        return {}
    try:
        p = wr.s3.read_parquet(path)
        if {"macro_cluster", "color_hex"}.issubset(p.columns):
            return dict(zip(p["macro_cluster"].astype("int64"), p["color_hex"].astype(str)))
        print("[warn] macro color palette missing expected columns; using deterministic fallback")
    except Exception as exc:
        print(f"[warn] could not read macro color palette at {path}: {exc}")
    return {}


def color_for_macro(macro_id: int, color_map: dict[int, str] | None = None) -> str:
    if color_map is None:
        color_map = {}
    return color_map.get(int(macro_id), stable_color_hex(int(macro_id)))
