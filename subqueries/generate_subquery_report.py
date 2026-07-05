"""
generate_subquery_report.py
===========================
Build a static HTML report for one subquery and publish it both locally (for GitHub)
and to S3 under the query's report/ prefix.

Outputs:
  docs/{DATABASE}/{QUERY_FOLDER}/report/index.html
  s3://.../subqueries/{QUERY_FOLDER}/report/index.html

Requirements addressed:
  - Clusters sorted by publication count (descending)
  - Display IDs assigned from 1..n after sorting
  - Country names converted from ISO-2 to natural names
  - Tab 1: cluster list + details pane
  - Tab 2: full table + interactive scatter plot
"""

from __future__ import annotations

import argparse
import json
import math
import mimetypes
from pathlib import Path
import sys
from typing import Any
from urllib.parse import urlparse

import awswrangler as wr
import boto3
import pandas as pd
import pycountry

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
  sys.path.insert(0, str(ROOT))

from common_config import (
    DEFAULT_QUERY_FOLDER_TOPIC,
    keywords_dir,
    macro_name_path,
    resolve_database,
    resolve_query_folder,
    subqueries_root,
)
from macro_palette import color_for_macro, load_macro_color_map


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DATABASE = "q20260629"
QUERY_FOLDER = DEFAULT_QUERY_FOLDER_TOPIC

SUBQUERIES_ROOT = subqueries_root(DATABASE)
KEYWORDS_DIR = keywords_dir(DATABASE)
MACRO_NAME_PATH = macro_name_path(DATABASE)
LOCAL_DOCS_ROOT = ROOT / "docs" 

TOP_TITLES = 10
USE_MACRO_CLUSTER = True
TITLE = "Artificial Intelligence"
SUBTITLE = "OpenAlex 20260625 | Articles | 1990-2026 | ‘artificial intelligence|machine learning|neural networks?|large language models?|deep learning|natural language processing|generative ai|transformer models?|retrieval-augmented generation|foundation models?’ | Direct Citations | Louvain Clustering"


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Generate a static HTML report for one subquery folder."
  )
  parser.add_argument(
    "--database",
    default=None,
    help="Classification database id, e.g. q20260629.",
  )
  parser.add_argument(
    "--query-folder",
    default=None,
    help="Subquery folder name to read/write under subqueries/.",
  )
  return parser.parse_args()


def load_macro_colors() -> dict[int, str]:
  return load_macro_color_map()


def load_macro_names() -> dict[int, str]:
  try:
    p = wr.s3.read_parquet(MACRO_NAME_PATH)
    if {"macro_cluster", "name"}.issubset(p.columns):
      return dict(zip(p["macro_cluster"].astype("int64"), p["name"].astype(str).str.strip()))
    print("[warn] macro name table missing expected columns")
  except Exception as exc:
    print(f"[warn] could not read macro names at {MACRO_NAME_PATH}: {exc}")
  return {}


def pick_col(df: pd.DataFrame, candidates: list[str], required: bool = False) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(f"Missing required columns. Expected one of: {candidates}")
    return None


def read_subset(base: str, name: str, required: bool = True) -> pd.DataFrame:
    path = f"{base}{name}/"
    try:
        return wr.s3.read_parquet(path)
    except Exception as exc:
        if required:
            raise RuntimeError(f"Could not read required subset at {path}: {exc}") from exc
        print(f"[warn] optional subset not available at {path}: {exc}")
        return pd.DataFrame()


def iso2_to_country_name(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if len(raw) == 2 and raw.isalpha():
        hit = pycountry.countries.get(alpha_2=raw.upper())
        if hit is not None:
            raw = hit.name
    # Normalize to preferred display naming.
    if raw == "Taiwan, Province of China":
        return "Taiwan"
    return raw


def is_blank_like(value: Any) -> bool:
    text = sanitize_text(value).strip().lower()
    return text in {"", "none", "nan", "null", "<na>", "na", "n/a"}


def sanitize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def keyword_fallback(kw: str, max_terms: int = 5) -> str:
    parts = [p.strip() for p in sanitize_text(kw).split(",") if p.strip()]
    if not parts:
        return "Unnamed Cluster"
    return " ".join(parts[:max_terms]).title()


def build_plot_data(summary_rows: list[dict[str, Any]], use_macro_cluster: bool = True) -> dict[str, Any]:
  by_macro: dict[str, dict[str, list[Any]]] = {}
  for r in summary_rows:
    macro = str(r.get("macro_cluster_label", "Unknown")) if use_macro_cluster else "All Clusters"
    if macro not in by_macro:
      by_macro[macro] = {
        "color": r.get("macro_color", "#7f7f7f") if use_macro_cluster else "#005f73",
        "x": [],
        "y": [],
        "size": [],
        "text": [],
        "custom": [],
      }
    by_macro[macro]["x"].append(r.get("avg_publication_year"))
    by_macro[macro]["y"].append(r.get("ranked_citation_score"))
    by_macro[macro]["size"].append(r.get("publications", 0))
    by_macro[macro]["text"].append(f"{r['display_id']}-{r['short_name']}")
    by_macro[macro]["custom"].append(
      [
        r["display_id"],
        r["global_id"],
        r["short_name"],
        r["name"],
        r["publications"],
        r.get("avg_publication_year"),
        r.get("avg_citation"),
        r.get("ranked_citation_score"),
      ]
    )
  return by_macro


def build_html(report_json: str) -> str:
    template = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Subquery Report</title>
  <script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>
  <style>
    :root {{
      --bg: #f7f6f3;
      --panel: #ffffff;
      --ink: #1f2a36;
      --muted: #6d747d;
      --line: #d8d9de;
      --brand: #005f73;
      --brand-soft: #d9eff1;
      --accent: #bb3e03;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Source Sans 3", "Segoe UI", "Helvetica Neue", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, #f0d7c9 0%, transparent 35%),
        radial-gradient(circle at bottom left, #d5e8ec 0%, transparent 45%),
        var(--bg);
      min-height: 100vh;
    }}
    .wrap {{ max-width: 1440px; margin: 0 auto; padding: 16px; }}
    .report-head {{
      margin: 2px 0 12px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: linear-gradient(180deg, #ffffff, #f9fbfc);
      box-shadow: 0 3px 14px rgba(0,0,0,0.04);
    }}
    .report-title {{ margin: 0 0 6px; font-size: 24px; line-height: 1.25; }}
    .report-subtitle {{ margin: 0; color: var(--muted); font-size: 14px; }}
    .tabs {{
      display: flex;
      gap: 8px;
      position: sticky;
      top: 0;
      z-index: 3;
      background: linear-gradient(180deg, rgba(247,246,243,0.97), rgba(247,246,243,0.88));
      backdrop-filter: blur(4px);
      padding: 10px 0;
    }}
    .tab-btn {{
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--ink);
      padding: 10px 14px;
      border-radius: 10px;
      font-weight: 700;
      cursor: pointer;
    }}
    .tab-btn.active {{
      background: var(--brand);
      color: white;
      border-color: var(--brand);
    }}
    .tab {{ display: none; }}
    .tab.active {{ display: block; }}
    .layout {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 14px;
      min-height: calc(100vh - 120px);
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      overflow: hidden;
      box-shadow: 0 4px 16px rgba(0,0,0,0.05);
    }}
    .left-head {{
      padding: 12px;
      font-weight: 800;
      border-bottom: 1px solid var(--line);
      background: #f0f8f9;
    }}
    .cluster-list {{
      list-style: none;
      margin: 0;
      padding: 0;
      overflow: auto;
      max-height: calc(100vh - 190px);
    }}
    .cluster-item {{
      padding: 10px 12px;
      border-bottom: 1px solid #eff0f3;
      cursor: pointer;
      font-size: 14px;
      transition: background 0.15s ease;
    }}
    .cluster-item:hover {{ background: #f5fbfb; }}
    .cluster-item.active {{
      background: var(--brand-soft);
      border-left: 4px solid var(--brand);
      padding-left: 8px;
      font-weight: 700;
    }}
    .detail {{ padding: 16px 18px 22px; }}
    .title {{ margin: 0 0 6px; font-size: 24px; line-height: 1.28; }}
    .subtitle {{ margin: 0 0 8px; color: var(--accent); font-size: 18px; font-weight: 700; }}
    .desc {{ margin: 0 0 8px; line-height: 1.5; }}
    .meta {{ margin: 0 0 14px; color: var(--muted); font-size: 13px; }}
    .section {{ margin-top: 14px; }}
    .section h3 {{ margin: 0 0 8px; font-size: 16px; }}
    .paper-list {{ margin: 0; padding-left: 18px; line-height: 1.45; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      background: white;
    }}
    th, td {{ border: 1px solid var(--line); padding: 7px 8px; text-align: left; }}
    th {{ background: #f3f7fa; }}
    tr.highlight-row td {{ background: #fff3a6; }}
    .tab2-section {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px;
      margin-bottom: 12px;
      box-shadow: 0 4px 16px rgba(0,0,0,0.05);
    }}
    .summary-meta {{
      margin: 4px 0 8px;
      color: var(--muted);
      font-size: 13px;
    }}
    .summary-scroll {{
      max-height: 420px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
    }}
    .summary-scroll thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: #eaf1f4;
    }}
    .name-details {{ margin: 0; }}
    .name-details > summary {{
      cursor: pointer;
      color: var(--brand);
      font-weight: 700;
      list-style: disclosure-closed;
    }}
    .name-details[open] > summary {{ list-style: disclosure-open; }}
    .name-desc {{
      margin-top: 6px;
      color: #324150;
      line-height: 1.4;
      max-width: 52ch;
      white-space: normal;
    }}
    #scatter {{ width: 100%; height: 680px; }}
    @media (max-width: 960px) {{
      .layout {{ grid-template-columns: 1fr; min-height: auto; }}
      .cluster-list {{ max-height: 280px; }}
      .summary-scroll {{ max-height: 300px; }}
      #scatter {{ height: 520px; }}
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"report-head\">
      <h1 class=\"report-title\" id=\"report-title\"></h1>
      <p class=\"report-subtitle\" id=\"report-subtitle\"></p>
    </div>

    <div class=\"tabs\">
      <button id=\"btn-tab1\" class=\"tab-btn active\">Clusters</button>
      <button id=\"btn-tab2\" class=\"tab-btn\">Summary & Scatter</button>
    </div>

    <section id=\"tab1\" class=\"tab active\">
      <div class=\"layout\">
        <aside class=\"panel\">
          <div class=\"left-head\">Clusters</div>
          <ul id=\"cluster-list\" class=\"cluster-list\"></ul>
        </aside>
        <main class=\"panel detail\" id=\"cluster-detail\"></main>
      </div>
    </section>

    <section id=\"tab2\" class=\"tab\">
      <div class=\"tab2-section\">
        <h3>Full Micro Cluster Table</h3>
        <div id=\"summary-meta\" class=\"summary-meta\"></div>
        <div class=\"summary-scroll\">
          <div id=\"summary-table\"></div>
        </div>
      </div>
      <div class=\"tab2-section\">
        <h3>Average Publication Year vs Ranked Citation</h3>
        <div id=\"scatter\"></div>
      </div>
    </section>
  </div>

  <script>
    const REPORT = __REPORT_JSON__;

    function fmtNumber(v, digits = 2) {{
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
      return Number(v).toFixed(digits);
    }}

    function fmtInt(v) {{
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
      return Number(v).toLocaleString();
    }}

    function escapeHtml(value) {{
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }}

    function tableHtml(headers, rows, headerAttrs = null) {{
      const head = `<tr>${{headers.map((h, i) => `<th ${{headerAttrs ? (headerAttrs[i] || "") : ""}}>${{h}}</th>`).join("")}}</tr>`;
      const body = rows.map(r => `<tr>${{r.map(c => `<td>${{c}}</td>`).join("")}}</tr>`).join("");
      return `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
    }}

    function tableHtmlRows(headers, rows) {{
      const head = `<tr>${{headers.map(h => `<th>${{h}}</th>`).join("")}}</tr>`;
      const body = rows.map(r => {{
        const cls = r.highlight ? ' class="highlight-row"' : "";
          return `<tr${{cls}}><td>${{r.label}}</td><td>${{fmtInt(r.freq)}}</td><td>${{fmtNumber(r.avg_publication_year, 1)}}</td><td>${{fmtNumber(r.avg_citation, 2)}}</td></tr>`;
      }}).join("");
      return `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
    }}

    function buildClusterList() {{
      const ul = document.getElementById("cluster-list");
      ul.innerHTML = "";
      REPORT.clusters.forEach((c, idx) => {{
        const li = document.createElement("li");
        li.className = "cluster-item" + (idx === 0 ? " active" : "");
        li.textContent = `${{c.display_id}}-${{c.short_name}}`;
        li.addEventListener("click", () => selectCluster(c.global_id));
        li.dataset.clusterId = String(c.global_id);
        ul.appendChild(li);
      }});
    }}

    function selectCluster(globalId) {{
      const cluster = REPORT.clusters.find(c => c.global_id === globalId);
      if (!cluster) return;

      document.querySelectorAll('.cluster-item').forEach(el => {{
        const active = Number(el.dataset.clusterId) === Number(globalId);
        el.classList.toggle('active', active);
      }});

      const papers = cluster.top_titles || [];
      const paperRows = papers.map(p =>
        `<li>[${{fmtInt(p.citations)}} cites${{p.publication_year ? " | " + p.publication_year : ""}}] ${{p.title}}</li>`
      ).join("");

      const countryRows = (cluster.countries || [])
        .filter(r => (r.country || "").trim().length > 0)
        .map(r => ({
          label: r.country,
          freq: r.freq,
          avg_publication_year: r.avg_publication_year,
          avg_citation: r.avg_citation,
          highlight: String(r.country || "").toLowerCase() === "japan",
        }));

      const instRows = (cluster.institutions || [])
        .filter(r => (r.institution || "").trim().length > 0)
        .map(r => ({
          label: r.institution,
          freq: r.freq,
          avg_publication_year: r.avg_publication_year,
          avg_citation: r.avg_citation,
          highlight: String(r.institution || "").toLowerCase().includes("tokyo"),
        }));

      const detail = document.getElementById("cluster-detail");
      detail.innerHTML = `
        <h1 class=\"title\">${{cluster.display_id}}-${{cluster.short_name}} (${{fmtInt(cluster.publications)}}, ${{fmtNumber(cluster.avg_publication_year, 1)}}, ${{fmtNumber(cluster.avg_citation, 2)}}, ${{fmtNumber(cluster.ranked_citation_score, 3)}})</h1>
        <h2 class=\"subtitle\">${{cluster.name || ""}}</h2>
        <p class=\"desc\">${{cluster.description || ""}}</p>
        <p class=\"meta\">Global ID: ${{cluster.global_id}}</p>

        <div class=\"section\">
          <h3>Top 10 Cited Articles</h3>
          <ol class=\"paper-list\">${{paperRows}}</ol>
        </div>

        <div class=\"section\">
          <h3>Countries</h3>
          ${{tableHtmlRows(["Country", "Count", "Avg Publication Year", "Avg Citation"], countryRows)}}
        </div>

        <div class=\"section\">
          <h3>Institutions</h3>
          ${{tableHtmlRows(["Institution", "Count", "Avg Publication Year", "Avg Citation"], instRows)}}
        </div>
      `;
    }}

    function buildSummaryTable() {{
      const columns = [
        { key: "display_id", label: "Display ID", type: "number" },
        { key: "global_id", label: "Global ID", type: "number" },
        { key: "short_name", label: "Short Name", type: "string" },
        { key: "name", label: "Name", type: "string" },
        ...(REPORT.use_macro_cluster ? [{ key: "macro_cluster_label", label: "Macro Cluster", type: "string" }] : []),
        { key: "publications", label: "Publications", type: "number" },
        { key: "avg_publication_year", label: "Avg Publication Year", type: "number" },
        { key: "avg_citation", label: "Avg Citation", type: "number" },
        { key: "ranked_citation_score", label: "Ranked Citation", type: "number" },
        { key: "recency", label: "Recency", type: "number" },
        { key: "japan", label: "Japan", type: "number" },
      ];

      const state = { key: "display_id", dir: "asc" };
      const root = document.getElementById("summary-table");
      const meta = document.getElementById("summary-meta");

      function normalize(v, type) {
        if (v === null || v === undefined) return type === "number" ? Number.NEGATIVE_INFINITY : "";
        if (type === "number") {
          const n = Number(v);
          return Number.isNaN(n) ? Number.NEGATIVE_INFINITY : n;
        }
        return String(v).toLowerCase();
      }

      function cellValue(c, col) {
        const v = c[col.key];
        if (col.key === "publications") return fmtInt(v);
        if (col.key === "avg_publication_year") return fmtNumber(v, 1);
        if (col.key === "avg_citation") return fmtNumber(v, 2);
        if (col.key === "ranked_citation_score") return fmtNumber(v, 3);
        if (col.key === "recency") return fmtNumber(v, 3);
        if (col.key === "japan") return fmtInt(v);
        if (col.key === "name") {
          const title = escapeHtml(v ?? "");
          const desc = escapeHtml(c.description ?? "");
          if (!desc) return title;
          return `<details class="name-details"><summary>${{title}}</summary><div class="name-desc">${{desc}}</div></details>`;
        }
        return escapeHtml(v ?? "");
      }

      function sortedData() {
        const dir = state.dir === "asc" ? 1 : -1;
        return [...REPORT.clusters].sort((a, b) => {
          const col = columns.find(c => c.key === state.key);
          const av = normalize(a[state.key], col.type);
          const bv = normalize(b[state.key], col.type);
          if (av < bv) return -1 * dir;
          if (av > bv) return 1 * dir;
          return a.display_id - b.display_id;
        });
      }

      function render() {
        meta.textContent = `Documents: ${{fmtInt(REPORT.total_publications)}}; Clusters: ${{fmtInt(REPORT.total_clusters)}}; Macro Clusters: ${{fmtInt(REPORT.total_macro_clusters)}}`;
        const headers = columns.map(c => {
          const arrow = state.key === c.key ? (state.dir === "asc" ? " ▲" : " ▼") : "";
          return `<button type="button" class="sort-btn" data-sort-key="${c.key}" style="all:unset;cursor:pointer;user-select:none;display:block;width:100%;font-weight:700;">${c.label}${arrow}</button>`;
        });
        const headerAttrs = columns.map(() => "");
        const rows = sortedData().map(c => columns.map(col => cellValue(c, col)));
        root.innerHTML = tableHtml(headers, rows, headerAttrs);
      }

      root.addEventListener("click", (ev) => {
        const target = ev.target.closest("[data-sort-key]");
        if (!target) return;
        const key = target.getAttribute("data-sort-key");
        if (!key) return;
        if (state.key === key) {
          state.dir = state.dir === "asc" ? "desc" : "asc";
        } else {
          state.key = key;
          state.dir = key === "display_id" ? "asc" : "desc";
        }
        render();
      });

      render();
    }}

    function buildScatter() {{
      if (typeof Plotly === "undefined") {{
        const target = document.getElementById("scatter");
        target.innerHTML = "<p style='color:#6d747d;padding:8px 4px;'>Scatter unavailable: Plotly failed to load. Check internet/CSP settings.</p>";
        return;
      }}

      const traces = [];
      const pointScale = REPORT.scatter_scale;

      Object.entries(REPORT.plot_by_macro).forEach(([macroLabel, v]) => {{
        const sizes = v.size.map(s => Math.max(6, Math.sqrt(Number(s || 0)) * pointScale));
        traces.push({
          type: "scatter",
          mode: "markers",
          name: macroLabel,
          x: v.x,
          y: v.y,
          text: v.text,
          customdata: v.custom,
          marker: {{
            color: v.color || "#7f7f7f",
            size: sizes,
            sizemode: "diameter",
            opacity: 0.8,
            line: {{width: 0.8, color: "#1f2a36"}},
          }},
          hovertemplate:
            "<b>%{customdata[0]}-%{customdata[2]}</b><br>" +
            "Global ID: %{customdata[1]}<br>" +
            "Name: %{customdata[3]}<br>" +
            "Publications: %{customdata[4]}<br>" +
            "Avg Year: %{customdata[5]:.1f}<br>" +
            "Avg Citation: %{customdata[6]:.2f}<br>" +
            "Ranked Citation: %{customdata[7]:.3f}<br>" +
            "<extra></extra>",
        });
      }});

      const layout = {{
        template: "plotly_white",
        xaxis: {{title: "Average Publication Year"}},
        yaxis: {{title: "Ranked Citation"}},
        legend: {{orientation: "h"}},
        showlegend: Boolean(REPORT.use_macro_cluster),
        margin: {{l: 50, r: 20, t: 20, b: 60}},
      }};

      Plotly.newPlot("scatter", traces, layout, {{responsive: true, displaylogo: false}});
    }}

    function bindTabs() {{
      const btn1 = document.getElementById("btn-tab1");
      const btn2 = document.getElementById("btn-tab2");
      const tab1 = document.getElementById("tab1");
      const tab2 = document.getElementById("tab2");

      btn1.addEventListener("click", () => {{
        btn1.classList.add("active");
        btn2.classList.remove("active");
        tab1.classList.add("active");
        tab2.classList.remove("active");
      }});
      btn2.addEventListener("click", () => {{
        btn2.classList.add("active");
        btn1.classList.remove("active");
        tab2.classList.add("active");
        tab1.classList.remove("active");
      }});
    }}

    bindTabs();
    document.getElementById("report-title").textContent = REPORT.title || "Subquery Report";
    document.getElementById("report-subtitle").textContent = REPORT.subtitle || "";
    buildClusterList();
    buildSummaryTable();
    if (REPORT.clusters.length) selectCluster(REPORT.clusters[0].global_id);
    buildScatter();
  </script>
</body>
</html>
"""
    rendered = template.replace("{{", "{").replace("}}", "}")
    return rendered.replace("__REPORT_JSON__", report_json)


def upload_report_folder(local_dir: Path, s3_prefix: str) -> list[str]:
    parsed = urlparse(s3_prefix)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 prefix: {s3_prefix}")
    bucket = parsed.netloc
    key_prefix = parsed.path.lstrip("/").rstrip("/") + "/"

    client = boto3.client("s3")
    uploaded: list[str] = []

    for file_path in local_dir.rglob("*"):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(local_dir).as_posix()
        key = f"{key_prefix}{rel}"
        content_type, _ = mimetypes.guess_type(str(file_path))
        extra = {"ContentType": content_type} if content_type else {}
        with file_path.open("rb") as f:
            client.put_object(Bucket=bucket, Key=key, Body=f.read(), **extra)
        uploaded.append(f"s3://{bucket}/{key}")
    return uploaded


def main() -> None:
  global DATABASE, QUERY_FOLDER, SUBQUERIES_ROOT, KEYWORDS_DIR, MACRO_NAME_PATH

  args = parse_args()
  DATABASE = resolve_database(args.database)
  QUERY_FOLDER = resolve_query_folder(args.query_folder, DEFAULT_QUERY_FOLDER_TOPIC)
  SUBQUERIES_ROOT = subqueries_root(DATABASE)
  KEYWORDS_DIR = keywords_dir(DATABASE)
  MACRO_NAME_PATH = macro_name_path(DATABASE)

  print("[config] database:", DATABASE)
  print("[config] query_folder:", QUERY_FOLDER)
  print("[config] use_macro_cluster:", USE_MACRO_CLUSTER)

  out_base = f"{SUBQUERIES_ROOT}{QUERY_FOLDER}/"

  print(f"[load] reading subquery datasets for '{QUERY_FOLDER}'")
  micro_rep = read_subset(out_base, "cluster_report_micro", required=True)
  papers = read_subset(out_base, "article_top10", required=True)
  countries = read_subset(out_base, "top_countries", required=True)
  insts = read_subset(out_base, "top_institutions", required=True)
  names = read_subset(out_base, "cluster_names", required=False)
  kw_micro = read_subset(KEYWORDS_DIR, "micro", required=False)
  macro_color_map = load_macro_colors() if USE_MACRO_CLUSTER else {}
  macro_name_map = load_macro_names() if USE_MACRO_CLUSTER else {}

  micro_id_col = pick_col(micro_rep, ["micro_cluster", "cluster"], required=True)
  pub_col = pick_col(micro_rep, ["publications"], required=True)
  avg_py_col = pick_col(micro_rep, ["ave_py", "avg_publication_year", "average_publication_year"], required=False)
  avg_cit_col = pick_col(micro_rep, ["ave_citations", "avg_citations", "average_citations"], required=False)
  rank_col = pick_col(
    micro_rep,
    ["yearly_rank_citations", "ranked_citation", "ranked_citation_score"],
    required=False,
  )
  recency_col = pick_col(micro_rep, ["recency_py", "recency"], required=False)
  japan_col = pick_col(micro_rep, ["japan_count", "japan"], required=False)
  macro_col = pick_col(micro_rep, ["macro_cluster"], required=False)
  if avg_py_col:
    print("[config] avg publication year source column:", avg_py_col)
  else:
    print("[warn] no avg publication year column found in cluster_report_micro")

  micro_rep = micro_rep.copy()
  micro_rep[micro_id_col] = micro_rep[micro_id_col].astype("int64")
  micro_rep[pub_col] = pd.to_numeric(micro_rep[pub_col], errors="coerce")
  if avg_py_col:
    micro_rep[avg_py_col] = pd.to_numeric(micro_rep[avg_py_col], errors="coerce")
  if avg_cit_col:
    micro_rep[avg_cit_col] = pd.to_numeric(micro_rep[avg_cit_col], errors="coerce")
  if rank_col:
    micro_rep[rank_col] = pd.to_numeric(micro_rep[rank_col], errors="coerce")
  if recency_col:
    micro_rep[recency_col] = pd.to_numeric(micro_rep[recency_col], errors="coerce")
  if japan_col:
    micro_rep[japan_col] = pd.to_numeric(micro_rep[japan_col], errors="coerce")

  papers = papers.copy()
  papers["micro_cluster"] = papers["micro_cluster"].astype("int64")
  papers["publication_year"] = pd.to_numeric(papers["publication_year"], errors="coerce")
  papers["citations"] = pd.to_numeric(papers["citations"], errors="coerce")

  countries = countries.copy()
  countries["micro_cluster"] = countries["micro_cluster"].astype("int64")
  countries["freq"] = pd.to_numeric(countries["freq"], errors="coerce")
  if "avg_publication_year" in countries.columns:
    countries["avg_publication_year"] = pd.to_numeric(countries["avg_publication_year"], errors="coerce")
  if "avg_citation" in countries.columns:
    countries["avg_citation"] = pd.to_numeric(countries["avg_citation"], errors="coerce")
  countries["country"] = countries["country"].map(iso2_to_country_name)

  insts = insts.copy()
  insts["micro_cluster"] = insts["micro_cluster"].astype("int64")
  insts["freq"] = pd.to_numeric(insts["freq"], errors="coerce")
  if "avg_publication_year" in insts.columns:
    insts["avg_publication_year"] = pd.to_numeric(insts["avg_publication_year"], errors="coerce")
  if "avg_citation" in insts.columns:
    insts["avg_citation"] = pd.to_numeric(insts["avg_citation"], errors="coerce")

  names_map: dict[int, dict[str, str]] = {}
  if not names.empty and "micro_cluster" in names.columns:
    n = names.copy()
    n["micro_cluster"] = n["micro_cluster"].astype("int64")
    for row in n.itertuples(index=False):
      names_map[int(row.micro_cluster)] = {
        "short_name": sanitize_text(getattr(row, "short_name", "")),
        "name": sanitize_text(getattr(row, "name", "")),
        "description": sanitize_text(getattr(row, "description", "")),
      }

  kw_map: dict[int, str] = {}
  if not kw_micro.empty and {"cluster", "keywords"}.issubset(kw_micro.columns):
    k = kw_micro.copy()
    k["cluster"] = k["cluster"].astype("int64")
    kw_map = dict(zip(k["cluster"], k["keywords"].fillna("")))

  merged = micro_rep.copy()
  if micro_id_col != "micro_cluster":
    merged = merged.rename(columns={micro_id_col: "micro_cluster"})

  sort_rank_col = rank_col if rank_col else pub_col
  merged = merged.sort_values([pub_col, sort_rank_col, "micro_cluster"], ascending=[False, False, True]).reset_index(drop=True)
  merged["display_id"] = range(1, len(merged) + 1)

  papers_by_micro: dict[int, list[dict[str, Any]]] = {}
  p_sorted = papers.sort_values(["micro_cluster", "citations"], ascending=[True, False])
  for mid, grp in p_sorted.groupby("micro_cluster"):
    top = grp.head(TOP_TITLES)
    papers_by_micro[int(mid)] = [
      {
        "title": sanitize_text(r.title),
        "citations": int(r.citations) if pd.notna(r.citations) else 0,
        "publication_year": int(r.publication_year) if pd.notna(r.publication_year) else None,
      }
      for r in top.itertuples(index=False)
    ]

  countries_by_micro: dict[int, list[dict[str, Any]]] = {}
  c_sorted = countries.sort_values(["micro_cluster", "freq", "country"], ascending=[True, False, True])
  for mid, grp in c_sorted.groupby("micro_cluster"):
    countries_by_micro[int(mid)] = [
      {
        "country": sanitize_text(r.country),
        "freq": int(r.freq) if pd.notna(r.freq) else 0,
        "avg_publication_year": float(getattr(r, "avg_publication_year", float("nan")))
        if pd.notna(getattr(r, "avg_publication_year", float("nan")))
        else None,
        "avg_citation": float(getattr(r, "avg_citation", float("nan")))
        if pd.notna(getattr(r, "avg_citation", float("nan")))
        else None,
      }
      for r in grp.itertuples(index=False)
      if not is_blank_like(getattr(r, "country", ""))
    ]

  insts_by_micro: dict[int, list[dict[str, Any]]] = {}
  i_sorted = insts.sort_values(["micro_cluster", "freq", "institution"], ascending=[True, False, True])
  for mid, grp in i_sorted.groupby("micro_cluster"):
    insts_by_micro[int(mid)] = [
      {
        "institution": sanitize_text(r.institution),
        "freq": int(r.freq) if pd.notna(r.freq) else 0,
        "avg_publication_year": float(getattr(r, "avg_publication_year", float("nan")))
        if pd.notna(getattr(r, "avg_publication_year", float("nan")))
        else None,
        "avg_citation": float(getattr(r, "avg_citation", float("nan")))
        if pd.notna(getattr(r, "avg_citation", float("nan")))
        else None,
      }
      for r in grp.itertuples(index=False)
    ]

  clusters: list[dict[str, Any]] = []
  for r in merged.itertuples(index=False):
    mid = int(r.micro_cluster)
    kwargs = names_map.get(mid, {})
    short_name = kwargs.get("short_name") or keyword_fallback(kw_map.get(mid, ""))
    long_name = kwargs.get("name") or short_name
    desc = kwargs.get("description") or ""

    if short_name == "Unnamed Cluster":
      short_name = f"Cluster {mid}"
      long_name = f"Cluster {mid}"

    avg_cit = float(getattr(r, avg_cit_col)) if avg_cit_col and pd.notna(getattr(r, avg_cit_col)) else float("nan")
    rank_score = float(getattr(r, rank_col)) if rank_col and pd.notna(getattr(r, rank_col)) else float("nan")
    avg_year = float(getattr(r, avg_py_col)) if avg_py_col and pd.notna(getattr(r, avg_py_col)) else float("nan")
    recency = float(getattr(r, recency_col)) if recency_col and pd.notna(getattr(r, recency_col)) else float("nan")
    japan = float(getattr(r, japan_col)) if japan_col and pd.notna(getattr(r, japan_col)) else float("nan")

    macro_id = int(getattr(r, macro_col)) if macro_col and pd.notna(getattr(r, macro_col)) else None
    macro_label = macro_name_map.get(int(macro_id), "Unknown") if macro_id is not None else "Unknown"
    macro_color = color_for_macro(int(macro_id), macro_color_map) if macro_id is not None else "#7f7f7f"

    clusters.append(
      {
        "display_id": int(r.display_id),
        "global_id": mid,
        "short_name": short_name,
        "name": long_name,
        "description": desc,
        "publications": int(getattr(r, pub_col)) if pd.notna(getattr(r, pub_col)) else 0,
        "avg_publication_year": None if math.isnan(avg_year) else avg_year,
        "avg_citation": None if math.isnan(avg_cit) else avg_cit,
        "ranked_citation_score": None if math.isnan(rank_score) else rank_score,
        "recency": None if math.isnan(recency) else recency,
        "japan": 0 if math.isnan(japan) else int(japan),
        "macro_cluster": macro_id if macro_id is not None else "Unknown",
        "macro_cluster_label": macro_label,
        "macro_color": macro_color,
        "top_titles": papers_by_micro.get(mid, []),
        "countries": [x for x in countries_by_micro.get(mid, []) if not is_blank_like(x.get("country", ""))],
        "institutions": [x for x in insts_by_micro.get(mid, []) if not is_blank_like(x.get("institution", ""))],
      }
    )

  plot_by_macro = build_plot_data(clusters, use_macro_cluster=USE_MACRO_CLUSTER)
  total_publications = int(sum(int(c.get("publications", 0) or 0) for c in clusters))
  total_macro_clusters = len({c.get("macro_cluster") for c in clusters})

  report_payload = {
    "title": TITLE,
    "subtitle": SUBTITLE,
    "query_folder": QUERY_FOLDER,
    "clusters": clusters,
    "plot_by_macro": plot_by_macro,
    "total_publications": total_publications,
    "total_clusters": len(clusters),
    "total_macro_clusters": total_macro_clusters,
    "use_macro_cluster": USE_MACRO_CLUSTER,
    "scatter_scale": 1.1,
  }

  out_dir = LOCAL_DOCS_ROOT / DATABASE / QUERY_FOLDER / "report"
  out_dir.mkdir(parents=True, exist_ok=True)
  html_path = out_dir / "index.html"
  html_path.write_text(build_html(json.dumps(report_payload, ensure_ascii=True)), encoding="utf-8")

  s3_report_prefix = f"{SUBQUERIES_ROOT}{QUERY_FOLDER}/report/"
  uploaded = upload_report_folder(out_dir, s3_report_prefix)

  print(f"[done] clusters: {len(clusters):,}")
  print(f"[done] local report: {html_path}")
  print(f"[done] uploaded: {len(uploaded)} files to {s3_report_prefix}")
  for p in uploaded:
    print(f"  - {p}")


if __name__ == "__main__":
    main()
