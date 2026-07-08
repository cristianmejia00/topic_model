"""
explore_subquery.py
===================
Quickly browse the S3 outputs of a subquery search run for one SUBQUERY.

Two ways to use it:

  Terminal (interactive):
      python 06-subquery_reports/explore_subquery.py --snapshot 2026-06-26 --query q20260629 --subquery quantum_computing
      # prints a summary, then prompts for a micro id (or 'meso N' / 'macro N')

  Notebook / import:
      from subqueries.explore_subquery import Explorer
      e = Explorer(subquery="quantum_computing", snapshot="2026-06-26", query="q20260629")
      e.summary()            # ranked table of matched micro clusters
      e.micro(12345)         # full profile: stats, keywords, top titles, countries, insts
      e.meso(678)            # meso context + its matched micros
      e.titles(12345)        # just the stored titles

Loads every subset once and serves lookups from memory.

Requires: numpy, pandas, awswrangler, pyarrow
"""

from __future__ import annotations
import argparse
import sys
import pandas as pd

from common_config import (
    resolve_paths,
)

pd.set_option("display.max_colwidth", 80)
pd.set_option("display.width", 160)

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
KW_TERMS = 6        # keyword terms shown in the summary table


def _short(kw, n=KW_TERMS):
    return ", ".join(kw.split(", ")[:n]) if isinstance(kw, str) else ""


class Explorer:
    def __init__(self, *, subquery: str, snapshot: str, query: str, autoload=True):
        paths = resolve_paths(snapshot=snapshot, query=query, subquery=subquery)
        self.database = paths.database
        self.snapshot = paths.snapshot
        self.query = paths.query
        self.folder = paths.subquery
        self.base = paths.subquery_base
        self.keywords_dir = paths.bertopic_root
        if autoload:
            self._load()

    # ------------------------------------------------------------------ load
    def _read(self, name):
        import awswrangler as wr
        try:
            return wr.s3.read_parquet(f"{self.base}{name}/")
        except Exception as e:               # missing/empty subset -> None
            print(f"[warn] could not read {name}: {e}")
            return None

    def _load(self):
        print(f"[load] {self.base}")
        self.matches = self._read("matches")
        self.micro_rep = self._read("cluster_report_micro")
        self.meso_rep  = self._read("cluster_report_meso")
        self.macro_rep = self._read("cluster_report_macro")
        self.papers  = self._read("article_top10")
        self.countries = self._read("top_countries")
        self.insts     = self._read("top_institutions")

        # keyword lookups from the bertopic outputs (not stored in the subquery)
        self.kw = {}
        for lvl in ("micro", "meso", "macro"):
            try:
                import awswrangler as wr
                k = wr.s3.read_parquet(f"{self.keywords_dir}{lvl}/")[["cluster", "keywords"]]
                self.kw[lvl] = dict(zip(k["cluster"].astype("int64"),
                                        k["keywords"].fillna("")))
            except Exception:
                self.kw[lvl] = {}
        print("[load] done")

    # --------------------------------------------------------------- summary
    def summary(self, top=None):
        n_micro = 0 if self.micro_rep is None else len(self.micro_rep)
        n_meso  = 0 if self.meso_rep  is None else len(self.meso_rep)
        n_macro = 0 if self.macro_rep is None else len(self.macro_rep)
        papers  = 0 if self.micro_rep is None else int(self.micro_rep["publications"].sum())
        print(f"\n=== query '{self.folder}' ===")
        print(f"{n_micro} micro · {n_meso} meso · {n_macro} macro clusters "
              f"· {papers:,} papers total\n")

        if self.micro_rep is None or self.micro_rep.empty:
            return
        t = self.micro_rep[["micro_cluster", "meso_cluster", "macro_cluster",
                        "publications", "ave_citations", "yearly_rank_citations",
                        "recency_py"]].copy()
        if self.matches is not None:
            t = t.merge(self.matches[["micro_cluster", "similarity"]],
                        on="micro_cluster", how="left")
        t["keywords"] = t["micro_cluster"].map(lambda m: _short(self.kw["micro"].get(int(m), "")))
        sort_col = "similarity" if "similarity" in t.columns else "publications"
        t = t.sort_values(sort_col, ascending=False)
        cols = (["micro_cluster", "similarity", "publications", "ave_citations",
                 "yearly_rank_citations", "recency_py", "keywords"]
                if "similarity" in t.columns else
                ["micro_cluster", "publications", "ave_citations",
                 "yearly_rank_citations", "recency_py", "keywords"])
        shown = t if top is None else t.head(top)
        with pd.option_context("display.max_rows", None):
            print(shown[cols].to_string(index=False,
                  formatters={"similarity": lambda x: f"{x:.3f}" if pd.notna(x) else "",
                              "ave_citations": lambda x: f"{x:.1f}",
                              "yearly_rank_citations": lambda x: f"{x:.3f}",
                              "recency_py": lambda x: f"{x:.2f}"}))
        if top is not None and len(t) > top:
            print(f"... {len(t) - top} more (call .summary(top=None) for all)")

    # ----------------------------------------------------------------- micro
    def micro(self, mid):
        mid = int(mid)
        if self.micro_rep is None or mid not in set(self.micro_rep["micro_cluster"]):
            print(f"[!] micro {mid} not in this query's results")
            return
        r = self.micro_rep[self.micro_rep["micro_cluster"] == mid].iloc[0]
        sim = ""
        if self.matches is not None:
            m = self.matches[self.matches["micro_cluster"] == mid]
            if len(m):
                sim = f"  ·  similarity {m.iloc[0]['similarity']:.3f}"
        print(f"\n=== micro {mid}{sim} ===")
        print(f"keywords : {self.kw['micro'].get(mid, '(none)')}")
        print(f"parents  : meso {int(r['meso_cluster'])}  ·  macro {int(r['macro_cluster'])}")
        print(f"size     : {int(r['publications'])} papers")
        print(f"citations: avg {r['ave_citations']:.1f} · median {r['median_citations']:.0f} "
              f"· max {int(r['max_citations'])}")
        print(f"rank/yr  : {r['yearly_rank_citations']:.3f}   recency(>=2024): {r['recency_py']:.2f}"
              f"   JP papers: {int(r['japan_count'])}")

        self.titles(mid)
        self._entities(mid)

    def titles(self, mid):
        mid = int(mid)
        if self.papers is None:
            return
        p = (self.papers[self.papers["micro_cluster"] == mid]
             .sort_values("citations", ascending=False))
        print(f"\n  top {len(p)} cited titles:")
        for _, row in p.iterrows():
            print(f"    [{int(row['citations']):>6} cites · {row['publication_year']}] {row['title']}")

    def _entities(self, mid):
        if self.countries is not None:
            c = self.countries[self.countries["micro_cluster"] == mid].head(10)
            if len(c):
                print("\n  top countries : " +
                      ", ".join(f"{r['country']}({int(r['freq'])})" for _, r in c.iterrows()))
        if self.insts is not None:
            i = self.insts[self.insts["micro_cluster"] == mid].head(10)
            if len(i):
                print("  top institutions: " +
                      ", ".join(f"{r['institution']}({int(r['freq'])})" for _, r in i.iterrows()))

    # ---------------------------------------------------------- meso / macro
    def meso(self, sid):
        self._context("meso", int(sid))

    def macro(self, sid):
        self._context("macro", int(sid))

    def _context(self, level, sid):
        rep = getattr(self, f"{level}_rep")
        col = f"{level}_cluster"
        if rep is None or sid not in set(rep[col]):
            print(f"[!] {level} {sid} not in this query's results")
            return
        r = rep[rep[col] == sid].iloc[0]
        print(f"\n=== {level} {sid} ===")
        print(f"keywords: {self.kw[level].get(sid, '(none)')}")
        print(f"size    : {int(r['publications'])} papers")
        if self.micro_rep is not None:
            kids = self.micro_rep[self.micro_rep[col] == sid]["micro_cluster"].tolist()
            print(f"matched micro clusters ({len(kids)}): "
                  + ", ".join(str(int(k)) for k in kids))


def main():
    parser = argparse.ArgumentParser(description="Explore a generated subquery output in terminal mode.")
    parser.add_argument("--snapshot", default=None, help="Snapshot token, e.g. 2026-06-26.")
    parser.add_argument("--query", default=None, help="Query token, e.g. q20260629.")
    parser.add_argument("--subquery", default=None, help="Subquery folder name under clustering/subqueries/.")
    parser.add_argument("--query-folder", default=None, help="Deprecated alias for --subquery.")
    args = parser.parse_args()

    paths = resolve_paths(
        snapshot=args.snapshot,
        query=args.query,
        subquery=args.subquery,
        query_folder=args.query_folder,
    )
    print("[config] snapshot:", paths.snapshot)
    print("[config] query:", paths.query)
    print("[config] database:", paths.database)
    print("[config] query_folder:", paths.subquery)

    e = Explorer(subquery=paths.subquery, snapshot=paths.snapshot, query=paths.query)
    e.summary()
    print("\nEnter a micro id to drill in — or 'meso N' / 'macro N' / 'q' to quit.")
    while True:
        try:
            cmd = input("> ").strip()
        except EOFError:
            break
        if cmd in ("q", "quit", "exit", ""):
            break
        try:
            if cmd.startswith("meso"):
                e.meso(cmd.split()[1])
            elif cmd.startswith("macro"):
                e.macro(cmd.split()[1])
            else:
                e.micro(cmd)
        except (ValueError, IndexError):
            print("  (type a micro id, 'meso N', 'macro N', or 'q')")


if __name__ == "__main__":
    main()