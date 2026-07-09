# chart_utils.R — Shared utilities for chart scripts
# Loaded by: dataset_bars.R, dataset_trends.R, cluster_stats.R, cluster_scatterplots.R, overlays.R

library(ggplot2)
library(glue)
library(jsonlite)

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------

#' Load the canonical color palette from assets/fukan_colors.json
#' @return A list with elements: base (10), extended (8), colorblind (8), full (18)
load_chart_palette <- function() {
  palette_path <- file.path(getwd(), "assets", "fukan_colors.json")
  pal <- fromJSON(palette_path)
  pal$full <- c(pal$base, pal$extended)
  pal
}

#' Recycle a color palette to the requested size
#' @param palette Character vector of hex colors
#' @param n Integer target size
#' @param set_last_grey Logical; force last color to light grey
#' @return Character vector of length n
recycle_palette <- function(palette, n, set_last_grey = FALSE) {
  if (is.null(palette) || length(palette) == 0 || n <= 0) return(character(0))
  pal <- rep_len(palette, n)
  if (set_last_grey && n > 0) pal[n] <- "#d3d3d3"
  pal
}

# ---------------------------------------------------------------------------
# Cluster code helpers
# ---------------------------------------------------------------------------

#' Remove formatting artifacts from cluster codes ("---" prefix and "-0" suffix)
#' @param x Character vector of cluster codes
#' @return Cleaned character vector
clean_cluster_code <- function(x) {
  gsub("---|-0", "", x)
}

#' Extract the main (top-level) cluster number from hierarchical codes like "1-2-3"
#' @param cluster_codes Character vector of cluster codes (already cleaned or raw)
#' @return Factor of main cluster numbers, ordered numerically
extract_main_cluster <- function(cluster_codes) {
  cleaned <- clean_cluster_code(cluster_codes)
  main <- sub("-.*", "", cleaned)
  lvls <- sort(unique(as.numeric(main)))
  factor(main, levels = as.character(lvls))
}

#' Assign hex colors to main cluster levels from a palette vector
#' @param main_clusters Factor from extract_main_cluster()
#' @param palette Character vector of hex colors (length >= number of levels)
#' @return Character vector of hex colors, with NA → grey
assign_cluster_colors <- function(main_clusters, palette) {
  n_levels <- nlevels(main_clusters)
  palette_use <- recycle_palette(palette, n_levels, set_last_grey = TRUE)
  colors <- palette_use[as.integer(main_clusters)]
  colors[is.na(colors)] <- "#d3d3d3"
  colors
}

# ---------------------------------------------------------------------------
# Cluster label resolution
# ---------------------------------------------------------------------------

#' Resolve display labels for clusters/topics/facets.
#' Returns a named list with two elements:
#'   - dataset: the input dataset with X_C_name column added/updated
#'   - rcs_merged: the input rcs_merged with X_C_name column added/updated
#'
#' @param rcs_merged Data frame (rcs_merged)
#' @param dataset Data frame (the document dataset)
#' @param unit_of_analysis Character ("cluster", "topic", or facet name)
#' @return Named list with $dataset and $rcs_merged, both with X_C_name set
resolve_cluster_labels <- function(rcs_merged, dataset, unit_of_analysis) {
  is_cluster_or_topic <- tolower(unit_of_analysis) %in% c("topic", "topics", "cluster", "clusters")
  has_global <- is_cluster_or_topic &&
    "global_name" %in% colnames(rcs_merged) &&
    !all(is.na(rcs_merged$global_name) | trimws(rcs_merged$global_name) == "" | tolower(trimws(rcs_merged$global_name)) == "nan")
  has_names  <- is_cluster_or_topic && !all(rcs_merged$cluster_name == "")

  if (is_cluster_or_topic && !has_names) {
    # Option 1: unnamed clusters — use cluster code as label
    dataset$X_C_name <- as.character(dataset$X_C)
    rcs_merged$X_C_name <- clean_cluster_code(as.character(rcs_merged$cluster_code))
  } else if (is_cluster_or_topic && has_names) {
    # Option 2: named clusters — prefer global_name over cluster_name
    # Per-row: use global_name if non-empty, otherwise cluster_name
    code_clean <- clean_cluster_code(rcs_merged$cluster_code)
    pick_name <- function(gn, cn) {
      gn <- trimws(as.character(gn))
      cn <- trimws(as.character(cn))
      if (!is.na(gn) && nzchar(gn) && tolower(gn) != "nan") gn else cn
    }
    gn_col <- if (has_global) rcs_merged$global_name else rep("", nrow(rcs_merged))
    chosen  <- mapply(pick_name, gn_col, rcs_merged$cluster_name)
    rcs_merged$X_C_name <- substr(paste0(code_clean, ". ", chosen), 1, 35)
    dataset$X_C_name <- chosen[match(dataset$X_C, rcs_merged$X_C)]
  } else {
    # Option 3: facet analysis — names come from dataset
    topic_names <- dataset[!duplicated(dataset$X_C), c("X_C", "X_C_name")]
    rcs_merged$X_C_name <- topic_names$X_C_name[match(rcs_merged$cluster, topic_names$X_C)]
  }

  list(dataset = dataset, rcs_merged = rcs_merged)
}

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

#' Standard chart theme
theme_chart <- function() {
  theme_bw()
}
