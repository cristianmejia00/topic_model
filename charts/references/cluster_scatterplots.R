print("###################### cluster_scatterplots.R")

# RE-RUNNABLE: Re-execute this script after cluster naming to get labeled charts.
#
# Charts that use cluster names as labels:
#   - Scatter plots (PY×Z9, PY×size, size×Z9, and sentiment variants)
#   - LDA-style bubble charts (if lda.json exists)
#
# When cluster names are empty, labels fall back to cleaned cluster_code.
# When cluster names are set, labels show "code. name" (truncated to 27 chars).
# The label column is resolved ONCE via resolve_cluster_labels() from chart_utils.R.

source(file.path(getwd(), "pipelines", "charts", "chart_utils.R"))
library(ggrepel)
library(dplyr)

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
unit_of_analysis <- settings$params$unit_of_analysis
column_labels    <- settings$rp$column_labels

chart_palette <- load_chart_palette()
default_pal   <- chart_palette$full

dir.create(file.path(output_folder_level, subfolder_clusters), recursive = TRUE, showWarnings = FALSE)

# ---------------------------------------------------------------------------
# Resolve cluster labels (one place, one time)
# ---------------------------------------------------------------------------
resolved <- resolve_cluster_labels(rcs_merged, dataset, unit_of_analysis)
dataset     <- resolved$dataset
rcs_merged  <- resolved$rcs_merged

use_cluster_names <- any(rcs_merged$cluster_name != "")
print(paste("Cluster names available:", use_cluster_names))

# ---------------------------------------------------------------------------
# Derive main_cluster + colors
# ---------------------------------------------------------------------------
rcs_merged$main_cluster <- extract_main_cluster(rcs_merged$cluster_code)
rcs_merged$color_hex    <- assign_cluster_colors(rcs_merged$main_cluster, default_pal)

# ---------------------------------------------------------------------------
# Prepare working RCS: filter out -99 and 99 subclusters
# ---------------------------------------------------------------------------
rcs_plot <- rcs_merged %>%
  filter(!grepl("-99", cluster_code), !grepl("^99$", as.character(cluster_code)))

# For facet analysis: keep only facets with >= 10 documents
if (!(tolower(unit_of_analysis) %in% c("topic", "topics", "cluster", "clusters"))) {
  rcs_plot <- rcs_plot %>% filter(documents >= 10)
}


# ===========================================================================
# PART 1: Scatter plots
# ===========================================================================

#' Scatter plot with text labels, sized by a third variable
plot_scatter <- function(rcs_data,
                         x_col, y_col, size_col,
                         x_label = x_col, y_label = y_col,
                         title = "") {
  # Build named color palette from data (main_cluster → color_hex)
  pal_df <- rcs_data %>%
    dplyr::distinct(main_cluster, color_hex) %>%
    dplyr::arrange(main_cluster)
  named_pal <- setNames(pal_df$color_hex, as.character(pal_df$main_cluster))

  p <- ggplot(rcs_data, aes(x = .data[[x_col]], y = .data[[y_col]])) +
    geom_point(aes(color = main_cluster, size = .data[[size_col]]), alpha = 0.75) +
    scale_color_manual(values = named_pal) +
    scale_size_continuous(range = c(2, 12)) +
    geom_text_repel(
      aes(label = clean_cluster_code(X_C_name)),
      size          = 3,
      max.overlaps  = 20,
      segment.color = "grey50",
      segment.size  = 0.3,
      box.padding   = 0.4,
      show.legend   = FALSE
    ) +
    labs(
      x     = x_label,
      y     = y_label,
      size  = "Documents",
      color = "Main Cluster",
      title = title
    ) +
    theme_minimal(base_size = 13) +
    theme(
      legend.position  = "right",
      panel.grid.minor = element_blank()
    )

  # Add year-step breaks when x axis is a publication year
  if (grepl("PY", x_col, ignore.case = TRUE)) {
    x_vals <- rcs_data[[x_col]]
    p <- p + scale_x_continuous(
      breaks = seq(
        floor(min(x_vals, na.rm = TRUE)),
        ceiling(max(x_vals, na.rm = TRUE)),
        by = 2
      )
    )
  }

  p
}

# Define scatter plot specifications: x, y, size, x_label, y_label, filename, title
scatter_specs <- list(
  list("PY_Mean",   "Z9_Mean",   "documents", "Average Publication Year", "Ave. Citations",
       "fig_scatter_clusters_PY_x_Z9",
       "Cluster Landscape: Publication Year vs. Citations"),
  list("PY_Mean",   "documents", "Z9_Mean",   "Average Publication Year", "Documents",
       "fig_scatter_clusters_PY_x_size",
       "Cluster Landscape: Publication Year vs. Documents"),
  list("documents", "Z9_Mean",   "PY_Mean",   "Documents",               "Ave. Citations",
       "fig_scatter_clusters_size_x_Z9",
       "Cluster Landscape: Documents vs. Citations")
)

# 4th scatter: normalized citations (conditional on column presence)
if ("Z9_ave_rank" %in% colnames(rcs_plot)) {
  scatter_specs <- c(scatter_specs, list(
    list("PY_Mean", "Z9_ave_rank", "documents",
         "Average Publication Year", "Mean Yearly-Normalized Citations (Z9)",
         "fig_scatter_clusters_PY_x_Z9_rank",
         "Cluster Landscape: Impact vs. Recency")
  ))
}

# Sentiment variants (conditional)
if ("sentiment_Mean" %in% colnames(rcs_merged)) {
  scatter_specs <- c(scatter_specs, list(
    list("PY_Mean",   "sentiment_Mean", "documents", column_labels["PY"],        column_labels["sentiment"],
         "fig_scatter_clusters_year_x_sentiment",
         "Cluster Landscape: Publication Year vs. Sentiment"),
    list("Z9_Mean",   "sentiment_Mean", "documents", column_labels["Z9"],        column_labels["sentiment"],
         "fig_scatter_clusters_Z9_x_sentiment",
         "Cluster Landscape: Citations vs. Sentiment"),
    list("documents", "sentiment_Mean", "documents", "Documents",                column_labels["sentiment"],
         "fig_scatter_clusters_size_x_sentiment",
         "Cluster Landscape: Documents vs. Sentiment")
  ))
}

for (spec in scatter_specs) {
  plot_scatter(rcs_plot, spec[[1]], spec[[2]], spec[[3]], spec[[4]], spec[[5]], spec[[7]])
  ggsave(file.path(output_folder_level, subfolder_clusters,
                   glue("{spec[[6]]}.{extension}")),
         width = 12, height = 6, units = "in")
}

