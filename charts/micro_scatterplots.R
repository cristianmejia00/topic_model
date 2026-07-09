#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(aws.s3)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(ggrepel)
})

`%||%` <- function(x, y) {
  if (!is.null(x) && nzchar(trimws(as.character(x)))) x else y
}

parse_cli_args <- function(args) {
  out <- list()
  i <- 1
  while (i <= length(args)) {
    token <- args[[i]]
    if (!startsWith(token, "--")) {
      stop(sprintf("Unexpected positional argument: %s", token), call. = FALSE)
    }

    if (grepl("^--[^=]+=", token)) {
      key <- sub("^--([^=]+)=.*$", "\\1", token)
      val <- sub("^--[^=]+=", "", token)
      out[[key]] <- val
      i <- i + 1
      next
    }

    key <- sub("^--", "", token)
    if (i == length(args) || startsWith(args[[i + 1]], "--")) {
      stop(sprintf("Missing value for argument --%s", key), call. = FALSE)
    }
    out[[key]] <- args[[i + 1]]
    i <- i + 2
  }
  out
}

parse_s3_uri <- function(uri) {
  if (!startsWith(uri, "s3://")) {
    stop(sprintf("Expected s3:// URI, got: %s", uri), call. = FALSE)
  }

  rest <- sub("^s3://", "", uri)
  slash <- regexpr("/", rest, fixed = TRUE)
  if (slash < 0) {
    bucket <- rest
    key <- ""
  } else {
    bucket <- substr(rest, 1, slash - 1)
    key <- substr(rest, slash + 1, nchar(rest))
  }

  list(bucket = bucket, key = key)
}

join_s3 <- function(base_dir, file_name) {
  base <- sub("/*$", "", base_dir)
  sprintf("%s/%s", base, file_name)
}

read_s3_parquet_dataset <- function(dir_uri, region = NULL) {
  parsed <- parse_s3_uri(dir_uri)
  prefix <- sub("/*$", "", parsed$key)
  if (nzchar(prefix)) {
    prefix <- paste0(prefix, "/")
  }

  list_args <- list(bucket = parsed$bucket, prefix = prefix, max = 10000)
  if (!is.null(region) && nzchar(trimws(region))) {
    list_args$region <- region
  }
  objs <- do.call(get_bucket, list_args)
  if (length(objs) == 0) {
    stop(sprintf("No objects found in %s", dir_uri), call. = FALSE)
  }

  keys <- vapply(objs, function(x) x[["Key"]], character(1), USE.NAMES = FALSE)
  parquet_keys <- keys[grepl("\\.parquet$", keys)]
  if (length(parquet_keys) == 0) {
    stop(sprintf("No parquet files found in %s", dir_uri), call. = FALSE)
  }

  parts <- lapply(parquet_keys, function(obj_key) {
    tmp_parquet <- tempfile(pattern = "micro_scatter_part_", fileext = ".parquet")
    on.exit(unlink(tmp_parquet), add = TRUE)

    save_args <- list(object = obj_key, bucket = parsed$bucket, file = tmp_parquet)
    if (!is.null(region) && nzchar(trimws(region))) {
      save_args$region <- region
    }
    do.call(save_object, save_args)

    as.data.frame(arrow::read_parquet(tmp_parquet))
  })

  bind_rows(parts)
}

pick_col <- function(df, candidates, required = FALSE) {
  found <- candidates[candidates %in% names(df)]
  if (length(found) > 0) {
    return(found[[1]])
  }
  if (required) {
    stop(
      sprintf("Missing required columns. Expected one of: %s", paste(candidates, collapse = ", ")),
      call. = FALSE
    )
  }
  NULL
}

parse_optional_numeric_arg <- function(raw_value, arg_name) {
  if (is.null(raw_value)) {
    return(NA_real_)
  }

  value <- suppressWarnings(as.numeric(raw_value))
  if (length(value) == 0 || is.na(value) || !is.finite(value)) {
    stop(sprintf("Invalid numeric value for --%s: %s", arg_name, as.character(raw_value)), call. = FALSE)
  }
  value
}

build_cluster_code_fallback <- function(df) {
  required <- c("micro_cluster", "macro_cluster", "publications")
  missing <- required[!(required %in% names(df))]
  if (length(missing) > 0) {
    stop(
      sprintf("cluster_report_micro is missing required columns for fallback cluster_code: %s", paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }

  rank_base <- df %>%
    transmute(
      micro_cluster = suppressWarnings(as.integer(micro_cluster)),
      macro_cluster = suppressWarnings(as.integer(macro_cluster)),
      publications = suppressWarnings(as.numeric(publications))
    ) %>%
    filter(!is.na(micro_cluster), !is.na(macro_cluster)) %>%
    group_by(micro_cluster, macro_cluster) %>%
    summarise(
      publications = if (all(is.na(publications))) 0 else max(publications, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(publications = ifelse(is.na(publications), 0, publications))

  if (nrow(rank_base) == 0) {
    return(rep("", nrow(df)))
  }

  macro_rank <- rank_base %>%
    group_by(macro_cluster) %>%
    summarise(publications = sum(publications, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(publications), macro_cluster) %>%
    mutate(macro_display_id = row_number())

  micro_rank <- rank_base %>%
    arrange(macro_cluster, desc(publications), micro_cluster) %>%
    group_by(macro_cluster) %>%
    mutate(micro_rank = row_number()) %>%
    ungroup() %>%
    left_join(macro_rank %>% select(macro_cluster, macro_display_id), by = "macro_cluster") %>%
    mutate(cluster_code = paste0(macro_display_id, "-", micro_rank)) %>%
    select(micro_cluster, macro_cluster, cluster_code)

  out <- df %>%
    mutate(
      micro_cluster_num = suppressWarnings(as.integer(micro_cluster)),
      macro_cluster_num = suppressWarnings(as.integer(macro_cluster))
    ) %>%
    left_join(
      micro_rank,
      by = c("micro_cluster_num" = "micro_cluster", "macro_cluster_num" = "macro_cluster")
    )

  out$cluster_code <- ifelse(is.na(out$cluster_code), "", as.character(out$cluster_code))
  out$cluster_code
}

ensure_cluster_code <- function(df) {
  fallback <- build_cluster_code_fallback(df)
  if (!("cluster_code" %in% names(df))) {
    df$cluster_code <- fallback
    return(df)
  }

  existing <- trimws(as.character(df$cluster_code))
  # Enforce 1-based codes only (e.g. 1-1, 2-7). Any zero/invalid segment is replaced.
  valid_nonzero <- grepl("^[1-9][0-9]*-[1-9][0-9]*$", existing)
  needs_fill <- is.na(existing) | existing == "" | !valid_nonzero
  existing[needs_fill] <- fallback[needs_fill]
  df$cluster_code <- existing
  df
}

add_macro_display_id <- function(df) {
  required <- c("macro_cluster", "publications")
  missing <- required[!(required %in% names(df))]
  if (length(missing) > 0) {
    stop(
      sprintf("Missing required columns for macro display ids: %s", paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }

  macro_map <- df %>%
    transmute(
      macro_cluster = suppressWarnings(as.integer(macro_cluster)),
      publications = suppressWarnings(as.numeric(publications))
    ) %>%
    filter(!is.na(macro_cluster), !is.na(publications)) %>%
    group_by(macro_cluster) %>%
    summarise(publications = sum(publications, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(publications), macro_cluster) %>%
    mutate(macro_display_id = row_number()) %>%
    select(macro_cluster, macro_display_id)

  df %>%
    mutate(macro_cluster = suppressWarnings(as.integer(macro_cluster))) %>%
    left_join(macro_map, by = "macro_cluster")
}

rebuild_cluster_code_for_plot <- function(df) {
  required <- c("macro_display_id", "micro_cluster", "publications")
  missing <- required[!(required %in% names(df))]
  if (length(missing) > 0) {
    stop(
      sprintf("Missing required columns to rebuild cluster_code: %s", paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }

  code_map <- df %>%
    transmute(
      macro_display_id = suppressWarnings(as.integer(macro_display_id)),
      micro_cluster = suppressWarnings(as.integer(micro_cluster)),
      publications = suppressWarnings(as.numeric(publications))
    ) %>%
    filter(!is.na(macro_display_id), !is.na(micro_cluster)) %>%
    distinct(macro_display_id, micro_cluster, .keep_all = TRUE) %>%
    arrange(macro_display_id, desc(publications), micro_cluster) %>%
    group_by(macro_display_id) %>%
    mutate(micro_rank = row_number()) %>%
    ungroup() %>%
    mutate(cluster_code = paste0(macro_display_id, "-", micro_rank)) %>%
    select(macro_display_id, micro_cluster, cluster_code)

  df %>%
    mutate(
      macro_display_id = suppressWarnings(as.integer(macro_display_id)),
      micro_cluster = suppressWarnings(as.integer(micro_cluster))
    ) %>%
    select(-any_of("cluster_code")) %>%
    left_join(code_map, by = c("macro_display_id", "micro_cluster")) %>%
    mutate(cluster_code = ifelse(is.na(cluster_code), "", cluster_code))
}

save_plot_to_s3 <- function(plot_obj, output_uri, width, height, dpi, region = NULL) {
  tmp_png <- tempfile(pattern = "micro_scatter_", fileext = ".png")
  on.exit(unlink(tmp_png), add = TRUE)

  ggsave(filename = tmp_png, plot = plot_obj, width = width, height = height, units = "in", dpi = dpi)

  parsed <- parse_s3_uri(output_uri)
  put_args <- list(file = tmp_png, object = parsed$key, bucket = parsed$bucket)
  if (!is.null(region) && nzchar(trimws(region))) {
    put_args$region <- region
  }
  ok <- do.call(put_object, put_args)
  if (!isTRUE(ok)) {
    stop(sprintf("Failed to upload image to %s", output_uri), call. = FALSE)
  }
}

build_scatter_plot <- function(df, y_col, y_label, plot_title, min_x = NA_real_, min_y = NA_real_) {
  plot_data <- df
  if (!is.na(min_x)) {
    plot_data <- plot_data %>% filter(ave_py >= min_x)
  }
  if (!is.na(min_y)) {
    plot_data <- plot_data %>% filter(.data[[y_col]] >= min_y)
  }

  if (nrow(plot_data) == 0) {
    stop(
      sprintf("No rows remain for plot '%s' after applying min_x/min_y filters.", plot_title),
      call. = FALSE
    )
  }

  color_key <- plot_data %>%
    distinct(macro_display_id, color_hex) %>%
    arrange(macro_display_id)
  palette <- setNames(color_key$color_hex, as.character(color_key$macro_display_id))

  ggplot(plot_data, aes(x = ave_py, y = .data[[y_col]])) +
    geom_point(aes(color = factor(macro_display_id), size = publications), alpha = 0.72) +
    geom_text_repel(
      aes(label = cluster_code),
      size = 3,
      max.overlaps = 60,
      box.padding = 0.35,
      point.padding = 0.15,
      segment.color = "grey50",
      segment.size = 0.3,
      show.legend = FALSE
    ) +
    scale_color_manual(values = palette, name = "Macro cluster") +
    scale_size_continuous(range = c(1.4, 7), name = "Publications") +
    scale_x_continuous(breaks = scales::pretty_breaks(n = 8)) +
    labs(
      title = plot_title,
      subtitle = "Micro-level clusters, colored by parent macro cluster",
      x = "Average publication year",
      y = y_label
    ) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank())
}

main <- function() {
  args <- parse_cli_args(commandArgs(trailingOnly = TRUE))

  snapshot <- args[["snapshot"]] %||% Sys.getenv("TOPIC_MODEL_SNAPSHOT")
  query <- args[["query"]] %||% Sys.getenv("TOPIC_MODEL_QUERY")
  subquery <- args[["subquery"]] %||% args[["query-folder"]] %||%
    Sys.getenv("TOPIC_MODEL_SUBQUERY") %||% Sys.getenv("TOPIC_MODEL_QUERY_FOLDER")

  if (is.null(snapshot) || !nzchar(snapshot)) {
    stop("Missing snapshot. Provide --snapshot or TOPIC_MODEL_SNAPSHOT.", call. = FALSE)
  }
  if (is.null(query) || !nzchar(query)) {
    stop("Missing query. Provide --query or TOPIC_MODEL_QUERY.", call. = FALSE)
  }
  if (is.null(subquery) || !nzchar(subquery)) {
    stop("Missing subquery. Provide --subquery or TOPIC_MODEL_SUBQUERY.", call. = FALSE)
  }

  aws_region <- args[["aws-region"]] %||%
    Sys.getenv("AWS_REGION") %||%
    Sys.getenv("AWS_DEFAULT_REGION") %||%
    "ap-northeast-1"
  aws_region <- trimws(as.character(aws_region))
  if (!nzchar(aws_region)) {
    aws_region <- "ap-northeast-1"
  }
  # Keep aws.s3 calls pinned to bucket region to avoid PermanentRedirect (HTTP 301).
  Sys.setenv(AWS_REGION = aws_region, AWS_DEFAULT_REGION = aws_region)

  width <- suppressWarnings(as.numeric(args[["width"]] %||% "12"))
  height <- suppressWarnings(as.numeric(args[["height"]] %||% "7"))
  dpi <- suppressWarnings(as.numeric(args[["dpi"]] %||% "240"))
  min_size <- 50
  min_x <- parse_optional_numeric_arg(args[["min_x"]] %||% args[["min-x"]], "min_x")
  min_y <- parse_optional_numeric_arg(args[["min_y"]] %||% args[["min-y"]], "min_y")

  clustering_root <- sprintf(
    "s3://openalex-results/snapshot_%s/queries/%s/network/clustering/",
    snapshot,
    query
  )
  subquery_root <- sprintf("%ssubqueries/%s/", clustering_root, subquery)

  micro_dir <- paste0(subquery_root, "cluster_report_micro/")
  macro_color_dir <- paste0(clustering_root, "cluster_color_macro/")
  charts_dir <- paste0(subquery_root, "charts/")

  cat(sprintf("[config] snapshot: %s\n", snapshot))
  cat(sprintf("[config] query: %s\n", query))
  cat(sprintf("[config] subquery: %s\n", subquery))
  cat(sprintf("[config] aws region: %s\n", aws_region))
  cat(sprintf("[config] min publications per micro: %s\n", min_size))
  cat(sprintf("[config] min_x: %s\n", ifelse(is.na(min_x), "none", format(min_x, trim = TRUE))))
  cat(sprintf("[config] min_y: %s\n", ifelse(is.na(min_y), "none", format(min_y, trim = TRUE))))
  cat(sprintf("[config] micro source: %s\n", micro_dir))
  cat(sprintf("[config] macro colors source: %s\n", macro_color_dir))
  cat(sprintf("[config] output charts dir: %s\n", charts_dir))

  micro <- read_s3_parquet_dataset(micro_dir, region = aws_region)
  colors <- read_s3_parquet_dataset(macro_color_dir, region = aws_region)

  required_micro <- c("micro_cluster", "macro_cluster", "publications", "ave_py", "ave_citations")
  missing_micro <- required_micro[!(required_micro %in% names(micro))]
  if (length(missing_micro) > 0) {
    stop(
      sprintf("cluster_report_micro missing required columns: %s", paste(missing_micro, collapse = ", ")),
      call. = FALSE
    )
  }

  rank_col <- pick_col(
    micro,
    c("yearly_rank_citations", "ranked_citation_score", "ranked_citation"),
    required = TRUE
  )

  if (!("macro_cluster" %in% names(colors)) || !("color_hex" %in% names(colors))) {
    stop("cluster_color_macro must contain columns: macro_cluster, color_hex", call. = FALSE)
  }

  colors_norm <- colors %>%
    transmute(
      macro_cluster = suppressWarnings(as.integer(macro_cluster)),
      color_hex = as.character(color_hex)
    ) %>%
    filter(!is.na(macro_cluster), !is.na(color_hex), trimws(color_hex) != "") %>%
    distinct(macro_cluster, .keep_all = TRUE)

  micro <- ensure_cluster_code(micro)

  plot_df <- micro %>%
    mutate(
      micro_cluster = suppressWarnings(as.integer(micro_cluster)),
      macro_cluster = suppressWarnings(as.integer(macro_cluster)),
      publications = suppressWarnings(as.numeric(publications)),
      ave_py = suppressWarnings(as.numeric(ave_py)),
      ave_citations = suppressWarnings(as.numeric(ave_citations)),
      ranked_metric = suppressWarnings(as.numeric(.data[[rank_col]])),
      cluster_code = trimws(as.character(cluster_code))
    ) %>%
    left_join(colors_norm, by = "macro_cluster") %>%
    mutate(color_hex = ifelse(is.na(color_hex), "#bfbfbf", color_hex)) %>%
    filter(!is.na(ave_py), !is.na(ave_citations), !is.na(ranked_metric), !is.na(publications)) %>%
    mutate(publications = pmax(publications, 0)) %>%
    filter(publications >= min_size) %>%
    add_macro_display_id() %>%
    filter(!is.na(macro_display_id)) %>%
    rebuild_cluster_code_for_plot()

  if (nrow(plot_df) == 0) {
    stop("No rows remain after applying numeric cleanup and MIN_SIZE filter.", call. = FALSE)
  }

  scatter_ave <- build_scatter_plot(
    plot_df,
    y_col = "ave_citations",
    y_label = "Average citations",
    plot_title = "Micro Cluster Landscape: Publication Year vs Citations",
    min_x = min_x,
    min_y = min_y
  )

  scatter_rank <- build_scatter_plot(
    plot_df,
    y_col = "ranked_metric",
    y_label = sprintf("Ranked normalized citations (%s)", rank_col),
    plot_title = "Micro Cluster Landscape: Publication Year vs Ranked Citations",
    min_x = min_x,
    min_y = min_y
  )

  out_ave <- join_s3(charts_dir, "fig_scatter_micro_PY_x_Z9.png")
  out_rank <- join_s3(charts_dir, "fig_scatter_micro_PY_x_Z9_rank.png")

  save_plot_to_s3(scatter_ave, out_ave, width = width, height = height, dpi = dpi, region = aws_region)
  save_plot_to_s3(scatter_rank, out_rank, width = width, height = height, dpi = dpi, region = aws_region)

  cat(sprintf("[done] wrote scatter plot: %s\n", out_ave))
  cat(sprintf("[done] wrote scatter plot: %s\n", out_rank))
}

main()
