#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(aws.s3)
  library(arrow)
  library(dplyr)
  library(ggplot2)
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

read_s3_parquet_dataset <- function(dir_uri, region = NULL, required = TRUE) {
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
    if (required) {
      stop(sprintf("No objects found in %s", dir_uri), call. = FALSE)
    }
    return(data.frame())
  }

  keys <- vapply(objs, function(x) x[["Key"]], character(1), USE.NAMES = FALSE)
  parquet_keys <- keys[grepl("\\.parquet$", keys)]
  if (length(parquet_keys) == 0) {
    if (required) {
      stop(sprintf("No parquet files found in %s", dir_uri), call. = FALSE)
    }
    return(data.frame())
  }

  parts <- lapply(parquet_keys, function(obj_key) {
    tmp_parquet <- tempfile(pattern = "micro_bar_part_", fileext = ".parquet")
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

parse_positive_int <- function(raw_value, arg_name, default_value) {
  if (is.null(raw_value) || !nzchar(trimws(as.character(raw_value)))) {
    return(as.integer(default_value))
  }
  value <- suppressWarnings(as.integer(raw_value))
  if (is.na(value) || value <= 0) {
    stop(sprintf("Invalid positive integer for --%s: %s", arg_name, as.character(raw_value)), call. = FALSE)
  }
  as.integer(value)
}

parse_positive_numeric <- function(raw_value, arg_name, default_value) {
  if (is.null(raw_value) || !nzchar(trimws(as.character(raw_value)))) {
    return(as.numeric(default_value))
  }
  value <- suppressWarnings(as.numeric(raw_value))
  if (is.na(value) || value <= 0) {
    stop(sprintf("Invalid positive number for --%s: %s", arg_name, as.character(raw_value)), call. = FALSE)
  }
  as.numeric(value)
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
  valid_nonzero <- grepl("^[1-9][0-9]*-[1-9][0-9]*$", existing)
  needs_fill <- is.na(existing) | existing == "" | !valid_nonzero
  existing[needs_fill] <- fallback[needs_fill]
  df$cluster_code <- existing
  df
}

safe_trim <- function(x) {
  out <- trimws(as.character(x))
  out[is.na(out)] <- ""
  out
}

truncate_text <- function(x, max_chars = 64) {
  sapply(
    x,
    function(item) {
      txt <- as.character(item)
      if (is.na(txt) || !nzchar(txt)) {
        return("")
      }
      if (nchar(txt) <= max_chars) {
        return(txt)
      }
      paste0(substr(txt, 1, max_chars - 1), "...")
    },
    USE.NAMES = FALSE
  )
}

panel_label_from_id <- function(panel_id, macros_per_panel, max_macro_display_id) {
  start_id <- (panel_id - 1L) * macros_per_panel + 1L
  end_id <- min(panel_id * macros_per_panel, max_macro_display_id)
  sprintf("Panel %02d (Macro %d-%d)", panel_id, start_id, end_id)
}

save_plot_to_s3 <- function(plot_obj, output_uri, width, height, dpi, region = NULL) {
  tmp_png <- tempfile(pattern = "micro_bars_", fileext = ".png")
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
  Sys.setenv(AWS_REGION = aws_region, AWS_DEFAULT_REGION = aws_region)

  min_size <- parse_positive_int(args[["min-size"]] %||% args[["min_size"]], "min-size", 50)
  top_per_macro <- parse_positive_int(args[["top-per-macro"]] %||% args[["top_per_macro"]], "top-per-macro", 10)
  macros_per_panel <- parse_positive_int(args[["macros-per-panel"]] %||% args[["macros_per_panel"]], "macros-per-panel", 6)

  panel_width <- parse_positive_numeric(args[["panel-width"]] %||% args[["panel_width"]], "panel-width", 8)
  panel_height <- parse_positive_numeric(args[["panel-height"]] %||% args[["panel_height"]], "panel-height", 6)
  dpi <- parse_positive_numeric(args[["dpi"]], "dpi", 240)

  clustering_root <- sprintf(
    "s3://openalex-results/snapshot_%s/queries/%s/network/clustering/",
    snapshot,
    query
  )
  subquery_root <- sprintf("%ssubqueries/%s/", clustering_root, subquery)

  micro_dir <- paste0(subquery_root, "cluster_report_micro/")
  names_dir <- paste0(subquery_root, "cluster_names/")
  macro_color_dir <- paste0(clustering_root, "cluster_color_macro/")
  charts_dir <- paste0(subquery_root, "charts/")

  cat(sprintf("[config] snapshot: %s\n", snapshot))
  cat(sprintf("[config] query: %s\n", query))
  cat(sprintf("[config] subquery: %s\n", subquery))
  cat(sprintf("[config] aws region: %s\n", aws_region))
  cat(sprintf("[config] min publications per micro: %d\n", min_size))
  cat(sprintf("[config] top micro per macro: %d\n", top_per_macro))
  cat(sprintf("[config] macros per panel: %d\n", macros_per_panel))
  cat(sprintf("[config] panel width (in): %.2f\n", panel_width))
  cat(sprintf("[config] panel height (in): %.2f\n", panel_height))
  cat(sprintf("[config] micro source: %s\n", micro_dir))
  cat(sprintf("[config] names source (optional): %s\n", names_dir))
  cat(sprintf("[config] macro colors source: %s\n", macro_color_dir))
  cat(sprintf("[config] output charts dir: %s\n", charts_dir))

  micro <- read_s3_parquet_dataset(micro_dir, region = aws_region, required = TRUE)
  names_df <- read_s3_parquet_dataset(names_dir, region = aws_region, required = FALSE)
  colors <- read_s3_parquet_dataset(macro_color_dir, region = aws_region, required = TRUE)

  required_micro <- c("micro_cluster", "macro_cluster", "publications")
  missing_micro <- required_micro[!(required_micro %in% names(micro))]
  if (length(missing_micro) > 0) {
    stop(
      sprintf("cluster_report_micro missing required columns: %s", paste(missing_micro, collapse = ", ")),
      call. = FALSE
    )
  }

  micro <- micro %>%
    mutate(
      micro_cluster = suppressWarnings(as.integer(micro_cluster)),
      macro_cluster = suppressWarnings(as.integer(macro_cluster)),
      publications = suppressWarnings(as.numeric(publications))
    ) %>%
    filter(!is.na(micro_cluster), !is.na(macro_cluster), !is.na(publications)) %>%
    mutate(publications = pmax(publications, 0))

  micro <- ensure_cluster_code(micro)
  micro$cluster_code <- safe_trim(micro$cluster_code)

  filtered <- micro %>%
    filter(publications >= min_size)

  if (nrow(filtered) == 0) {
    stop("No rows remain after minimum publications filter.", call. = FALSE)
  }

  macro_rank <- filtered %>%
    group_by(macro_cluster) %>%
    summarise(macro_publications = sum(publications, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(macro_publications), macro_cluster) %>%
    mutate(macro_display_id = row_number())

  filtered <- filtered %>%
    left_join(macro_rank, by = "macro_cluster") %>%
    arrange(macro_display_id, desc(publications), micro_cluster) %>%
    group_by(macro_display_id) %>%
    mutate(micro_rank = row_number()) %>%
    ungroup() %>%
    mutate(cluster_code = paste0(macro_display_id, "-", micro_rank))

  selected <- filtered %>%
    arrange(macro_display_id, desc(publications), micro_cluster) %>%
    group_by(macro_display_id) %>%
    slice_head(n = top_per_macro) %>%
    ungroup()

  if (nrow(selected) == 0) {
    stop("No rows remain after top-per-macro selection.", call. = FALSE)
  }

  names_clean <- data.frame()
  if (nrow(names_df) > 0 && ("micro_cluster" %in% names(names_df))) {
    name_col <- pick_col(names_df, c("short_name", "name", "cluster_name"), required = FALSE)
    if (!is.null(name_col)) {
      names_clean <- names_df %>%
        transmute(
          micro_cluster = suppressWarnings(as.integer(micro_cluster)),
          short_name = safe_trim(.data[[name_col]])
        ) %>%
        filter(!is.na(micro_cluster), short_name != "") %>%
        distinct(micro_cluster, .keep_all = TRUE)
    }
  }

  colors_clean <- colors %>%
    transmute(
      macro_cluster = suppressWarnings(as.integer(macro_cluster)),
      color_hex = safe_trim(color_hex)
    ) %>%
    filter(!is.na(macro_cluster), color_hex != "") %>%
    distinct(macro_cluster, .keep_all = TRUE)

  plot_df <- selected %>%
    left_join(names_clean, by = "micro_cluster") %>%
    left_join(colors_clean, by = "macro_cluster") %>%
    mutate(
      short_name = safe_trim(short_name),
      color_hex = ifelse(is.na(color_hex) | color_hex == "", "#bfbfbf", color_hex),
      label_text = ifelse(short_name == "", cluster_code, paste0(cluster_code, ". ", short_name)),
      label_text = truncate_text(label_text, max_chars = 64),
      panel_id = ((macro_display_id - 1L) %/% macros_per_panel) + 1L
    )

  if (nrow(plot_df) == 0) {
    stop("No rows remain after joining names/colors.", call. = FALSE)
  }

  max_macro_display <- max(plot_df$macro_display_id, na.rm = TRUE)
  panel_summary <- plot_df %>%
    distinct(panel_id) %>%
    arrange(panel_id) %>%
    mutate(panel_label = sapply(panel_id, panel_label_from_id, macros_per_panel = macros_per_panel, max_macro_display_id = max_macro_display))

  plot_df <- plot_df %>%
    left_join(panel_summary, by = "panel_id") %>%
    mutate(y_key = paste0("P", panel_id, "__", label_text))

  desired_order <- plot_df %>%
    arrange(panel_id, macro_display_id, desc(publications), micro_cluster) %>%
    pull(y_key)
  plot_df$y_key <- factor(plot_df$y_key, levels = rev(unique(desired_order)))

  panel_count <- max(plot_df$panel_id, na.rm = TRUE)
  panel_cols <- max(1L, ceiling(sqrt(panel_count)))
  panel_rows <- max(1L, ceiling(panel_count / panel_cols))

  fill_map <- plot_df %>%
    distinct(macro_display_id, color_hex) %>%
    arrange(macro_display_id)
  palette <- setNames(fill_map$color_hex, as.character(fill_map$macro_display_id))

  bars_plot <- ggplot(plot_df, aes(x = publications, y = y_key)) +
    geom_col(aes(fill = factor(macro_display_id)), width = 0.62, alpha = 0.95) +
    scale_fill_manual(values = palette, name = "Macro") +
    scale_x_sqrt(labels = scales::label_number(big.mark = ",", accuracy = 1)) +
    scale_y_discrete(labels = function(x) sub("^P[0-9]+__", "", x)) +
    facet_wrap(~panel_label, scales = "free_y", ncol = panel_cols) +
    labs(
      title = "Documents per Micro Cluster",
      subtitle = sprintf(
        "Filtered: publications >= %d; max %d micro clusters per macro; %d macro clusters per panel",
        min_size,
        top_per_macro,
        macros_per_panel
      ),
      x = "Number of documents (sqrt scale)",
      y = "Micro cluster"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      panel.grid.minor = element_blank(),
      panel.grid.major.y = element_blank(),
      axis.text.y = element_text(size = 6.6),
      strip.text = element_text(face = "bold", size = 10),
      legend.position = "none",
      legend.key.width = grid::unit(10, "pt"),
      plot.title = element_text(face = "bold", size = 16),
      plot.subtitle = element_text(size = 10)
    )

  file_name <- sprintf(
    "fig_micro_cluster_bars_min%d_top%d_mpp%d.png",
    min_size,
    top_per_macro,
    macros_per_panel
  )
  output_uri <- join_s3(charts_dir, file_name)

  width <- panel_cols * panel_width
  height <- panel_rows * panel_height

  save_plot_to_s3(bars_plot, output_uri, width = width, height = height, dpi = dpi, region = aws_region)

  macro_count <- n_distinct(plot_df$macro_display_id)
  micro_count <- nrow(plot_df)
  cat(sprintf("[summary] macros in chart: %d\n", macro_count))
  cat(sprintf("[summary] micro rows in chart: %d\n", micro_count))
  cat(sprintf("[summary] panel count: %d (%d cols x %d rows)\n", panel_count, panel_cols, panel_rows))
  cat(sprintf("[done] wrote bar chart: %s\n", output_uri))
}

main()
