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

get_script_dir <- function() {
  all_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- all_args[grepl("^--file=", all_args)]
  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[[1]]))))
  }
  getwd()
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
    tmp_parquet <- tempfile(pattern = "level_bar_part_", fileext = ".parquet")
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

parse_positive_int <- function(raw_value, arg_name, default_value) {
  if (is.null(raw_value) || !nzchar(trimws(as.character(raw_value)))) {
    return(as.integer(default_value))
  }
  value <- suppressWarnings(as.integer(raw_value))
  if (is.na(value) || value <= 0) {
    stop(sprintf("Invalid positive integer for --%s: %s", arg_name, as.character(raw_value)), call. = FALSE)
  }
  value
}

parse_positive_numeric <- function(raw_value, arg_name, default_value) {
  if (is.null(raw_value) || !nzchar(trimws(as.character(raw_value)))) {
    return(as.numeric(default_value))
  }
  value <- suppressWarnings(as.numeric(raw_value))
  if (is.na(value) || value <= 0) {
    stop(sprintf("Invalid positive number for --%s: %s", arg_name, as.character(raw_value)), call. = FALSE)
  }
  value
}

parse_levels <- function(raw_level) {
  valid <- c("macro", "meso", "micro")
  if (is.null(raw_level) || !nzchar(trimws(as.character(raw_level)))) {
    return(valid)
  }

  tokens <- unlist(strsplit(raw_level, ",", fixed = TRUE), use.names = FALSE)
  tokens <- trimws(tolower(tokens))
  tokens <- tokens[tokens != ""]
  if (length(tokens) == 0) {
    return(valid)
  }

  bad <- tokens[!(tokens %in% valid)]
  if (length(bad) > 0) {
    stop(sprintf("Invalid --level value(s): %s", paste(unique(bad), collapse = ", ")), call. = FALSE)
  }

  unique(tokens)
}

build_cluster_code_fallback_micro <- function(df) {
  required <- c("micro_cluster", "macro_cluster", "publications")
  missing <- required[!(required %in% names(df))]
  if (length(missing) > 0) {
    return(rep("", nrow(df)))
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
      micro_rank %>% rename(fallback_cluster_code = cluster_code),
      by = c("micro_cluster_num" = "micro_cluster", "macro_cluster_num" = "macro_cluster")
    )

  out$fallback_cluster_code <- ifelse(
    is.na(out$fallback_cluster_code),
    "",
    as.character(out$fallback_cluster_code)
  )
  out$fallback_cluster_code
}

ensure_micro_cluster_code <- function(df) {
  fallback <- build_cluster_code_fallback_micro(df)
  existing <- if ("cluster_code" %in% names(df)) safe_trim(df$cluster_code) else rep("", nrow(df))
  valid <- grepl("^[1-9][0-9]*-[1-9][0-9]*$", existing)
  needs_fill <- existing == "" | !valid
  existing[needs_fill] <- fallback[needs_fill]
  existing[is.na(existing)] <- ""
  existing
}

panel_label_from_id <- function(panel_id, macros_per_panel, max_macro_display_id) {
  start_id <- (panel_id - 1L) * macros_per_panel + 1L
  end_id <- min(panel_id * macros_per_panel, max_macro_display_id)
  sprintf("Panel %02d (Macro %d-%d)", panel_id, start_id, end_id)
}

save_plot_dual <- function(plot_obj, s3_uri, local_path, width, height, dpi, region = NULL) {
  dir.create(dirname(local_path), recursive = TRUE, showWarnings = FALSE)

  ggsave(
    filename = local_path,
    plot = plot_obj,
    width = width,
    height = height,
    units = "in",
    dpi = dpi,
    bg = "white"
  )

  parsed <- parse_s3_uri(s3_uri)
  put_args <- list(file = local_path, object = parsed$key, bucket = parsed$bucket)
  if (!is.null(region) && nzchar(trimws(region))) {
    put_args$region <- region
  }
  ok <- do.call(put_object, put_args)
  if (!isTRUE(ok)) {
    stop(sprintf("Failed to upload image to %s", s3_uri), call. = FALSE)
  }
}

prepare_level_bar_data <- function(level, report_df, colors_df, names_df, min_size, top_per_parent) {
  id_candidates <- switch(
    level,
    macro = c("macro_cluster", "cluster"),
    meso = c("meso_cluster", "cluster"),
    micro = c("micro_cluster", "cluster")
  )
  id_col <- pick_col(report_df, id_candidates, required = TRUE)

  if (!("publications" %in% names(report_df))) {
    stop(sprintf("cluster_report_%s missing required column: publications", level), call. = FALSE)
  }

  name_col <- pick_col(report_df, c("short_name", "name", "cluster_name"), required = FALSE)

  df <- report_df %>%
    mutate(
      cluster_id = suppressWarnings(as.integer(.data[[id_col]])),
      publications = suppressWarnings(as.numeric(publications)),
      macro_parent = if ("macro_cluster" %in% names(report_df)) suppressWarnings(as.integer(macro_cluster)) else NA_integer_,
      short_name = if (!is.null(name_col)) safe_trim(.data[[name_col]]) else "",
      cluster_code = if ("cluster_code" %in% names(report_df)) safe_trim(cluster_code) else ""
    ) %>%
    filter(!is.na(cluster_id), !is.na(publications)) %>%
    mutate(publications = pmax(publications, 0)) %>%
    filter(publications >= min_size)

  if (nrow(df) == 0) {
    return(data.frame())
  }

  if (level == "macro") {
    df$macro_parent <- df$cluster_id
  } else {
    df$macro_parent[is.na(df$macro_parent)] <- df$cluster_id[is.na(df$macro_parent)]
  }

  df <- df %>% arrange(cluster_id, desc(publications)) %>% distinct(cluster_id, .keep_all = TRUE)

  if (level == "micro" && nrow(names_df) > 0 && ("micro_cluster" %in% names(names_df))) {
    names_col <- pick_col(names_df, c("short_name", "name", "cluster_name"), required = FALSE)
    if (!is.null(names_col)) {
      names_clean <- names_df %>%
        transmute(
          micro_cluster = suppressWarnings(as.integer(micro_cluster)),
          short_name_named = safe_trim(.data[[names_col]])
        ) %>%
        filter(!is.na(micro_cluster), short_name_named != "") %>%
        distinct(micro_cluster, .keep_all = TRUE)

      df <- df %>%
        left_join(names_clean, by = c("cluster_id" = "micro_cluster")) %>%
        mutate(short_name = ifelse(short_name == "" & !is.na(short_name_named), short_name_named, short_name)) %>%
        select(-any_of("short_name_named"))
    }
  }

  if (level == "micro") {
    tmp_micro <- df %>%
      transmute(
        micro_cluster = cluster_id,
        macro_cluster = macro_parent,
        publications = publications,
        cluster_code = cluster_code
      )
    df$cluster_code <- ensure_micro_cluster_code(tmp_micro)
    df$cluster_code[df$cluster_code == ""] <- as.character(df$cluster_id[df$cluster_code == ""])
  } else {
    df <- df %>%
      arrange(desc(publications), cluster_id) %>%
      mutate(display_id = row_number(), cluster_code = as.character(display_id))
  }

  macro_rank <- df %>%
    group_by(macro_parent) %>%
    summarise(macro_publications = sum(publications, na.rm = TRUE), .groups = "drop") %>%
    arrange(desc(macro_publications), macro_parent) %>%
    mutate(macro_display_id = row_number())

  color_map <- colors_df %>%
    transmute(
      macro_cluster = suppressWarnings(as.integer(macro_cluster)),
      color_hex = safe_trim(color_hex)
    ) %>%
    filter(!is.na(macro_cluster), color_hex != "") %>%
    distinct(macro_cluster, .keep_all = TRUE)

  out <- df %>%
    left_join(macro_rank, by = c("macro_parent" = "macro_parent")) %>%
    left_join(color_map, by = c("macro_parent" = "macro_cluster")) %>%
    mutate(
      macro_display_id = ifelse(is.na(macro_display_id), 0L, macro_display_id),
      color_hex = ifelse(is.na(color_hex) | color_hex == "", "#bfbfbf", color_hex),
      short_name = safe_trim(short_name),
      label_text = ifelse(short_name == "", cluster_code, paste0(cluster_code, ". ", short_name)),
      label_text = truncate_text(label_text, max_chars = 64)
    )

  if (level %in% c("meso", "micro")) {
    out <- out %>%
      arrange(macro_display_id, desc(publications), cluster_id) %>%
      group_by(macro_display_id) %>%
      slice_head(n = top_per_parent) %>%
      ungroup()
  }

  out
}

build_bar_plot <- function(plot_df, level, min_size, top_per_parent, macros_per_panel) {
  max_macro_display <- max(plot_df$macro_display_id, na.rm = TRUE)
  panel_summary <- plot_df %>%
    distinct(macro_display_id) %>%
    arrange(macro_display_id) %>%
    mutate(panel_id = ((macro_display_id - 1L) %/% macros_per_panel) + 1L) %>%
    distinct(panel_id) %>%
    arrange(panel_id) %>%
    mutate(panel_label = sapply(panel_id, panel_label_from_id, macros_per_panel = macros_per_panel, max_macro_display_id = max_macro_display))

  plot_df <- plot_df %>%
    mutate(panel_id = ((macro_display_id - 1L) %/% macros_per_panel) + 1L) %>%
    left_join(panel_summary, by = "panel_id") %>%
    mutate(y_key = paste0("P", panel_id, "__", label_text))

  desired_order <- plot_df %>%
    arrange(panel_id, macro_display_id, desc(publications), cluster_id) %>%
    pull(y_key)
  plot_df$y_key <- factor(plot_df$y_key, levels = rev(unique(desired_order)))

  panel_count <- max(plot_df$panel_id, na.rm = TRUE)
  panel_cols <- max(1L, ceiling(sqrt(panel_count)))

  fill_map <- plot_df %>%
    distinct(macro_display_id, color_hex) %>%
    arrange(macro_display_id)
  palette <- setNames(fill_map$color_hex, as.character(fill_map$macro_display_id))

  subtitle <- if (level == "macro") {
    sprintf("Filtered: publications >= %d", min_size)
  } else {
    sprintf("Filtered: publications >= %d; max %d clusters per macro", min_size, top_per_parent)
  }

  plt <- ggplot(plot_df, aes(x = publications, y = y_key)) +
    geom_col(aes(fill = factor(macro_display_id)), width = 0.62, alpha = 0.95) +
    scale_fill_manual(values = palette, name = "Macro") +
    scale_x_sqrt(labels = scales::label_number(big.mark = ",", accuracy = 1)) +
    scale_y_discrete(labels = function(x) sub("^P[0-9]+__", "", x)) +
    facet_wrap(~panel_label, scales = "free_y", ncol = panel_cols) +
    labs(
      title = sprintf("%s Clusters: Documents per Cluster", tools::toTitleCase(level)),
      subtitle = subtitle,
      x = "Number of documents (sqrt scale)",
      y = sprintf("%s cluster", level)
    ) +
    theme_minimal(base_size = 11) +
    theme(
      panel.grid.minor = element_blank(),
      panel.grid.major.y = element_blank(),
      axis.text.y = element_text(size = 6.6),
      strip.text = element_text(face = "bold", size = 10),
      legend.position = "none",
      plot.title = element_text(face = "bold", size = 16),
      plot.subtitle = element_text(size = 10)
    )

  list(plot = plt, panel_cols = panel_cols, panel_count = panel_count)
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

  levels <- parse_levels(args[["level"]])

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
  top_per_parent <- parse_positive_int(args[["top-per-parent"]] %||% args[["top_per_parent"]] %||% args[["top-per-macro"]] %||% args[["top_per_macro"]], "top-per-parent", 10)
  macros_per_panel <- parse_positive_int(args[["macros-per-panel"]] %||% args[["macros_per_panel"]], "macros-per-panel", 6)
  panel_width <- parse_positive_numeric(args[["panel-width"]] %||% args[["panel_width"]], "panel-width", 8)
  panel_height <- parse_positive_numeric(args[["panel-height"]] %||% args[["panel_height"]], "panel-height", 6)
  dpi <- parse_positive_numeric(args[["dpi"]], "dpi", 240)

  script_dir <- get_script_dir()
  repo_root <- normalizePath(file.path(script_dir, ".."), winslash = "/", mustWork = FALSE)

  clustering_root <- sprintf(
    "s3://openalex-results/snapshot_%s/queries/%s/network/clustering/",
    snapshot,
    query
  )
  subquery_root <- sprintf("%ssubqueries/%s/", clustering_root, subquery)

  colors_dir <- paste0(clustering_root, "cluster_color_macro/")
  names_dir <- paste0(subquery_root, "cluster_names/")

  local_root <- file.path(
    repo_root,
    "06-subquery_reports",
    "excel",
    sprintf("snapshot_%s_%s", snapshot, query),
    subquery
  )

  cat(sprintf("[config] snapshot: %s\n", snapshot))
  cat(sprintf("[config] query: %s\n", query))
  cat(sprintf("[config] subquery: %s\n", subquery))
  cat(sprintf("[config] levels: %s\n", paste(levels, collapse = ", ")))
  cat(sprintf("[config] aws region: %s\n", aws_region))
  cat(sprintf("[config] min publications: %d\n", min_size))
  cat(sprintf("[config] top per macro-parent: %d\n", top_per_parent))
  cat(sprintf("[config] macros per panel: %d\n", macros_per_panel))
  cat(sprintf("[config] local output root: %s\n", local_root))

  colors <- read_s3_parquet_dataset(colors_dir, region = aws_region, required = TRUE)
  if (!("macro_cluster" %in% names(colors)) || !("color_hex" %in% names(colors))) {
    stop("cluster_color_macro must contain columns: macro_cluster, color_hex", call. = FALSE)
  }

  names_df <- read_s3_parquet_dataset(names_dir, region = aws_region, required = FALSE)

  for (level in levels) {
    level_report_dir <- paste0(subquery_root, "cluster_report_", level, "/")
    level_s3_dir <- paste0(subquery_root, "charts/", level, "/")
    level_local_dir <- file.path(local_root, level)

    cat(sprintf("[level:%s] source: %s\n", level, level_report_dir))
    cat(sprintf("[level:%s] s3 output dir: %s\n", level, level_s3_dir))
    cat(sprintf("[level:%s] local output dir: %s\n", level, level_local_dir))

    report_df <- read_s3_parquet_dataset(level_report_dir, region = aws_region, required = TRUE)
    plot_df <- prepare_level_bar_data(
      level,
      report_df,
      colors,
      names_df,
      min_size = min_size,
      top_per_parent = top_per_parent
    )

    if (nrow(plot_df) == 0) {
      cat(sprintf("[level:%s] skipped (no rows after filters)\n", level))
      next
    }

    built <- build_bar_plot(
      plot_df,
      level = level,
      min_size = min_size,
      top_per_parent = top_per_parent,
      macros_per_panel = macros_per_panel
    )

    panel_rows <- max(1L, ceiling(built$panel_count / built$panel_cols))
    width <- built$panel_cols * panel_width
    height <- panel_rows * panel_height

    out_name <- sprintf(
      "fig_bars_%s_min%d_top%d_mpp%d.png",
      level,
      min_size,
      top_per_parent,
      macros_per_panel
    )

    out_s3 <- join_s3(level_s3_dir, out_name)
    out_local <- file.path(level_local_dir, out_name)

    save_plot_dual(
      built$plot,
      out_s3,
      out_local,
      width = width,
      height = height,
      dpi = dpi,
      region = aws_region
    )

    cat(sprintf("[summary] level=%s rows=%d panels=%d (%d x %d)\n", level, nrow(plot_df), built$panel_count, built$panel_cols, panel_rows))
    cat(sprintf("[done] %s\n", out_s3))
    cat(sprintf("[done] %s\n", out_local))
  }
}

main()
