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
    tmp_parquet <- tempfile(pattern = "level_scatter_part_", fileext = ".parquet")
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

pick_rank_col <- function(df) {
  candidates <- c("yearly_rank_citations", "ranked_citation_score", "ranked_citation")
  pick_col(df, candidates, required = FALSE)
}

rank_label_from_col <- function(rank_col) {
  if (is.null(rank_col)) {
    return("Ranked citations")
  }
  labels <- list(
    yearly_rank_citations = "Yearly rank citations",
    ranked_citation_score = "Ranked citation score",
    ranked_citation = "Ranked citations"
  )
  labels[[rank_col]] %||% "Ranked citations"
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

prepare_level_scatter_data <- function(level, report_df, colors_df, min_size, rank_col = NULL) {
  id_candidates <- switch(
    level,
    macro = c("macro_cluster", "cluster"),
    meso = c("meso_cluster", "cluster"),
    micro = c("micro_cluster", "cluster")
  )

  id_col <- pick_col(report_df, id_candidates, required = TRUE)
  req <- c(id_col, "publications", "ave_py", "ave_citations")
  missing <- req[!(req %in% names(report_df))]
  if (length(missing) > 0) {
    stop(
      sprintf("cluster_report_%s missing required columns: %s", level, paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }

  name_col <- pick_col(report_df, c("short_name", "name", "cluster_name"), required = FALSE)

  df <- report_df %>%
    mutate(
      cluster_id = suppressWarnings(as.integer(.data[[id_col]])),
      publications = suppressWarnings(as.numeric(publications)),
      ave_py = suppressWarnings(as.numeric(ave_py)),
      ave_citations = suppressWarnings(as.numeric(ave_citations)),
      rank_metric = if (!is.null(rank_col)) suppressWarnings(as.numeric(.data[[rank_col]])) else NA_real_,
      macro_parent = if ("macro_cluster" %in% names(report_df)) suppressWarnings(as.integer(macro_cluster)) else NA_integer_,
      short_name = if (!is.null(name_col)) safe_trim(.data[[name_col]]) else "",
      cluster_code = if ("cluster_code" %in% names(report_df)) safe_trim(cluster_code) else ""
    ) %>%
    filter(!is.na(cluster_id), !is.na(publications), !is.na(ave_py), !is.na(ave_citations)) %>%
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
      label_text = ifelse(short_name == "", cluster_code, paste0(cluster_code, ". ", short_name))
    )

  out
}

build_scatter_plot <- function(
  df,
  level,
  y_col = "ave_citations",
  y_axis_label = "Average citations",
  y_title_label = "Citations",
  min_x = NA_real_,
  min_y = NA_real_
) {
  plot_df <- df
  if (!is.na(min_x)) {
    plot_df <- plot_df %>% filter(ave_py >= min_x)
  }
  if (!is.na(min_y)) {
    plot_df <- plot_df %>% filter(.data[[y_col]] >= min_y)
  }

  plot_df <- plot_df %>% filter(!is.na(.data[[y_col]]))

  if (nrow(plot_df) == 0) {
    stop(sprintf("No rows remain for %s scatter after filters.", level), call. = FALSE)
  }

  color_key <- plot_df %>%
    distinct(macro_display_id, color_hex) %>%
    arrange(macro_display_id)
  palette <- setNames(color_key$color_hex, as.character(color_key$macro_display_id))

  subtitle <- sprintf("%s-level clusters, colored by parent macro cluster", tools::toTitleCase(level))

  ggplot(plot_df, aes(x = ave_py, y = .data[[y_col]])) +
    geom_point(aes(color = factor(macro_display_id), size = publications), alpha = 0.74) +
    geom_text_repel(
      aes(label = label_text),
      size = 3,
      max.overlaps = 80,
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
      title = sprintf("%s Cluster Landscape: Publication Year vs %s", tools::toTitleCase(level), y_title_label),
      subtitle = subtitle,
      x = "Average publication year",
      y = y_axis_label
    ) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank())
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

  width <- parse_positive_numeric(args[["width"]], "width", 12)
  height <- parse_positive_numeric(args[["height"]], "height", 7)
  dpi <- parse_positive_numeric(args[["dpi"]], "dpi", 240)
  min_size <- parse_positive_numeric(args[["min-size"]] %||% args[["min_size"]], "min-size", 50)

  script_dir <- get_script_dir()
  repo_root <- normalizePath(file.path(script_dir, ".."), winslash = "/", mustWork = FALSE)

  clustering_root <- sprintf(
    "s3://openalex-results/snapshot_%s/queries/%s/network/clustering/",
    snapshot,
    query
  )
  subquery_root <- sprintf("%ssubqueries/%s/", clustering_root, subquery)
  colors_dir <- paste0(clustering_root, "cluster_color_macro/")

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
  cat(sprintf("[config] min publications per cluster: %.0f\n", min_size))
  cat(sprintf("[config] local output root: %s\n", local_root))

  colors <- read_s3_parquet_dataset(colors_dir, region = aws_region, required = TRUE)
  if (!("macro_cluster" %in% names(colors)) || !("color_hex" %in% names(colors))) {
    stop("cluster_color_macro must contain columns: macro_cluster, color_hex", call. = FALSE)
  }

  for (level in levels) {
    level_report_dir <- paste0(subquery_root, "cluster_report_", level, "/")
    level_s3_dir <- paste0(subquery_root, "charts/", level, "/")
    level_local_dir <- file.path(local_root, level)

    cat(sprintf("[level:%s] source: %s\n", level, level_report_dir))
    cat(sprintf("[level:%s] s3 output dir: %s\n", level, level_s3_dir))
    cat(sprintf("[level:%s] local output dir: %s\n", level, level_local_dir))

    report_df <- read_s3_parquet_dataset(level_report_dir, region = aws_region, required = TRUE)
    rank_col <- pick_rank_col(report_df)
    if (is.null(rank_col)) {
      cat(sprintf("[level:%s] rank metric missing; rank scatter will be skipped\n", level))
    } else {
      cat(sprintf("[level:%s] rank metric column: %s\n", level, rank_col))
    }

    plot_df <- prepare_level_scatter_data(level, report_df, colors, min_size = min_size, rank_col = rank_col)

    if (nrow(plot_df) == 0) {
      cat(sprintf("[level:%s] skipped (no rows after filters)\n", level))
      next
    }

    default_plot <- build_scatter_plot(plot_df, level = level)
    default_name <- sprintf("fig_scatter_%s_PY_x_Z9.png", level)
    default_s3 <- join_s3(level_s3_dir, default_name)
    default_local <- file.path(level_local_dir, default_name)

    save_plot_dual(default_plot, default_s3, default_local, width = width, height = height, dpi = dpi, region = aws_region)
    cat(sprintf("[done] %s\n", default_s3))
    cat(sprintf("[done] %s\n", default_local))

    has_rank <- !is.null(rank_col) && any(!is.na(plot_df$rank_metric))
    if (has_rank) {
      rank_plot <- build_scatter_plot(
        plot_df,
        level = level,
        y_col = "rank_metric",
        y_axis_label = rank_label_from_col(rank_col),
        y_title_label = "Z9 Rank"
      )
      rank_name <- sprintf("fig_scatter_%s_PY_x_Z9_rank.png", level)
      rank_s3 <- join_s3(level_s3_dir, rank_name)
      rank_local <- file.path(level_local_dir, rank_name)

      save_plot_dual(rank_plot, rank_s3, rank_local, width = width, height = height, dpi = dpi, region = aws_region)
      cat(sprintf("[done] %s\n", rank_s3))
      cat(sprintf("[done] %s\n", rank_local))
    } else {
      cat(sprintf("[level:%s] skipped rank scatter (no non-null rank values)\n", level))
    }

    if (level == "micro") {
      micro_extra <- tryCatch(
        build_scatter_plot(plot_df, level = level, min_x = 2020, min_y = 0.6),
        error = function(e) {
          cat(sprintf("[level:%s] skipped filtered citation scatter: %s\n", level, conditionMessage(e)))
          NULL
        }
      )
      if (!is.null(micro_extra)) {
        extra_name <- "fig_scatter_micro_PY_x_Z9_minx2020_miny0p6.png"
        extra_s3 <- join_s3(level_s3_dir, extra_name)
        extra_local <- file.path(level_local_dir, extra_name)

        save_plot_dual(micro_extra, extra_s3, extra_local, width = width, height = height, dpi = dpi, region = aws_region)
        cat(sprintf("[done] %s\n", extra_s3))
        cat(sprintf("[done] %s\n", extra_local))
      }

      if (has_rank) {
        micro_rank_extra <- tryCatch(
          build_scatter_plot(
            plot_df,
            level = level,
            y_col = "rank_metric",
            y_axis_label = rank_label_from_col(rank_col),
            y_title_label = "Z9 Rank",
            min_x = 2020,
            min_y = 0.6
          ),
          error = function(e) {
            cat(sprintf("[level:%s] skipped filtered rank scatter: %s\n", level, conditionMessage(e)))
            NULL
          }
        )
        if (!is.null(micro_rank_extra)) {
          extra_rank_name <- "fig_scatter_micro_PY_x_Z9_rank_minx2020_miny0p6.png"
          extra_rank_s3 <- join_s3(level_s3_dir, extra_rank_name)
          extra_rank_local <- file.path(level_local_dir, extra_rank_name)

          save_plot_dual(
            micro_rank_extra,
            extra_rank_s3,
            extra_rank_local,
            width = width,
            height = height,
            dpi = dpi,
            region = aws_region
          )
          cat(sprintf("[done] %s\n", extra_rank_s3))
          cat(sprintf("[done] %s\n", extra_rank_local))
        }
      }
    }
  }
}

main()
