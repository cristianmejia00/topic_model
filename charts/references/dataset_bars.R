print("###################### dataset_bars.R")

# Dataset-level bar charts for categorical columns and yearly publication trends.
# Does NOT use cluster names — purely dataset-level stats.

source(file.path(getwd(), "pipelines", "charts", "chart_utils.R"))
library(dplyr)
library(tools)

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
dataset          <- dataset
document_label   <- toTitleCase(settings$params$dataset_source)
column_labels    <- settings$rp$column_labels
categorical_cols <- settings$rp$categorical_long_reports
available_cols   <- colnames(dataset)

dir.create(file.path(output_folder_level, subfolder_dataset), recursive = TRUE, showWarnings = FALSE)

# ---------------------------------------------------------------------------
# Utility: create a frequency table + bar chart for a multi-value column
# ---------------------------------------------------------------------------
create_report_and_barchart <- function(column_data,
                                       column_name,
                                       item_label = "Item",
                                       document_label = "Documents",
                                       top_items = 20) {
  stats_df <- column_data %>%
    as.character() %>%
    strsplit("; ") %>%
    unlist() %>%
    tolower() %>%
    toTitleCase() %>%
    gsub("Ieee", "IEEE", .) %>%
    gsub("International", "Int.", .) %>%
    gsub("Usa", "USA", .) %>%
    gsub("Peoples R China", "China", .) %>%
    substr(1, 45) %>%
    tibble(Item = .) %>%
    count(Item, name = "Documents") %>%
    arrange(desc(Documents))

  if (extension != "svg") {
    write.csv(stats_df,
              file = file.path(output_folder_level, subfolder_dataset,
                               glue("dataset_{column_name}.csv")),
              row.names = FALSE)
  }

  plot_rows <- stats_df %>% slice_head(n = top_items)

  ggplot(plot_rows, aes(x = Item, y = Documents)) +
    geom_bar(stat = "identity", width = 0.7, fill = "deepskyblue3") +
    coord_flip() +
    scale_x_discrete(name = item_label, limits = rev) +
    scale_y_continuous(name = document_label) +
    theme_chart()
  ggsave(file.path(output_folder_level, subfolder_dataset,
                   glue("fig_{gsub(' ', '_', column_name)}.{extension}")),
         width = 1000, height = 1000, units = "px")
}

# ---------------------------------------------------------------------------
# Bar charts for each categorical column
# ---------------------------------------------------------------------------
for (col in categorical_cols) {
  if (col %in% available_cols && !all(is.na(dataset[[col]]))) {
    print(col)
    create_report_and_barchart(dataset[[col]],
                               column_name = col,
                               item_label = column_labels[col])
  } else {
    print(glue("{col} is empty or missing. Skipped."))
  }
}

# ---------------------------------------------------------------------------
# Yearly publication trends
# ---------------------------------------------------------------------------
yearly_trends <- dataset$PY %>%
  as.numeric() %>%
  tibble(Year = .) %>%
  count(Year, name = "Documents") %>%
  arrange(Year)

if (extension != "svg") {
  write.csv(yearly_trends,
            file = file.path(output_folder_level, subfolder_dataset, "data_yearly_trends.csv"),
            row.names = FALSE)
}

yearly_plot <- yearly_trends %>% slice_tail(n = 10)

ggplot(yearly_plot, aes(x = factor(Year), y = Documents)) +
  geom_bar(stat = "identity", width = 0.7, fill = "deepskyblue3") +
  scale_x_discrete(name = "Year") +
  scale_y_continuous(name = "Documents") +
  theme_chart()
ggsave(file.path(output_folder_level, subfolder_dataset,
                 glue("fig_yearly_trends.{extension}")))
