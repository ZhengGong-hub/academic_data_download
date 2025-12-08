# Load packages
for (i in c("arrow", "dplyr", "tidyr", "lfe", "lubridate", "ggplot2", "rlang", "purrr", "broom", "sandwich", "lmtest", "stargazer")) {
  library(i, character.only = TRUE)
}

#setwd("/Users/andrekramer/Library/CloudStorage/OneDrive-UniversitaetSt.Gallen/Analysts Forecast Error/Linear vs. relative/Tables")
#setwd("/Users/andrekramer/Library/CloudStorage/OneDrive-UniversitaetSt.Gallen/BEAT Reinsurance/Code and analysis/Figures")

# Define function for regression tables over two pages
split_latex_table_custom <- function(table_tex, split_after_label,
                                     output_part1, output_part2,
                                     header_text = "Premiums ceded",
                                     first_col_width = "5cm",
                                     group_headers = NULL) {
  # Find split line
  split_idx <- grep(split_after_label, table_tex)[1]
  if (is.na(split_idx)) stop("Label for splitting not found")
  
  # Locate tabular block
  tabular_start <- grep("\\\\begin\\{tabular\\}", table_tex)[1]
  tabular_end   <- tail(grep("\\\\end\\{tabular\\}", table_tex), 1)
  if (is.na(tabular_start) || is.na(tabular_end)) {
    stop("Could not locate tabular environment.")
  }
  
  # Extract tabular line and fix first column width
  tabular_format_line <- table_tex[tabular_start]
  tabular_format_line_fixed <- sub(
    pattern = "(\\{)([lcr])",
    replacement = paste0("\\1p{", first_col_width, "}"),
    tabular_format_line
  )
  
  # Count number of data columns (excluding first p{...} column)
  # (Assumes remaining cols are l/c/r; if there are p{..} etc., adjust accordingly.)
  align_spec <- gsub(".*\\{(.*)\\}.*", "\\1", tabular_format_line)
  num_aligns <- nchar(gsub("[^lcr]", "", align_spec))
  num_cols <- num_aligns - 1
  if (num_cols <= 0) stop("Could not infer number of data columns > 0 from tabular spec.")
  
  # Build header rows
  if (!is.null(group_headers) && length(group_headers) > 0) {
    sizes  <- as.integer(unname(group_headers))
    labels <- names(group_headers)
    if (any(is.na(sizes)) || any(sizes <= 0)) stop("All group_headers values must be positive integers.")
    if (is.null(labels) || any(labels == "")) stop("group_headers must be a *named* numeric vector.")
    if (sum(sizes) != num_cols) {
      stop(sprintf("Sum of group_headers (%d) must equal number of data columns (%d).",
                   sum(sizes), num_cols))
    }
    
    # Build \multicolumn segments and corresponding \cline spans
    multicol_parts <- character(length(sizes))
    cline_parts    <- character(length(sizes))
    start_col <- 2
    for (i in seq_along(sizes)) {
      end_col <- start_col + sizes[i] - 1
      multicol_parts[i] <- paste0("\\multicolumn{", sizes[i], "}{c}{\\textbf{", labels[i], "}}")
      cline_parts[i]    <- paste0("\\cline{", start_col, "-", end_col, "}")
      start_col <- end_col + 1
    }
    
    header_block <- c(
      tabular_format_line_fixed,
      "\\\\[-1.8ex]\\hline",
      "\\hline \\\\[-1.8ex]",
      paste0(" & ", paste(multicol_parts, collapse = " & "), " \\\\"),
      paste0(cline_parts, collapse = " "),
      paste0("\\\\[-1.8ex] & ", paste0("(", 1:num_cols, ")", collapse = " & "), " \\\\"),
      "\\hline \\\\[-1.8ex]"
    )
  } else {
    # Single centered header across all data columns
    header_block <- c(
      tabular_format_line_fixed,
      "\\\\[-1.8ex]\\hline",
      "\\hline \\\\[-1.8ex]",
      paste0(" & \\multicolumn{", num_cols, "}{c}{\\textbf{", header_text, "}} \\\\"),
      paste0("\\cline{2-", num_cols + 1, "}"),
      paste0("\\\\[-1.8ex] & ", paste0("(", 1:num_cols, collapse = " & "), " \\\\"),
      "\\hline \\\\[-1.8ex]"
    )
  }
  
  # Extract data
  data_start <- tabular_start + 1
  part1_data <- table_tex[(data_start + length(header_block) - 1):(split_idx + 1)]
  
  if ((split_idx + 2) <= (tabular_end - 1)) {
    part2_data_raw <- table_tex[(split_idx + 2):(tabular_end - 1)]
  } else {
    part2_data_raw <- character(0)
  }
  
  # Remove Note: lines
  note_pattern <- "^\\s*(\\\\textit\\{)?Note:"
  part2_data <- part2_data_raw[!grepl(note_pattern, part2_data_raw)]
  
  # Part 1 output
  part1_full <- c(header_block, part1_data, "\\hline", "\\end{tabular}")
  
  # Continued block (centered bold line above second table)
  continued_block <- c(
    "\\begin{tabular}{@{}p{18cm}@{}}",
    "\\\\[-1.8ex]",
    "\\multicolumn{1}{c}{\\textbf{Table continued.}} \\\\",
    "\\\\[-1.8ex]",
    "\\end{tabular}"
  )
  
  # Part 2 output
  part2_full <- c(continued_block, header_block, part2_data, "\\end{tabular}")
  
  # Write output
  writeLines(part1_full, con = output_part1)
  writeLines(part2_full, con = output_part2)
  
  message("Split complete: ", output_part1, " and ", output_part2)
}

clean_for_summary <- function(df) {
  # Ensure data frame
  df <- as.data.frame(df)
  
  # Keep only numeric columns (stargazer summary + cor() will want this anyway)
  num_idx <- vapply(df, is.numeric, logical(1L))
  df_num  <- df[ , num_idx, drop = FALSE]
  
  # Replace Inf / -Inf / NaN with NA for all numeric columns
  df_num[] <- lapply(df_num, function(x) {
    x[!is.finite(x)] <- NA_real_
    x
  })
  
  # (Optional) drop rows that are completely NA across all numeric vars
  df_num <- df_num[rowSums(!is.na(df_num)) > 0, , drop = FALSE]
  
  return(df_num)
}



split_corr_to_latex <- function(corr_mat,
                                base_filename = "correlation_matrix_part",
                                max_cols = 10) {
  # corr_mat: square correlation matrix with row/col names already set
  # base_filename: prefix for output files (without .tex)
  # max_cols: maximum number of correlation columns per LaTeX table
  
  if (!requireNamespace("stargazer", quietly = TRUE)) {
    stop("Package 'stargazer' is required but not installed.")
  }
  
  # Internal helper: generate LaTeX tabular for a given submatrix
  make_corr_table_tex <- function(mat, file) {
    table_tex <- capture.output(
      stargazer::stargazer(
        mat,
        type    = "latex",
        digits  = 3,
        summary = FALSE,
        header  = FALSE,
        float   = TRUE,
        float.env = "table",
        table.placement = "!ht"
      )
    )
    
    # Find the tabular line
    tabular_line <- grep("\\\\begin\\{tabular\\}", table_tex)
    
    # Column spec: first column = variable names (left),
    # remaining columns = correlations (centered)
    n_corr_cols <- ncol(mat)
    col_spec <- paste0(
      "l",                       # first column: left-aligned names
      paste(rep("c", n_corr_cols), collapse = "")  # centered correlations
    )
    
    table_tex[tabular_line] <- sprintf(
      "\\begin{tabular}{@{\\extracolsep{5pt}}%s}",
      col_spec
    )
    
    # Extract only the tabular block
    start <- grep("\\\\begin\\{tabular\\}", table_tex)
    end   <- grep("\\\\end\\{tabular\\}", table_tex)
    
    tabular_only <- table_tex[start:end]
    
    # Write to file
    writeLines(tabular_only, file)
  }
  
  p <- ncol(corr_mat)
  if (p == 0L) stop("corr_mat has zero columns.")
  
  # Determine chunk boundaries
  starts <- seq(1L, p, by = max_cols)
  n_parts <- length(starts)
  
  out_files <- character(n_parts)
  
  for (k in seq_along(starts)) {
    from <- starts[k]
    to   <- min(from + max_cols - 1L, p)
    
    sub_mat <- corr_mat[, from:to, drop = FALSE]
    
    file_k <- sprintf("%s%d.tex", base_filename, k)
    make_corr_table_tex(sub_mat, file_k)
    
    out_files[k] <- file_k
  }
  
  invisible(out_files)
}



##############################
# Analysts
##############################

setwd("/home/ubuntu/academic_data_download/Analysis/Files")


# ---------------------   Summary statistics and correlation matrix  ---------------------------


# Variables for summary statistics
vars <- c("eps_beat", "eps_linear", "revision_pt", "revision_eps_1y", "eps_act", "eps_est", "prc_log", "marketcap_x_log", "vol_log", "f_roa", "f_pm", "f_ep", "f_ig", "f_dtm", "f_cfp", "f_btm", "cum_ret_252d_log", "cum_ret_126d_log", "cum_ret_22d_log", "cum_ret_1d_log")

# Label vector for summary statistics
var_labels <- c(
  eps_beat      = "Percent EPS surprise",
  eps_linear    = "Level EPS surprise",
  revision_pt   = "Price target revision",
  revision_eps_1y = "1-year EPS revision",
  eps_act       = "Actual EPS",
  eps_est       = "Estimated EPS",
  prc_log           = "log(Price)",
  marketcap_x_log   = "log(Market capitalization)",
  vol_log           = "log(Trading volume)",
  f_roa         = "Return on Assets",
  f_pm          = "Profit margin",
  f_ep          = "Earnings-to-price ratio",
  f_ig          = "Investment growth",
  f_dtm         = "Debt-to-market value",
  f_cfp         = "Cash-flow-to-price ratio",
  f_btm         = "Book-to-market ratio",
  cum_ret_252d_log = "log(1+lagged 12-month return)",
  cum_ret_126d_log = "log(1+lagged 6-month return)", 
  cum_ret_22d_log = "log(1+lagged 1-month return)", 
  cum_ret_1d_log = "log(1+lagged 1-day return)"
)

# Subset data
df_sum <- clean_for_summary(subset(df_regression_backup, select = vars))
df_sum <- as.data.frame(df_sum)

table_tex <- capture.output(
  stargazer(
    df_sum,
    type = "latex",
    digits = 3,
    covariate.labels = unname(var_labels[vars]),
    summary.stat = c("n","mean","median", "sd"),
    notes.align = "l",
    float = TRUE, float.env = "table", table.placement = "!ht",
    header = FALSE,    # keep your LaTeX preamble clean
    out = "summary_stats.tex"
  )
)

# Extract only the lines between \begin{tabular} and \end{tabular}
start <- grep("\\\\begin\\{tabular\\}", table_tex)
end   <- grep("\\\\end\\{tabular\\}", table_tex)

tabular_only <- table_tex[start:end]

# Write to new file
writeLines(tabular_only, "summary_stats.tex")


## Correlation Matrix
corr_mat <- cor(df_sum, use = "pairwise.complete.obs")

# Rename variables
colnames(corr_mat) <- unname(var_labels[colnames(corr_mat)])
rownames(corr_mat) <- unname(var_labels[rownames(corr_mat)])

# Only keep lower triangle
corr_mat[upper.tri(corr_mat)] <- NA

# Print correlation matrix with stargazer
table_tex <- capture.output(
  stargazer(
    corr_mat,
    type = "latex",
    digits = 3,
    summary = FALSE,        
    header = FALSE,
    float = TRUE,
    float.env = "table",
    table.placement = "!ht",
    label = "tab:corr",
    out = "correlation_matrix.tex")
)

# Make all columns left-aligned in the tabular environment
# 2. Find the tabular line
tabular_line <- grep("\\\\begin\\{tabular\\}", table_tex)
n_cols <- ncol(corr_mat) + 1
col_spec <- paste(rep("l", n_cols), collapse = "")

table_tex[tabular_line] <- sprintf(
  "\\begin{tabular}{@{\\extracolsep{5pt}}%s}", 
  col_spec
)

start <- grep("\\\\begin\\{tabular\\}", table_tex)
end   <- grep("\\\\end\\{tabular\\}", table_tex)

tabular_only <- table_tex[start:end]

# Save correlation matrix
writeLines(tabular_only, "correlation_matrix.tex")


# ---------------------   Regression: Price Target Revision  ---------------------------

table_tex <- capture.output(
  stargazer(pt1, pt2, pt3, pt4, pt5,
            digits = 3,
            type = "latex",
            keep = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_act$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                     "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            order = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_act$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                      "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            covariate.labels = c("EPSLinear", "EPSBeat", "log(Price)", "Reported EPS", "Estimated EPS", "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)",
                                 "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "no", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "no", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "no", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  latex_output <- append(latex_output, paste0(
    "& \\multicolumn{5}{c}{\\textbf{Target price revision (in \\%)}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_price_target.tex")


# ---------------------   Regression: Forward Return  ---------------------------

table_tex <- capture.output(
  stargazer(pt11, pt12, pt13, pt14, pt15,
            digits = 3,
            type = "latex",
            keep = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                     "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            order = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                      "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            covariate.labels = c("EPSLinear", "EPSBeat", "log(Price)", "Estimated EPS", "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)",
                                 "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  
  # Add column titles
  latex_output <- append(latex_output, paste0(
    " & \\multicolumn{1}{c}{\\textbf{Fwd 1 day}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 5 day}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 1 month}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 6 month}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 1 year}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_fwd_return.tex")


# ---------------------   Regression: Price Target Revision (different horizons)  ---------------------------

table_tex <- capture.output(
  stargazer(h1_5, h2_5, h3_5, h1_10, h2_10, h3_10, h1_120, h2_120, h3_120, 
            digits = 3,
            type = "latex",
            keep = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                     "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            order = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                      "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            covariate.labels = c("EPSLinear", "EPSBeat", "log(Price)", "Estimated EPS", "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)",
                                 "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes", "yes", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes", "yes", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes", "yes", "yes", "yes", "yes", "yes"))
  )
)



# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  
  # Add column titles
  latex_output <- append(latex_output, paste0(
    " & \\multicolumn{3}{c}{\\textbf{5 days}}",
    " & \\multicolumn{3}{c}{\\textbf{10 days}}",
    " & \\multicolumn{3}{c}{\\textbf{120 days}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_price_target_horizons.tex")

f_roa # Return on asset
f_pm # Profit margin
f_ep # earnings to price
f_ig # investment growth
f_dtm # debt to market cap
f_cfp # cashflow to price
f_btm # book to market


# ---------------------   Regression: Price Target Revision with financial statement variables  ---------------------------

table_tex <- capture.output(
  stargazer(pt2r, pt3r, pt4r, pt5r,
            digits = 3,
            type = "latex",
            keep = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_act$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                     "^cum_ret_1d_log$", "^vol_log$", "^marketcap_x_log$", "^f_roa$", "^f_btm$", "^f_pm$", "^f_ep$", "^f_ig$", "^f_dtm$", "^f_cfp$", "Constant"),
            order = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_act$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                      "^cum_ret_1d_log$", "^vol_log$", "^marketcap_x_log$", "^f_roa$", "^f_btm$", "^f_pm$", "^f_ep$", "^f_ig$", "^f_dtm$", "^f_cfp$", "Constant"),
            covariate.labels = c("EPSLinear", "EPSBeat", "log(Price)", "Reported EPS", "Estimated EPS", "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)",
                                 "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "Return on assets", "Book-to-market ratio", "Profit margin", "Earnings-to-price ratio",
                                 "Investment growth", "Debt-to-market value", "Cash-flow-to-price ratio", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  latex_output <- append(latex_output, paste0(
    "& \\multicolumn{4}{c}{\\textbf{Target price revision (in \\%)}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_price_target_financial_statement_var.tex")



# ---------------------   Regression: EPS Revision  ---------------------------

table_tex <- capture.output(
  stargazer(eps1, eps2, eps3, eps4, eps5,
            digits = 3,
            type = "latex",
            keep = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_act$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                     "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            order = c("^eps_linear$", "^eps_beat$", "^prc_log$", "^eps_act$", "^eps_est$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$",
                      "^cum_ret_1d_log$", "vol_log$", "^marketcap_x_log$", "Constant"),
            covariate.labels = c("EPSLinear", "EPSBeat", "log(Price)", "Reported EPS", "Estimated EPS", "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)",
                                 "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "no", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "no", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "no", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  latex_output <- append(latex_output, paste0(
    "& \\multicolumn{5}{c}{\\textbf{Earnings revision (in \\%)}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_earnings.tex")









##############################
# Retails
##############################


# ---------------------   Summary statistics and correlation matrix  ---------------------------


# Variables for summary statistics
vars <- c("estimate", "linear",
          "imbalance_in_-1d_100000_0", "imbalance_in_-1d_500_0", "imbalance_in_-1d_2000_500", "imbalance_in_-1d_10000_2000", "imbalance_in_-1d_30000_10000", "imbalance_in_-1d_100000_30000", 
          "prc_log", "marketcap_log", "vol_x_100000_0_log", "f_rp_ess", "f_rp_bmq", "f_rp_bee", "f_rp_bam", "f_rp_bca", "f_rp_css", "f_rp_ber", "f_rp_event_count",
          "f_roa", "f_pm", "f_ep", "f_ig", "f_dtm", "f_cfp", "f_btm", "cum_ret_252d_log", "cum_ret_126d_log", "cum_ret_22d_log", "cum_ret_1d_log")

# Label vector for summary statistics
var_labels <- c(
  estimate      = "Percent target price",
  linear    = "Level target price",
  
  `imbalance_in_-1d_100000_0` = "Lagged 1-day Imb 0-100000",
  `imbalance_in_-1d_500_0` = "Lagged 1-day Imb 0-500",
  `imbalance_in_-1d_2000_500` = "Lagged 1-day Imb 500-2000",
  `imbalance_in_-1d_10000_2000` = "Lagged 1-day Imb 2000-10000",
  `imbalance_in_-1d_30000_10000` = "Lagged 1-day Imb 10000-30000",
  `imbalance_in_-1d_100000_30000` = "Lagged 1-day Imb 30000-100000",
  
  prc_log           = "log(Price)",
  marketcap_log   = "log(Market capitalization)",
  vol_x_100000_0_log           = "log(Trading volume)",
  
  # Factors
  f_roa         = "Return on Assets",
  f_pm          = "Profit margin",
  f_ep          = "Earnings-to-price ratio",
  f_ig          = "Investment growth",
  f_dtm         = "Debt-to-market value",
  f_cfp         = "Cash-flow-to-price ratio",
  f_btm         = "Book-to-market ratio",
  
  # Sentiment measures
  f_rp_ess = "ESS",
  f_rp_bmq = "BMQ",
  f_rp_bee = "BEE",
  f_rp_bam = "BAM",
  f_rp_bca = "BCA",
  f_rp_css = "CSS",
  f_rp_ber = "BER",
  f_rp_event_count = "Number events",
  
  # Lagged returns
  cum_ret_252d_log = "log(1+lagged 12-month return)",
  cum_ret_126d_log = "log(1+lagged 6-month return)", 
  cum_ret_22d_log = "log(1+lagged 1-month return)", 
  cum_ret_1d_log = "log(1+lagged 1-day return)"
)

# Subset data
df_sum <- clean_for_summary(subset(df_summary, select = vars))
df_sum <- as.data.frame(df_sum)

table_tex <- capture.output(
  stargazer(
    df_sum,
    type = "latex",
    digits = 3,
    covariate.labels = unname(var_labels[vars]),
    summary.stat = c("n","mean","median", "sd"),
    notes.align = "l",
    float = TRUE, float.env = "table", table.placement = "!ht",
    header = FALSE    # keep your LaTeX preamble clean
  )
)

# Extract only the lines between \begin{tabular} and \end{tabular}
start <- grep("\\\\begin\\{tabular\\}", table_tex)
end   <- grep("\\\\end\\{tabular\\}", table_tex)

tabular_only <- table_tex[start:end]

# Write to new file
writeLines(tabular_only, "summary_stats_retails.tex")



## Correlation Matrix
corr_mat <- cor(df_sum, use = "pairwise.complete.obs")

# Rename variables
p <- ncol(corr_mat)
colnames(corr_mat) <- paste0("(", seq_len(p), ")")
rownames(corr_mat) <- paste0("(", seq_len(p), ") ", var_labels[rownames(corr_mat)])

#colnames(corr_mat) <- unname(var_labels[colnames(corr_mat)])
#rownames(corr_mat) <- unname(var_labels[rownames(corr_mat)])

# Only keep lower triangle
corr_mat[upper.tri(corr_mat)] <- NA

# (1) Either split correlation table
split_corr_to_latex(corr_mat,
                    base_filename = "correlation_matrix_retails_part",
                    max_cols = 10)

# (2) Not split correlation table
# Print correlation matrix with stargazer
table_tex <- capture.output(
  stargazer(
    corr_mat,
    type = "latex",
    digits = 3,
    summary = FALSE,        
    header = FALSE,
    float = TRUE,
    float.env = "table",
    table.placement = "!ht",
    label = "tab:corr")
)

# Make all columns left-aligned in the tabular environment
# 2. Find the tabular line
tabular_line <- grep("\\\\begin\\{tabular\\}", table_tex)
n_cols <- ncol(corr_mat) + 1
col_spec <- paste(rep("l", n_cols), collapse = "")

table_tex[tabular_line] <- sprintf(
  "\\begin{tabular}{@{\\extracolsep{5pt}}%s}", 
  col_spec
)

start <- grep("\\\\begin\\{tabular\\}", table_tex)
end   <- grep("\\\\end\\{tabular\\}", table_tex)

tabular_only <- table_tex[start:end]

# Save correlation matrix
writeLines(tabular_only, "correlation_matrix_retails.tex")











# ---------------------   Regression: Imbalance  ---------------------------

table_tex <- capture.output(
  stargazer(im1, im2, im3, im4, sen1,
            digits = 3,
            type = "latex",
            keep = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$", "^cum_ret_1d_log$",
                     "^vol_x_100000_0_log$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            order = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$", "^cum_ret_1d_log$",
                      "^vol_x_100000_0_log$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            covariate.labels = c("Level target price", "Percent target price", "log(Price)", "Lagged 1-day Imb 0-100000", "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)",
                                 "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "ESS", "BMQ", "BEE", "BAM", "BCA", "CSS", "BER", "Number events", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "no", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "no", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "no", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  latex_output <- append(latex_output, paste0(
    "& \\multicolumn{5}{c}{1-day Imb 0-100000 (\\%)} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_imbalance.tex")

# ---------------------   Regression: Imbalance by buckets  ---------------------------

table_tex <- capture.output(
  stargazer(buc_500_1, buc_2000_1, buc_10000_1, buc_30000_1, buc_100000_1,
            digits = 3,
            type = "latex",
            keep = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_500_0`$", "^`imbalance_in_-1d_2000_500`$", "^`imbalance_in_-1d_10000_2000`$", "^`imbalance_in_-1d_30000_10000`$", "^`imbalance_in_-1d_100000_30000`$",
                     "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$", "^cum_ret_1d_log$", "^vol_x_100000_0_log$", "marketcap_log$", "Constant"),
            order = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_500_0`$", "^`imbalance_in_-1d_2000_500`$", "^`imbalance_in_-1d_10000_2000`$", "^`imbalance_in_-1d_30000_10000`$", "^`imbalance_in_-1d_100000_30000`$",
                      "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$", "^cum_ret_1d_log$", "^vol_x_100000_0_log$", "marketcap_log$", "Constant"),
            covariate.labels = c("Level target price", "Percent target price", "log(Price)", "Lagged 1-day Imb 0-500", "Lagged 1-day Imb 500-2000", "Lagged 1-day Imb 2000-10000", "Lagged 1-day Imb 10000-30000", "Lagged 1-day Imb 30000-100000",
                                 "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)", "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  
  # Add column titles
  latex_output <- append(latex_output, paste0(
    " & \\multicolumn{1}{c}{1-day Imb 0-500 (\\%)}",
    " & \\multicolumn{1}{c}{1-day Imb 500-2000 (\\%)}",
    " & \\multicolumn{1}{c}{1-day Imb 2000-10000 (\\%)}",
    " & \\multicolumn{1}{c}{1-day Imb 10000-30000 (\\%)}",
    " & \\multicolumn{1}{c}{1-day Imb 30000-100000 (\\%)} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_imbalance_buckets.tex")





# ---------------------   Regression: Retail returns  ---------------------------

table_tex <- capture.output(
  stargazer(ret1, ret2, ret3, ret4, ret5,
            digits = 3,
            type = "latex",
            keep = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$", "^cum_ret_1d_log$",
                     "^vol_x_100000_0_log$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            order = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^cum_ret_252d_log$", "^cum_ret_126d_log$", "^cum_ret_22d_log$", "^cum_ret_1d_log$",
                      "^vol_x_100000_0_log$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            covariate.labels = c("Level target price", "Percent target price", "log(Price)", "Lagged 1-day Imb 0-100000", "log(1+lagged 12-month return)", "log(1+lagged 6-month return)", "log(1+lagged 1-month return)",
                                 "log(1+lagged 1-day return)", "log(Volume)", "log(Market Capitalization)", "ESS", "BMQ", "BEE", "BAM", "BCA", "CSS", "BER", "Number events", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  
  # Add column titles
  latex_output <- append(latex_output, paste0(
    " & \\multicolumn{1}{c}{\\textbf{Fwd 1 day}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 5 day}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 1 month}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 6 month}}",
    " & \\multicolumn{1}{c}{\\textbf{Fwd 1 year}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]
getwd()

# Write to .tex file
writeLines(latex_output, "regression_return_retails.tex")


# ---------------------   Regression: CAR FF1  ---------------------------

table_tex <- capture.output(
  stargazer(ff1_1, ff1_2, ff1_3, ff1_4, ff1_5,
            digits = 3,
            type = "latex",
            keep = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^car_252d_ff1_log$", "^car_126d_ff1_log$", "^car_22d_ff1_log$", "^car_1d_ff1_log$",
                     "^ `vol_in_-1d_log`$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            order = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^car_252d_ff1_log$", "^car_126d_ff1_log$", "^car_22d_ff1_log$", "^car_1d_ff1_log$",
                      "^ `vol_in_-1d_log`$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            covariate.labels = c("Level target price", "Percent target price", "log(Price)", "Lagged 1-day Imb 0-100000", "log(1+lagged 12-month CAR)", "log(1+lagged 6-month CAR)", "log(1+lagged 1-month CAR)",
                                 "log(1+lagged 1-day CAR)", "log(Volume)", "log(Market Capitalization)", "ESS", "BMQ", "BEE", "BAM", "BCA", "CSS", "BER", "Number events", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  
  # Add column titles
  latex_output <- append(latex_output, paste0(
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 1$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 2$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 3$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 4$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 5$}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_return_retails_car_ff1.tex")


# ---------------------   Regression: CAR FF3  ---------------------------


table_tex <- capture.output(
  stargazer(ff3_1, ff3_2, ff3_3, ff3_4, ff3_5,
            digits = 3,
            type = "latex",
            keep = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^car_252d_ff3_log$", "^car_126d_ff3_log$", "^car_22d_ff3_log$", "^car_1d_ff3_log$",
                     "^ `vol_in_-1d_log`$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            order = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^car_252d_ff3_log$", "^car_126d_ff3_log$", "^car_22d_ff3_log$", "^car_1d_ff3_log$",
                      "^ `vol_in_-1d_log`$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            covariate.labels = c("Level target price", "Percent target price", "log(Price)", "Lagged 1-day Imb 0-100000", "log(1+lagged 12-month CAR)", "log(1+lagged 6-month CAR)", "log(1+lagged 1-month CAR)",
                                 "log(1+lagged 1-day CAR)", "log(Volume)", "log(Market Capitalization)", "ESS", "BMQ", "BEE", "BAM", "BCA", "CSS", "BER", "Number events", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  
  # Add column titles
  latex_output <- append(latex_output, paste0(
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 1$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 2$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 3$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 4$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 5$}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_return_retails_car_ff3.tex")


# ---------------------   Regression: CAR FF6  ---------------------------


table_tex <- capture.output(
  stargazer(ff6_1, ff6_2, ff6_3, ff6_4, ff6_5,
            digits = 3,
            type = "latex",
            keep = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^car_252d_ff6_log$", "^car_126d_ff6_log$", "^car_22d_ff6_log$", "^car_1d_ff6_log$",
                     "^ `vol_in_-1d_log`$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            order = c("^linear$", "^estimate$", "^prc_log$", "^`imbalance_in_-1d_100000_0`$", "^car_252d_ff6_log$", "^car_126d_ff6_log$", "^car_22d_ff6_log$", "^car_1d_ff6_log$",
                      "^ `vol_in_-1d_log`$", "marketcap_log$", "^f_rp_ess$", "^f_rp_bmq$", "^f_rp_bee$", "^f_rp_bam$", "^f_rp_bca$", "^f_rp_css$", "^f_rp_ber$", "f_rp_event_count", "Constant"),
            covariate.labels = c("Level target price", "Percent target price", "log(Price)", "Lagged 1-day Imb 0-100000", "log(1+lagged 12-month CAR)", "log(1+lagged 6-month CAR)", "log(1+lagged 1-month CAR)",
                                 "log(1+lagged 1-day CAR)", "log(Volume)", "log(Market Capitalization)", "ESS", "BMQ", "BEE", "BAM", "BCA", "CSS", "BER", "Number events", "Constant"),
            dep.var.labels.include = FALSE,
            float = FALSE,
            #dep.var.caption = NULL,
            star.cutoffs = c(.1, .05, .01),
            no.space = TRUE,
            omit.stat = c("rsq", "ser", "f"),
            add.lines = list(
              c("Stock fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Day fixed effects", "yes", "yes", "yes", "yes", "yes"),
              c("Analyst fixed effects", "yes", "yes", "yes", "yes", "yes"))
  )
)

# Remove or replace the line containing "Dependent variable:"
latex_output <- table_tex[!grepl("Dependent variable:", table_tex)]

# Remove lines that contain model type labels like "Linear regression"
latex_output <- latex_output[!grepl("Linear regression|felm", table_tex)]

# Add label row above the column indices
idx <- grep("\\(1\\)", latex_output)

# Only insert if a match was found
if (length(idx) > 0) {
  pos <- ifelse(idx[1] > 2, idx[1] - 2, 0)
  
  # Add column titles
  latex_output <- append(latex_output, paste0(
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 1$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 2$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 3$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 4$}}",
    " & \\multicolumn{1}{c}{\\textbf{$\\Delta t = 5$}} \\\\"
  ), after = pos)
}

# Remove any NA rows
latex_output <- latex_output[!is.na(latex_output)]

# Write to .tex file
writeLines(latex_output, "regression_return_retails_car_ff6.tex")
getwd()

