# Load packages
for (i in c("arrow", "dplyr", "tidyr", "lfe", "lubridate", "ggplot2", "rlang", "purrr", "broom", "sandwich", "lmtest")) {
  library(i, character.only = TRUE)
}


##############################
# Define functions
##############################

analyze_felm_correlations <- function(felm_spec, data,
                                      scale_numeric = TRUE,
                                      center_for_interactions = TRUE,
                                      top_k = 25,
                                      drop_intercept = TRUE) {
  # --- 1) Extract the "y ~ x" part from felm's multi-part formula -------------
  get_main_formula <- function(spec) {
    if (inherits(spec, "felm")) {
      # extract original formula string
      f <- stats::formula(spec)
      # felm's formula is a "felm formula" object; coerce to character safely
      fs <- deparse(f, width.cutoff = 500)
      fs <- paste(fs, collapse = " ")
    } else if (inherits(spec, "formula")) {
      fs <- deparse(spec, width.cutoff = 500); fs <- paste(fs, collapse = " ")
    } else if (is.character(spec)) {
      fs <- spec
    } else {
      stop("felm_spec must be a felm object, a formula, or a character formula.")
    }
    # split at '|' and keep the first block 'y ~ x'
    parts <- strsplit(fs, "\\|", fixed = FALSE)[[1]]
    yx <- trimws(parts[1])
    stats::as.formula(yx)
  }
  
  main_form <- get_main_formula(felm_spec)
  
  # --- 2) Build model matrix X with interactions exactly as in the formula ----
  #    We only need RHS; keep factors as dummies (model.matrix default)
  rhs_form <- stats::reformulate(attr(stats::terms(main_form), "term.labels"))
  X <- stats::model.matrix(rhs_form, data = data, na.action = na.omit)
  
  if (drop_intercept && "(Intercept)" %in% colnames(X)) {
    X <- X[, colnames(X) != "(Intercept)", drop = FALSE]
  }
  
  # --- 3) Optional centering/scaling for numeric columns ----------------------
  # Centering reduces collinearity between main effects and their interactions.
  if (center_for_interactions || scale_numeric) {
    is_binary01 <- function(v) {
      u <- unique(na.omit(v))
      length(u) <= 2 && all(sort(u) %in% c(0,1))
    }
    for (j in seq_len(ncol(X))) {
      v <- X[, j]
      if (is.numeric(v) && !is_binary01(v)) {
        if (center_for_interactions) v <- v - mean(v, na.rm = TRUE)
        if (scale_numeric) {
          sdv <- stats::sd(v, na.rm = TRUE)
          if (is.finite(sdv) && sdv > 0) v <- v / sdv
        }
        X[, j] <- v
      }
    }
  }
  
  # If any columns are constant (can happen after dummies drop), drop them
  keep <- apply(X, 2, function(v) sd(v, na.rm = TRUE) > 0)
  X <- X[, keep, drop = FALSE]
  
  # --- 4) Correlations (pairwise complete obs) --------------------------------
  cor_mat <- stats::cor(X, use = "pairwise.complete.obs")
  # top |r| pairs
  get_top_pairs <- function(C, k) {
    p <- ncol(C)
    out <- data.frame(var1 = character(), var2 = character(),
                      r = numeric(), abs_r = numeric(),
                      stringsAsFactors = FALSE)
    if (p < 2) return(out)
    for (i in 1:(p-1)) {
      for (j in (i+1):p) {
        r <- C[i, j]
        out <- rbind(out, data.frame(
          var1 = colnames(C)[i],
          var2 = colnames(C)[j],
          r = r,
          abs_r = abs(r),
          stringsAsFactors = FALSE
        ))
      }
    }
    out[order(-out$abs_r), ][seq_len(min(k, nrow(out))), ]
  }
  top_corr <- get_top_pairs(cor_mat, top_k)
  
  # --- 5) VIFs (no packages): regress each column on the rest -----------------
  # VIF_j = 1 / (1 - R^2_j)
  vif <- rep(NA_real_, ncol(X)); names(vif) <- colnames(X)
  for (j in seq_len(ncol(X))) {
    yj <- X[, j]
    Xj <- X[, -j, drop = FALSE]
    # add small ridge if necessary to avoid singularities in QR
    df_j <- data.frame(y = yj, Xj)
    fit <- tryCatch(stats::lm(y ~ . , data = df_j),
                    error = function(e) NULL)
    if (!is.null(fit)) {
      r2 <- max(0, min(1, summary(fit)$r.squared))
      vif[j] <- 1 / (1 - r2 + 1e-12)
    }
  }
  
  # --- 6) Condition indices (Belsley) -----------------------------------------
  # Use eigenvalues of the correlation matrix
  eig <- eigen(cor_mat, symmetric = TRUE, only.values = TRUE)$values
  cond_index <- sqrt(max(eig, na.rm = TRUE) / pmax(eig, 1e-12))
  names(cond_index) <- paste0("Dim", seq_along(cond_index))
  
  # --- 7) Console summary ------------------------------------------------------
  cat("\n=== Design diagnostics (incl. interactions) ===\n")
  cat("Columns in X:", ncol(X), "\n")
  cat("Highest |correlations| (top", min(top_k, ncol(X)*(ncol(X)-1)/2), "pairs):\n")
  print(utils::head(top_corr, 10), row.names = FALSE)
  cat("\nVIF summary:\n")
  print(summary(vif[is.finite(vif)]))
  cat("\nCondition index (max):", round(max(cond_index, na.rm = TRUE), 2), "\n")
  if (max(cond_index, na.rm = TRUE) > 30) {
    cat("  > Warning: condition index > 30 suggests serious multicollinearity.\n")
  } else if (max(cond_index, na.rm = TRUE) > 15) {
    cat("  > Note: condition index > 15 suggests moderate multicollinearity.\n")
  }
  
  invisible(list(
    X = X,
    cor = cor_mat,
    top_cor = top_corr,
    vif = vif,
    condition_index = cond_index
  ))
}

winsorize_vec <- function(x, probs = c(0.01, 0.99)) {
  qs <- quantile(x, probs = probs, na.rm = TRUE, names = FALSE, type = 7)
  x <- pmin(pmax(x, qs[1]), qs[2])
  x
}

log_vars <- function(data, vars, add_one = FALSE, suffix = "_log") {
  vars_in_data <- intersect(vars, names(data))
  if (length(vars_in_data) == 0) {
    warning("None of the specified variables are in the data.")
    return(data)
  }
  
  for (v in vars_in_data) {
    new_name <- paste0(v, suffix)
    
    if (add_one) {
      data[[new_name]] <- log(1 + data[[v]])
    } else {
      data[[new_name]] <- log(data[[v]])
    }
  }
  
  data
}


standardise_vars <- function(data, vars) {
  
  vars_in_data <- intersect(vars, names(data))
  if (length(vars_in_data) == 0) {
    warning("None of the specified variables are in the data.")
    return(data)
  }
  
  for (v in vars_in_data) {
    data[[v]] <- as.numeric(scale(data[[v]]))
  }
  
  data
}

plot_bucket_series <- function(data,
                               prefix      = "signedvol",
                               suffix      = "_agg",
                               bucket_col  = "bucket",
                               buckets     = 1:5,
                               cols        = c("black","green","blue","grey","red"),
                               pch         = 16,
                               cex         = 0.9,
                               type        = "b",   # "p","b","l","o"
                               main        = NULL,
                               ylab        = NULL,
                               ylim        = NULL,
                               add_legend  = TRUE) {
  stopifnot(length(buckets) == length(cols))
  
  # 1) Find matching columns and extract horizons
  pattern <- paste0("^", prefix, "_in_-?\\d+d", suffix, "$")
  present <- grep(pattern, names(data), value = TRUE)
  if (length(present) == 0)
    stop("No matching columns found for prefix '", prefix, "' and suffix '", suffix, "'.")
  
  # Extract numeric horizons (e.g., "-66", "22", "252") and sort
  get_h <- function(nm) {
    as.integer(sub(paste0("^", prefix, "_in_(-?\\d+)d", suffix, "$"), "\\1", nm))
  }
  h <- vapply(present, get_h, FUN.VALUE = integer(1))
  ord <- order(h)
  present <- present[ord]
  h <- h[ord]
  
  x <- seq_along(present)
  
  # 2) Helper to get numeric vector for a bucket row
  get_y <- function(b) {
    rowsel <- data[[bucket_col]] == b
    if (!any(rowsel)) return(rep(NA_real_, length(present)))
    as.numeric(as.matrix(data[rowsel, present, drop = FALSE]))
  }
  
  # 3) Y limits across all requested buckets if not provided
  if (is.null(ylim)) {
    mat_vals <- vapply(buckets, get_y, FUN.VALUE = numeric(length(present)))
    ylim <- range(mat_vals, na.rm = TRUE)
    if (!is.finite(ylim[1]) || !is.finite(ylim[2])) ylim <- c(-0.01, 0.01)
  }
  
  # 4) Plot first bucket
  y1 <- get_y(buckets[1])
  plot(x, y1,
       xaxt = "n", ylim = ylim, pch = pch, cex = cex, type = type,
       xlab = "", ylab = if (is.null(ylab)) prefix else ylab,
       main = if (is.null(main)) paste0("Bucket series: ", prefix) else main,
       col = cols[1])
  axis(1, at = x, labels = h, las = 1)
  
  # 5) Add remaining buckets
  if (length(buckets) > 1) {
    for (i in 2:length(buckets)) {
      yi <- get_y(buckets[i])
      if (type %in% c("b","o")) {
        points(x, yi, col = cols[i], pch = pch, cex = cex)
        lines(x, yi, col = cols[i])
      } else if (type == "l") {
        lines(x, yi, col = cols[i])
      } else {
        points(x, yi, col = cols[i], pch = pch, cex = cex)
      }
    }
  }
  
  if (add_legend) {
    legend("topleft", inset = 0.02, title = "Bucket",
           legend = buckets, col = cols, pch = pch, cex = 0.9, bty = "n")
  }
  
  invisible(list(columns_used = present, horizons = h, x_index = x))
}


imbalance_manual <- function(ticker, date, data){
  date = as.Date(date)
  sb = data$sb_in_0d[data$sym_root == ticker & data$ann_deemed_date == date]
  ss = data$ss_in_0d[data$sym_root == ticker & data$ann_deemed_date == date]
  print("Manually calculated")
  print((sb-ss)/(sb+ss))
  print(data$imbalance_in_0d[data$sym_root == ticker & data$ann_deemed_date == date])
  return((sb-ss)/(sb+ss) == data$imbalance_in_0d[data$sym_root == ticker & data$ann_deemed_date == date])
}


##############################
# Load data
##############################


# Load with trade size buckets
df <- read_parquet("/home/ubuntu/academic_data_download/Analysis/stock_split_win.parquet")


# Set print to max
options(max.print = .Machine$integer.max)
#options(width = 10000)


##############################
# Manipulate data frame
##############################


# Sanity check
sum(df$sb_in_0d_100000_0 - (df$sb_in_0d_500_0 + df$sb_in_0d_2000_500 + df$sb_in_0d_10000_2000 + df$sb_in_0d_30000_10000 + df$sb_in_0d_100000_30000), na.rm = TRUE) # 0

colnames(df)
# Filter data frame and calculate variables. Prc is the closing price on day ann.
df <- df %>% filter(prc >= 5) %>% 
  mutate(
    # Change date format
    ann_deemed_date = as.Date(ann_deemed_date, format = "%Y-%m-%d"), date_stock_split = as.Date(date_stock_split, format = "%Y-%m-%d"), last_earnings_deemed_date = as.Date(last_earnings_deemed_date, format = "%Y-%m-%d"), next_earnings_deemed_date = as.Date(next_earnings_deemed_date, format = "%Y-%m-%d"),
    after_earning = ann_deemed_date - last_earnings_deemed_date, after_stock_split = as.numeric(ann_deemed_date - date_stock_split), Post = ifelse(after_stock_split > 1, 1, 0),
    prc_yest = prc/(1+ret), estimate = (pt - prc_yest) / prc_yest, linear = pt - prc_yest, linear_abs = abs(pt - prc_yest), direction = as.integer(pt > prc_yest),
    Year = as.integer(format(ann_deemed_date, "%Y")), Month = as.integer(gsub("-", "",format(ann_deemed_date, "%Y-%m"))),
    
    real_stock_split = ifelse(stock_split > 1,1,0), bucket_split =  ifelse(stock_split >= 2,1,0),
    
    # Shift past returns
    cum_ret_252d = (cum_ret_252d+1)*prc_yest / prc - 1, cum_ret_126d = (cum_ret_126d+1)*prc_yest / prc - 1, cum_ret_22d = (cum_ret_22d+1)*prc_yest / prc - 1, cum_ret_1d = (cum_ret_2d+1)*prc_yest / prc - 1
    

  ) %>% filter(after_stock_split != 0, real_stock_split==1) # Price target not on the same day as stock split






#df %>% group_by(stock_split) %>% summarize(n=n()) %>% distinct


# Calculate imabalance measures for each trade size bucket
all_sb <- grep("^sb(_in_-?\\d+d)?_\\d+_\\d+$", names(df), value = TRUE)
if (length(all_sb) == 0) stop("No sb_* columns found.")

get_bucket <- function(x) sub(".*_(\\d+_\\d+)$", "\\1", x)
buckets <- sort(unique(get_bucket(all_sb)))

# For a given bucket, list all sb columns (plain and _in_±kd)
sb_cols_for_bucket <- function(bucket) {
  # matches sb_<bucket> and sb_in_-5d_<bucket>, etc.
  pattern <- paste0("^sb(?:_in_-?\\d+d)?_", bucket, "$")
  grep(pattern, names(df), value = TRUE)
}

# For each bucket, compute (sb-ss)/(sb+ss) and (sb-ss)/vol with aligned counterparts
for (bucket in buckets) {
  sb_cols <- sb_cols_for_bucket(bucket)
  if (length(sb_cols) == 0) next
  
  for (sb in sb_cols) {
    # Matching ss column
    ss <- sub("^sb", "ss", sb)
    if (!ss %in% names(df)) stop("Missing matching ss column for: ", sb)
    
    # Decide the matching vol column:
    # If this is an 'in' horizon (sb_in_..._bucket), use vol_in_<same horizon>_<bucket>
    # else (plain sb_<bucket>) prefer vol_in_0d_<bucket>, else vol_x_<bucket>
    
    # Decide the matching vol column:
    if (grepl("^sb_in_-?\\d+d_", sb)) {
      # Extract the horizon, e.g. -5d, 0d, 66d, 252d
      horizon <- sub("^sb_in_(-?\\d+d)_.*$", "\\1", sb)
      vol <- paste0("vol_in_", horizon)  # e.g. "vol_in_-5d"
      
      if (!vol %in% names(df)) stop("Missing matching vol column for: ", sb)
    } else {
      # plain sb_<bucket>: use overall vol_in_0d (no bucket)
      vol0 <- "vol_in_0d"
      volx <- "vol_x"
      vol  <- if (vol0 %in% names(df)) vol0 else if (volx %in% names(df)) volx else NA_character_
      if (is.na(vol)) stop("No vol_in_0d or vol_x found (needed for ", sb, ")")
    }
    
    
    # Output names mirror the sb name, just with prefixes changed
    imb <- sub("^sb", "imbalance", sb)  # (sb - ss) / (sb + ss)
    sgv <- sub("^sb", "signedvol", sb)  # (sb - ss) / vol
    
    # Compute safely
    num <- df[[sb]] - df[[ss]]
    
    den <- df[[sb]] + df[[ss]]
    den[den == 0] <- NA_real_
    df[[imb]] <- num / den
    
    den2 <- df[[vol]]
    den2[den2 == 0] <- NA_real_
    df[[sgv]] <- num / den2
  }
}


# Example: Imbalance measure for AAPL
df %>% filter(ticker=="AAPL", ann_deemed_date == as.Date("2024-04-12")) %>% select(imbalance_in_0d_500_0, imbalance_in_0d_2000_500, imbalance_in_0d_10000_2000, imbalance_in_0d_30000_10000, imbalance_in_0d_100000_30000, imbalance_in_0d_100000_0)

# Example: Buys for AAPL
df %>% filter(ticker=="AAPL", ann_deemed_date == as.Date("2024-04-12")) %>% select(sb_in_0d_500_0, sb_in_0d_2000_500, sb_in_0d_10000_2000, sb_in_0d_30000_10000, sb_in_0d_100000_30000, sb_in_0d_100000_0) %>%
  mutate(Sum = sum(sb_in_0d_500_0, sb_in_0d_2000_500, sb_in_0d_10000_2000, sb_in_0d_30000_10000, sb_in_0d_100000_30000))

# Example: Buckets are added correctly
df %>% filter(ticker=="AAPL", ann_deemed_date == as.Date("2024-04-12")) %>% select(ss_in_0d_500_0, ss_in_0d_2000_500, ss_in_0d_10000_2000, ss_in_0d_30000_10000, ss_in_0d_100000_30000, ss_in_0d_100000_0) %>% 
  mutate(Sum = sum(ss_in_0d_500_0, ss_in_0d_2000_500, ss_in_0d_10000_2000, ss_in_0d_30000_10000, ss_in_0d_100000_30000))


# Get some statistics
nrow(df) # 22,221
summary(df$direction)["Mean"] # mean: 0.8390711

colnames(df)


##############################
# Regressions: Statistics
##############################


# For summary statistics
df_summary <- df
vars_ret   <- c("cum_ret_252d", "cum_ret_126d", "cum_ret_22d", "cum_ret_1d", "fwd_ret_1d", "fwd_ret_5d", "fwd_ret_22d", "fwd_ret_126d", "fwd_ret_252d")
vars_level <- c("vol_x_100000_0", "marketcap", "prc")

df_summary <- log_vars(df_summary, vars_ret, add_one = TRUE, suffix = "_log")
df_summary <- log_vars(df_summary, vars_level, add_one = FALSE, suffix = "_log")

# Handle infinite values
for (v in intersect(paste0(union(vars_ret, vars_level), "_log"), names(df_summary))) {
  bad <- !is.finite(df_summary[[v]])  # Inf, -Inf, NaN
  df_summary[[v]][bad] <- NA
}

# Concentration of target price releases
df_summary %>% group_by(after_earning_ann) %>% summarize(Number_price_targets = n(), Percent = 100*Number_price_targets / nrow(df_summary))



##############################
# Regressions: Price Target
##############################

# 5 days after earnings announcement day
days_after_earnings = 3
df_regression <- df %>% filter(after_earning > days_after_earnings) %>% mutate(Post = ifelse(ann_deemed_date >= as.Date("2018-01-01"),1,0), bucket = dplyr::ntile(linear, 5), 
                                                                               
                                                                               # Imbalance measure
                                                                               imbalance_in_1d_100000_0 = 100*imbalance_in_1d_100000_0, imbalance_in_1d_500_0=100*imbalance_in_1d_500_0, imbalance_in_1d_2000_500=100*imbalance_in_1d_2000_500, imbalance_in_1d_10000_2000=100*imbalance_in_1d_10000_2000, imbalance_in_1d_100000_30000=100*imbalance_in_1d_100000_30000, imbalance_in_1d_30000_10000=100*imbalance_in_1d_30000_10000,
                                                                               imbalance_in_2d_100000_0=100*imbalance_in_2d_100000_0, imbalance_in_5d_100000_0=100*imbalance_in_5d_100000_0, imbalance_in_66d_100000_0=100*imbalance_in_66d_100000_0, imbalance_in_132d_100000_0=100*imbalance_in_132d_100000_0, imbalance_in_252d_100000_0=100*imbalance_in_252d_100000_0,
                                                                               
                                                                               # Signedvol measure
                                                                               signedvol_in_1d_100000_0 = 100*signedvol_in_1d_100000_0, signedvol_in_1d_500_0=100*signedvol_in_1d_500_0, signedvol_in_1d_2000_500=100*signedvol_in_1d_2000_500, signedvol_in_1d_10000_2000=100*signedvol_in_1d_10000_2000, signedvol_in_1d_100000_30000=100*signedvol_in_1d_100000_30000, signedvol_in_1d_30000_10000=100*signedvol_in_1d_30000_10000,
                                                                               signedvol_in_2d_100000_0=100*signedvol_in_2d_100000_0, signedvol_in_5d_100000_0=100*signedvol_in_5d_100000_0, signedvol_in_66d_100000_0=100*signedvol_in_66d_100000_0, signedvol_in_132d_100000_0=100*signedvol_in_132d_100000_0, signedvol_in_252d_100000_0=100*signedvol_in_252d_100000_0)

df_regression$bucket <- factor(df_regression$bucket, levels = 1:5); df_regression$bucket <- relevel(df_regression$bucket, ref = "3")


# Transform variables
vars_ret   <- c("cum_ret_252d", "cum_ret_126d", "cum_ret_22d", "cum_ret_1d", "fwd_ret_1d", "fwd_ret_2d", "fwd_ret_3d", "fwd_ret_4d", "fwd_ret_5d", "fwd_ret_22d", "fwd_ret_126d", "fwd_ret_252d")
vars_level <- c("vol_in_-1d", "marketcap", "prc", "prc_yest")

df_regression <- log_vars(df_regression, vars_ret, add_one = TRUE, suffix = "_log")
df_regression <- log_vars(df_regression, vars_level, add_one = FALSE, suffix = "_log")

# Handle infinite values
for (v in intersect(paste0(union(vars_ret, vars_level), "_log"), names(df_regression))) {
  bad <- !is.finite(df_regression[[v]])  # Inf, -Inf, NaN
  df_regression[[v]][bad] <- NA
}

# Create backup
df_regression_backup <- df_regression

# Winsorization
cols_to_winsor <- union(paste0(union(vars_ret, vars_level), "_log"), c(
  
  # Imbalance measure
  "imbalance_in_1d_500_0", "imbalance_in_0d_500_0", "imbalance_in_-1d_500_0", "imbalance_in_1d_2000_500", "imbalance_in_0d_2000_500", "imbalance_in_-1d_2000_500", "imbalance_in_1d_10000_2000", "imbalance_in_0d_10000_2000", "imbalance_in_-1d_10000_2000", "imbalance_in_1d_100000_30000", "imbalance_in_0d_100000_30000", "imbalance_in_-1d_100000_30000",
  "imbalance_in_1d_30000_10000", "imbalance_in_0d_30000_10000", "imbalance_in_-1d_30000_10000", "imbalance_in_1d_100000_0", "imbalance_in_0d_100000_0", "imbalance_in_-1d_100000_0", 
  
  # Signedvol measure
  "signedvol_in_1d_500_0", "signedvol_in_0d_500_0", "signedvol_in_-1d_500_0", "signedvol_in_1d_2000_500", "signedvol_in_0d_2000_500", "signedvol_in_-1d_2000_500", "signedvol_in_1d_10000_2000", "signedvol_in_0d_10000_2000", "signedvol_in_-1d_10000_2000", "signedvol_in_1d_100000_30000", "signedvol_in_0d_100000_30000", "signedvol_in_-1d_100000_30000",
  "signedvol_in_1d_30000_10000", "signedvol_in_0d_30000_10000", "signedvol_in_-1d_30000_10000", "signedvol_in_1d_100000_0", "signedvol_in_0d_100000_0", "signedvol_in_-1d_100000_0", 
  
  # Imbalance: Different horizons
  "imbalance_in_2d_100000_0", "imbalance_in_5d_100000_0", "imbalance_in_66d_100000_0", "imbalance_in_132d_100000_0", "imbalance_in_252d_100000_0",
  
  # Other dependent variables
  "estimate", "linear", "f_roa", "f_btm", "f_pm", "f_ep", "f_ig", "f_dtm", "f_cfp",
  
  # Sentiment variables
  "f_rp_ess", "f_rp_bmq", "f_rp_bee", "f_rp_bam", "f_rp_bca", "f_rp_css", "f_rp_ber", "f_rp_event_count", "f_rp_ess_agg_7d", "f_rp_event_count_agg_7d"))


df_regression <- df_regression %>% mutate(across(.cols = all_of(cols_to_winsor), .fns  = ~ winsorize_vec(.x, probs = c(0.01, 0.99))))

# Standardization
df_regression <- standardise_vars(df_regression, setdiff(cols_to_winsor, c("imbalance_in_1d_100000_0", "imbalance_in_1d_500_0", "imbalance_in_1d_2000_500", "imbalance_in_1d_10000_2000", "imbalance_in_1d_100000_30000", "imbalance_in_1d_30000_10000",
                                                                           "imbalance_in_0d_100000_0", "imbalance_in_0d_500_0", "imbalance_in_0d_2000_500", "imbalance_in_0d_10000_2000", "imbalance_in_0d_100000_30000", "imbalance_in_0d_30000_10000",
                                                                           "signedvol_in_1d_100000_0", "signedvol_in_1d_500_0", "signedvol_in_1d_2000_500", "signedvol_in_1d_10000_2000", "signedvol_in_1d_100000_30000", "signedvol_in_1d_30000_10000",
                                                                           "signedvol_in_0d_100000_0", "signedvol_in_0d_500_0", "signedvol_in_0d_2000_500", "signedvol_in_0d_10000_2000", "signedvol_in_0d_100000_30000", "signedvol_in_0d_30000_10000",
                                                                           
                                                                           "fwd_ret_1d_log", "fwd_ret_2d_log", "fwd_ret_3d_log", "fwd_ret_4d_log", "fwd_ret_5d_log", "fwd_ret_22d_log", "fwd_ret_126d_log", "fwd_ret_252d_log")))


# Retail imbalance
im3 <- felm(imbalance_in_1d_100000_0 ~ linear + estimate + prc_yest_log +
              `imbalance_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, 
            data = df_regression)
summary(im3)

summary(df_regression)


analyze_felm_correlations(im3, df_regression)

plot(density(df_regression$stock_split))

df_regression %>% filter(abs(after_stock_split) < 30) %>% group_by(bucket_split, Post) %>% summarize(Price = mean(prc), Rows = n())
View(df_regression %>% filter(abs(after_stock_split) < 30) %>% select(ticker, ann_deemed_date, after_stock_split, stock_split, bucket_split, prc) %>% arrange(ticker, stock_split, after_stock_split))




im3 <- felm(imbalance_in_1d_100000_30000 ~ estimate*prc_yest_log*Post +
              `imbalance_in_-1d_100000_30000` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, 
            data = df_regression %>% filter(bucket_split == 1))
summary(im3)

analyze_felm_correlations(im3, df_regression)

im4 <- felm(imbalance_in_1d_100000_0 ~ estimate*prc_yest_log +
              `imbalance_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(im4)


View(df %>% filter(ticker == "AAPL") %>% select(ticker, prc, prc_yest, date_stock_split, ann_deemed_date, after_stock_split, pt))


df %>% filter(abs(after_stock_split) < 10) %>% group_by(Post) %>% summarize(Estimate = mean(estimate), Linear = mean(linear), Price = mean(prc))





im5 <- felm(imbalance_in_1d_100000_0 ~ bucket + estimate + prc_yest_log +
              `imbalance_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(im5)




# Retail signedvol
sig4 <- felm(signedvol_in_1d_100000_0 ~ linear + estimate + prc_yest_log +
               `signedvol_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(sig4)

sig5 <- felm(signedvol_in_1d_100000_0 ~ bucket + estimate + prc_yest_log +
               `signedvol_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(sig5)


# Get characteristics of buckets
df_regression %>% group_by(bucket) %>% summarize(Linear = mean(linear), Size = mean(marketcap, na.rm = T))




# Correlation signedvol vs. imbalance
cor(df_regression$signedvol_100000_0, df_regression$imbalance_100000_0, use = "complete.obs") # 0.7071359


# Correlation price vs. linear variable
cor(df_regression$prc_yest, df_regression$linear, use = "complete.obs") # 0.4845085
summary(df$prc_yest)
colnames(df)


#df_regression %>% filter(prc_yest < 0) %>% select(ticker, permno) %>% distinct(ticker, permno)






#analyze_felm_correlations(sig4, df_regression)

# Different trade size buckets
buckets <- list(c(0,500), c(500,2000), c(2000,10000), c(10000,30000), c(30000,100000))

mods_full <- list()
mods_trim <- list()

for (b in buckets) {
  lower <- b[1]
  upper <- b[2]
  
  # numeric values for filtering
  lower_num <- lower
  upper_num <- upper
  
  # string versions for column names
  lower_str <- as.character(as.integer(lower))
  upper_str <- as.character(as.integer(upper))
  
  # Names of dependent and lagged variables (now match your df columns)
  dep_var <- paste0("imbalance_in_1d_",  upper_str, "_", lower_str)
  lag_var <- paste0("imbalance_in_-1d_", upper_str, "_", lower_str)
  
  # Model name labels
  model_name_full <- paste0("buc_", upper_str, "_", days_after_earnings)
  model_name_trim <- paste0("buc_", upper_str, "a_", days_after_earnings)
  
  # Build formula string
  fml_str <- paste0(
    "`", dep_var, "` ~ ",
    "linear + estimate + prc_yest_log + `", lag_var, "` + ",
    "cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + ",
    "`vol_in_-1d_log` + marketcap_log | ",
    "permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date"
  )
  
  fml <- as.formula(fml_str)
  
  # Full sample
  mods_full[[model_name_full]] <- felm(fml, data = df_regression)
  
  
  # also create objects in the workspace if you want:
  assign(model_name_full, felm(fml, data = df_regression), envir = .GlobalEnv)
  
  # Price-restricted sample: prc < upper bound of bucket
  mods_trim[[model_name_trim]] <- felm(
    fml,
    data = df_regression %>% filter(prc < upper_num)
  )
}

summary(buc_100000_5)

# See regression results
lapply(mods_full, summary)
lapply(mods_trim, summary)


summary(buc_500_3)

# Different time horizons
days <- c(1,2,5,66,132,252)

models_days <- list()

for (day in days) {
  # Change name of independent variable
  dep_var <- paste0("imbalance_in_", day, "d_100000_0")
  
  # build formula string
  fml_str <- paste0(
    dep_var, " ~ ",
    "linear + estimate + `imbalance_in_-1d_100000_0` + ",
    "cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + ",
    "vol_x_100000_0_log + marketcap_log | ",
    "permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date"
  )
  
  fml <- as.formula(fml_str)
  
  # Change regression name
  model_name <- paste0("d_", day)
  
  # run regression
  fit <- felm(fml, data = df_regression)
  
  # store in a list
  models_days[[model_name]] <- fit
  
  # also create objects in the workspace if you want:
  assign(model_name, fit, envir = .GlobalEnv)
}

# See regression results
lapply(models_days, summary)


##############################
# Robustness: Sentiment
##############################


# Add sentiment measures
sen1 <- felm(imbalance_in_1d_100000_0 ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
               `imbalance_in_-1d_100000_0` +  cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(sen1)

summary(im5)

# Add aggregated sentiment measures
sen2 <- felm(imbalance_in_1d_100000_0 ~ linear + estimate + prc_yest_log + f_rp_ess_agg_7d + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count_agg_7d +
               `imbalance_in_-1d_100000_0` +  cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(sen2)



##############################
# Robustness: Fwd Returns
##############################

#summary(df_regression$fwd_ret_1d_log)

# Regressions are without cum_ret_1d_log because it soaks up part of variation due to analysts price target
ret1 <- felm(fwd_ret_1d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
               `imbalance_in_-1d_100000_0` + cum_ret_1d_log + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret1)

ret2 <- felm(fwd_ret_2d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
               `imbalance_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret2)

ret3 <- felm(fwd_ret_3d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
               `imbalance_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret3)

ret4 <- felm(fwd_ret_4d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
               `imbalance_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret4)

ret5 <- felm(fwd_ret_5d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
               `imbalance_in_-1d_100000_0` + cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret5)

ret22 <- felm(fwd_ret_22d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
                `imbalance_in_-1d_100000_0` +  cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret22)

ret126 <- felm(fwd_ret_126d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
                 `imbalance_in_-1d_100000_0` +  cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret126)

ret252 <- felm(fwd_ret_252d_log ~ linear + estimate + prc_yest_log + f_rp_ess + f_rp_bmq + f_rp_bee + f_rp_bam + f_rp_bca + f_rp_css + f_rp_ber + f_rp_event_count +
                 `imbalance_in_-1d_100000_0` +  cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + `vol_in_-1d_log` + marketcap_log | permno + ann_deemed_date + amaskcd | 0 | permno + ann_deemed_date, data = df_regression)
summary(ret252)





















colnames(df_earnings)


summary(df$linear)

# 2) Columns to winsorize (adjust as needed)
cols_to_winsor <- c(
  "linear", "linear_abs", "pt", "estimate","cum_ret_1d","cum_ret_5d","cum_ret_252d",
  "vol_x","f_roa","f_sp","f_btm", "imbalance_in_1d", 
  "imbalance_in_-1d","imbalance_in_-2d","imbalance_in_-3d",
  "imbalance_in_-4d","imbalance_in_-5d"
)

df_earnings <- df_earnings |>
  rename_with(make.names, everything()) |>
  mutate(across(
    .cols = all_of(make.names(cols_to_winsor)),
    .fns  = ~ winsorize_vec(.x, probs = c(0.01, 0.99)),
    .names = "{.col}"
  ))


# Test Residuals
m6 <- felm(linear ~ estimate | 0 | 0 | 0, data = df_earnings)
summary(m6)

# (1) take residuals
# 1) Which rows were used in the regression?
mf <- model.frame(m6)
used_rows <- as.integer(rownames(mf))

# 2) Build a full-length residual vector and place residuals at used rows
res_full <- rep(NA_real_, nrow(df_earnings))
res_full[used_rows] <- residuals(m6)

# 3) Attach to df_earnings
df_earnings$resid_optimism <- res_full

colnames(df_earnings)

m7 <- felm(imbalance_over_5d ~ resid_optimism + estimate + signedvol_in_0d + `signedvol_in_-1d` + `signedvol_in_-2d` + `signedvol_in_-3d` + `signedvol_in_-4d`
           + `signedvol_in_-5d` + cum_ret_1d + cum_ret_5d + cum_ret_252d + log(marketcap) + vol_x +  f_roa + f_sp + f_btm | permno + ann_deemed_date | 0 | permno, data = df_earnings)
summary(m7)

m7 <- felm(test ~ y_pred_catboost + estimate + `signedvol_in_-1d` + `signedvol_in_-2d` + `signedvol_in_-3d` + `signedvol_in_-4d`
           + `signedvol_in_-5d` + cum_ret_5d + cum_ret_252d + log(marketcap) + vol_x +  f_roa + f_sp + f_btm | permno + Year | 0 | permno, data = df_earnings)
summary(m7)


df_earnings$test <- df_earnings$s / df_earnings$`s_in_-22d`

summary(df_earnings$linear)

colnames(df)
# Portfolio sorts
res <- double_portfolio_sort_plots(
  data        = df_earnings,
  date_col    = ann_deemed_date,        # or trade_date
  test_var    = linear,     # X (e.g., PT upside)
  control_var = estimate,         # Y (e.g., log mktcap)
  outcome     = fwd_ret_252d,      # e.g., next-252d return
  weight      = NULL,    # NULL for equal-weight
  n_x = 5, n_y = 5,
  agg = "median",
  axis_agg = "median",
  title_prefix = ""
)



colnames(df)

# Create cross-sectional buckets
df_earnings <- add_buckets_by_date(
  df_earnings,
  var  = linear,
  date = ann_deemed_date,
  n = 5,
  method = "ntile",
  col_name = "bucket_linear",
  ordered = FALSE   # <- important
)


res_daily <- fama_macbeth_manual(
  data    = df_earnings,
  date_col= ann_deemed_date,   # your daily date column
  formula = imbalance_in_1d ~ linear + pt + estimate + imbalance_in_0d + `imbalance_in_-1d` + `imbalance_in_-2d` + `imbalance_in_-3d` + `imbalance_in_-4d`
  + `imbalance_in_-5d` + cum_ret_1d + cum_ret_5d + cum_ret_252d + log(marketcap) + vol_x +  f_roa + f_sp + f_btm,
  weight  = NULL,              # or "marketcap" for WLS
  nw_lag  = 5,                 # ~ one trading week
  min_n   = NULL               # defaults to (#pred+1)
)

print(res_daily, n = Inf, width = Inf)



summary(df$pt)


install.packages("fixest")
b  <- coef(m6)
me <- function(flow) b["optimism_cat"] + b["optimism_cat:imbalance_over_5d"]*flow
c(P10 = me(quantile(df_earnings$imbalance_over_5d, .10, na.rm=TRUE)),
  P50 = me(quantile(df_earnings$imbalance_over_5d, .50, na.rm=TRUE)),
  P90 = me(quantile(df_earnings$imbalance_over_5d, .90, na.rm=TRUE)))


flow_star <- -b["optimism_cat"] / b["optimism_cat:imbalance_over_5d"]  # ≈ 1.01
flow_star

range(df_earnings$imbalance_over_5d, na.rm=TRUE)





