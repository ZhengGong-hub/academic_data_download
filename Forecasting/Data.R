############################################################ 

############## Create data frame

############################################################ 

# Install packages
for (i in c("arrow", "dplyr", "tidyr", "lfe", "lubridate", "ggplot2", "rlang", "purrr", "broom", "sandwich", "lmtest", "glmnet", "xgboost", "yardstick")){
  install.packages(i)
}

# Load packages
for (i in c("arrow", "dplyr", "tidyr", "lfe", "lubridate", "ggplot2", "rlang", "purrr", "broom", "sandwich", "lmtest", "glmnet", "xgboost", "yardstick")) {
  library(i, character.only = TRUE)
}

#start_year <- 2013

# Set working directory
setwd("/home/ubuntu/academic_data_download/data")

##### Load TAQ data set
path = "taq/processed/taq_retail_markethour_processed_combined.parquet"
taq_df <- read_parquet(path)

# Sort by date
taq_df <- taq_df[order(taq_df$date), ]

# Convert to datetime
taq_df$date <- as.Date(taq_df$date)

# Replace NA in sym_suffix with empty string
taq_df$sym_suffix[is.na(taq_df$sym_suffix)] <- ""

# Create full_name
taq_df$full_name <- paste0(taq_df$sym_root, taq_df$sym_suffix)
nrow(taq_df) # 9,669,046

# Get rid of duplicates
taq_df <- taq_df %>% add_count(permno, date) %>%  filter(n == 1) %>% select(-n)
nrow(taq_df) # 9,645,676


##### Load CRSP data set
path = "pricevol/pricevol_processed_past_prc.parquet"
pricevol <- read_parquet(path)
nrow(pricevol) # 38,510,382

# Convert to datetime
pricevol$date <- as.Date(pricevol$date)

# Convert volume to thousands and round
pricevol$vol <- round(pricevol$vol / 1000, 0)


##### Load forward and past returns
path = "pricevol/pricevol_processed.parquet"
fwd <- read_parquet(path)
nrow(fwd) # 38,510,382

# Merge with pricevol
fwd <- fwd %>% mutate(date = as.Date(date))
pricevol <- pricevol %>% left_join(fwd %>% select(permno,date, setdiff(names(fwd), names(pricevol))),by = c("permno", "date"))
rm(fwd)
gc()

##### Load marketcap data set
path = "pricevol/marketcap.parquet"
marketcap <- read_parquet(path)
nrow(marketcap) # 37,838,222

# Convert to datetime and exclude duplicates
marketcap <- marketcap %>% mutate(date = as.Date(date)) %>% select(date, permco, marketcap) %>% add_count(permco, date) %>%  filter(n == 1) %>% select(-n)

# Merge with pricevol
pricevol <- pricevol %>% left_join(marketcap, by = c("permco", "date"))
rm(marketcap)


##### Load past volume data set
path = "pricevol/pricevol_processed_with_past_vol.parquet"
vol <- read_parquet(path)
nrow(vol) # 38,510,382

# Convert to datetime
vol <- vol %>% mutate(date = as.Date(date))

# Merge with pricevol
pricevol <- pricevol %>% left_join(vol %>% select(permno, date, setdiff(names(vol), names(pricevol))),by = c("permno", "date"))
rm(vol)
gc()


##### Load Ravenpack data set
path = "ravenpack/f_rp_ess.parquet"
ravenpack <- read_parquet(path)
nrow(ravenpack) # 31,020,164

# Convert to datetime
ravenpack$trading_day_et <- as.Date(ravenpack$trading_day_et)

# Merge with pricevol
pricevol <- pricevol %>% left_join(ravenpack, by = c("permco", "date" = "trading_day_et"))
rm(ravenpack); gc()


##### Load Factors data set
path = "factors/combined/factors_combined.parquet"
factors <- read_parquet(path)
nrow(factors) # 37,803,204

# Convert to datetime and remove duplicates
factors <- factors %>% mutate(date = as.Date(date)) %>% add_count(permco, date) %>%  filter(n == 1) %>% select(-n)

# Merge with pricevol
pricevol <- pricevol %>% left_join(factors %>% select(permco, date, setdiff(names(factors), names(pricevol))),by = c("permco", "date"))
nrow(pricevol) # 38,510,382
rm(factors); gc()


##### Merge TAQ and CRSP
taq_df <- taq_df %>% left_join(pricevol, by = c("permno", "date"))


# Save data as parquet file
write_parquet(taq_df, "/home/ubuntu/academic_data_download/Forecasting/data.parquet")


############################################################ 

############## Manipulate data frame

############################################################ 

taq <- read_parquet("/home/ubuntu/academic_data_download/Forecasting/data.parquet")
nrow(taq) # 9,645,676

# Filter data frame
taq <- taq %>% filter(prc >= 5, marketcap > 0) %>% mutate(date = as.Date(date, format = "%Y-%m-%d"), year = year(date), quarter = quarter(date))

# Calculate imbalance measures for each trade size bucket
all_sb <- grep("^sb(_in_-?\\d+d)?_\\d+_\\d+$", names(taq), value = TRUE)
if (length(all_sb) == 0) stop("No sb_* columns found.")

get_bucket <- function(x) sub(".*_(\\d+_\\d+)$", "\\1", x)
buckets <- sort(unique(get_bucket(all_sb)))

# For a given bucket, list all sb columns (plain and _in_±kd)
sb_cols_for_bucket <- function(bucket) {
  # matches sb_<bucket> and sb_in_-5d_<bucket>, etc.
  pattern <- paste0("^sb(?:_in_-?\\d+d)?_", bucket, "$")
  grep(pattern, names(taq), value = TRUE)
}

# For each bucket, compute (sb-ss)/(sb+ss) and (sb-ss)/vol with aligned counterparts
for (bucket in buckets) {
  sb_cols <- sb_cols_for_bucket(bucket)
  if (length(sb_cols) == 0) next
  
  for (sb in sb_cols) {
    # Matching ss column
    ss <- sub("^sb", "ss", sb)
    if (!ss %in% names(taq)) stop("Missing matching ss column for: ", sb)
    
    # Decide the matching vol column:
    # If this is an 'in' horizon (sb_in_..._bucket), use vol_in_<same horizon>_<bucket>
    # else (plain sb_<bucket>) prefer vol_in_0d_<bucket>, else vol_x_<bucket>
    
    # Decide the matching vol column:
    if (grepl("^sb_in_-?\\d+d_", sb)) {
      # Extract the horizon, e.g. -5d, 0d, 66d, 252d
      horizon <- sub("^sb_in_(-?\\d+d)_.*$", "\\1", sb)
      vol <- paste0("vol_in_", horizon)  # e.g. "vol_in_-5d"
      
      if (!vol %in% names(taq)) stop("Missing matching vol column for: ", sb)
    } else {
      # plain sb_<bucket>: use overall vol_in_0d (no bucket)
      vol0 <- "vol_in_0d"
      volx <- "vol_x"
      vol  <- if (vol0 %in% names(taq)) vol0 else if (volx %in% names(taq)) volx else NA_character_
      if (is.na(vol)) stop("No vol_in_0d or vol_x found (needed for ", sb, ")")
    }
    
    
    # Output names mirror the sb name, just with prefixes changed
    imb <- sub("^sb", "imbalance", sb)  # (sb - ss) / (sb + ss)
    sgv <- sub("^sb", "signedvol", sb)  # (sb - ss) / vol
    
    # Compute safely
    num <- taq[[sb]] - taq[[ss]]
    
    den <- taq[[sb]] + taq[[ss]]
    den[den == 0] <- NA_real_
    taq[[imb]] <- num / den
    
    den2 <- taq[[vol]]
    den2[den2 == 0] <- NA_real_
    taq[[sgv]] <- num / den2
  }
}


# Only keep rows where imbalance measure is not NA
# Replace Inf with NA in all numeric columns
# Replace NA with 0 in RavenPack columns
taq <- taq %>% filter(!is.infinite(imbalance_in_1d_100000_0), date >= as.Date("2013-12-08")) %>% mutate(across(where(is.numeric), ~ { .x[is.infinite(.x)] <- NA_real_; .x }), across(starts_with("f_rp_"), ~ ifelse(is.na(.x), 0, .x)))
nrow(taq); gc() # 7,166,001



############################################################ 

############## Machine Learning

############################################################ 


##### Define functions

# Drop rows where percentage of factors ("f_") is missing
remove_row_with_missing_values <- function(df, threshold = 50) {
  if (!is.data.frame(df)) stop("Input must be a data frame")
  if (nrow(df) == 0) stop("Input data frame is empty")
  if (ncol(df) == 0) stop("Input data frame has no columns")
  if (threshold < 0 || threshold > 100) {
    stop(sprintf("Threshold must be between 0 and 100, got %s", threshold))
  }
  
  df_clean <- df
  
  f_columns <- grep("^f_", names(df_clean), value = TRUE)
  if (length(f_columns) == 0) {
    df_clean$missing_pct <- 0
  } else {
    df_clean$missing_pct <- rowMeans(is.na(df_clean[, f_columns, drop = FALSE])) * 100
  }
  
  before_n <- nrow(df_clean)
  df_clean <- df_clean[df_clean$missing_pct <= threshold, ]
  df_clean$missing_pct <- NULL
  
  df_clean
}

# Get all quarters from previous three years. Necessary input for training
get_last_three_years <- function(current_yq) {
  # current_yq = c(year, quarter)
  stopifnot(length(current_yq) == 2)
  year0 <- current_yq[1]
  q0    <- current_yq[2]
  
  res <- list()
  k <- 1
  for (year in (year0 - 3):year0) {
    if (year == year0 - 3) {
      start_q <- q0
    } else {
      start_q <- 1
    }
    if (year == year0) {
      end_q <- q0 - 1
    } else {
      end_q <- 4
    }
    if (end_q >= start_q) {
      for (quarter in start_q:end_q) {
        res[[k]] <- c(year, quarter)
        k <- k + 1
      }
    }
  }
  if (length(res) == 0) return(matrix(numeric(0), ncol = 2))
  do.call(rbind, res)
}


# Set directories for training and test sample
training_data_dir <- "/home/ubuntu/academic_data_download/Forecasting/training_data"
test_data_dir     <- "/home/ubuntu/academic_data_download/Forecasting/test_data"
output_data_dir <- "/home/ubuntu/academic_data_download/Forecasting/output_data"

# Drop rows with too many missing values across f_ columns
taq_processed <- remove_row_with_missing_values(taq, threshold = 10)

# Median imputation for remaining factor columns ("f_")
for (col in grep("^f_", names(taq_processed), value = TRUE)) {
  med <- median(taq_processed[[col]], na.rm = TRUE)
  nas <- is.na(taq_processed[[col]])
  taq_processed[[col]][nas] <- med
}

# Rolling train/test by year
year_start <- min(taq_processed$year, na.rm = TRUE)
year_end   <- max(taq_processed$year, na.rm = TRUE)
paste("first_year:", year_start); paste("last_year:", year_end); gc()

# Split into train and test set. Train set keeps previous three years and test set keeps current year
for (yr in seq(year_start + 3, year_end)) {
  last_x_years <- seq(yr - 3, yr - 1)
  
  # Write train set data
  train_set <- taq_processed[taq_processed$year %in% last_x_years, ]
  write_parquet(train_set, file.path(training_data_dir, paste0(yr, ".parquet")))
  
  # Write test set data
  test_set <- taq_processed[taq_processed$year == yr, ]
  write_parquet(test_set, file.path(test_data_dir, paste0(yr, ".parquet")))
  gc()
}


# Run Machine Learners
MLFramework <- function(train_df, test_df, target_col, feature_cols, output_dir) {
  
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  models_dir <- file.path(output_dir, "models")
  dir.create(models_dir, recursive = TRUE, showWarnings = FALSE)
  
  main_log_file <- file.path(output_dir, "ml_run.txt")
  writeLines(
    sprintf("ML Framework Run - %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    con = main_log_file
  )
  
  write_to_file <- function(filepath, content, append = TRUE) {cat(content, "\n", file = filepath, append = append)}
  
  # Compare forecast with true value
  calc_metrics <- function(y_true, y_pred) {
    mse  <- mean((y_true - y_pred)^2)
    rmse <- sqrt(mse)
    mae  <- mean(abs(y_true - y_pred))
    ss_res <- sum((y_true - y_pred)^2)
    ss_tot <- sum((y_true - mean(y_true))^2)
    r2   <- 1 - ss_res / ss_tot
    list(mse = mse, rmse = rmse, mae = mae, r2 = r2)
  }
  
  # Prepare data (winsorize + scale using training stats)
  prepare_data <- function() {
    X_train <- train_df[, feature_cols, drop = FALSE]
    y_train <- train_df[[target_col]]
    X_test  <- test_df[, feature_cols, drop = FALSE]
    y_test  <- test_df[[target_col]]
    
    winsorize <- function(x) {
      qs <- quantile(x, c(0.01, 0.99), na.rm = TRUE)
      pmin(pmax(x, qs[1]), qs[2])
    }
    X_train <- as.data.frame(lapply(X_train, winsorize))
    X_test  <- as.data.frame(lapply(X_test,  winsorize))
    
    means <- sapply(X_train, mean)
    sds   <- sapply(X_train, sd)
    sds[sds == 0] <- 1  # avoid division by zero
    
    scale_with <- function(df) {
      as.data.frame(scale(df, center = means, scale = sds))
    }
    
    X_train_s <- scale_with(X_train)
    X_test_s  <- scale_with(X_test)
    
    list(
      X_train = as.matrix(X_train_s),
      y_train = y_train,
      X_test  = as.matrix(X_test_s),
      y_test  = y_test
    )
  }
  
  save_feature_importance <- function(importance, model_name) {
    if (is.null(importance)) return(invisible(NULL))
    if (length(importance) != length(feature_cols)) {
      # just skip if lengths don't match
      return(invisible(NULL))
    }
    importance_df <- data.frame(
      feature    = feature_cols,
      importance = as.numeric(importance)
    )
    importance_df <- importance_df %>%
      mutate(abs_importance = abs(importance)) %>%
      arrange(desc(abs_importance)) %>%
      select(-abs_importance)
    importance_file <- file.path(models_dir, paste0(model_name, "_feature_importance.csv"))
    write.csv(importance_df, importance_file, row.names = FALSE)
  }
  
  # Initialize models as a list of training functions
  initialize_models <- function() {
    list(
      linear = function(X, y) {
        data <- as.data.frame(X)
        data$y <- y
        lm(y ~ ., data = data)
      },
      ridge = function(X, y) {
        cv.glmnet(X, y, alpha = 0)  # ridge with CV
      },
      lasso = function(X, y) {
        cv.glmnet(X, y, alpha = 1)  # lasso with CV
      },
      xgb = function(X, y) {
        dtrain <- xgb.DMatrix(data = X, label = y)
        xgboost(
          data = dtrain,
          nrounds = 500,
          eta = 0.05,
          max_depth = 6,
          subsample = 0.8,
          colsample_bytree = 0.8,
          objective = "reg:squarederror",
          verbose = 0
        )
      }
      # You could add lightgbm and catboost here if you install those R packages
    )
  }
  
  data_list      <- prepare_data()
  model_builders <- initialize_models()
  trained_models <- list()   # will hold fitted models
  
  # Train models
  train_models <- function() {
    scores <- list()
    
    for (name in names(model_builders)) {
      model <- model_builders[[name]](data_list$X_train, data_list$y_train)
      
      # feature importance (crude approximations)
      importance <- NULL
      if (name == "linear") {
        coefs <- coef(model)
        importance <- coefs[-1]  # drop intercept
      } else if (name %in% c("ridge", "lasso")) {
        coefs <- coef(model, s = "lambda.min")
        importance <- as.numeric(coefs)[-1]
      } else if (name == "xgb") {
        imp <- xgb.importance(model = model)
        importance <- rep(0, length(feature_cols))
        names(importance) <- feature_cols
        if (!is.null(imp) && nrow(imp) > 0) {
          # align by feature name if they are simple "f1","f2", etc.
          # if your feature names are more complex, you might need mapping
          common <- intersect(imp$Feature, names(importance))
          importance[common] <- imp$Gain[match(common, imp$Feature)]
        }
      }
      save_feature_importance(importance, name)
      
      # in-sample metrics
      y_pred <- switch(
        name,
        linear = as.numeric(predict(model, newdata = as.data.frame(data_list$X_train))),
        ridge  = as.numeric(predict(model, newx = data_list$X_train, s = "lambda.min")),
        lasso  = as.numeric(predict(model, newx = data_list$X_train, s = "lambda.min")),
        xgb    = as.numeric(predict(model, newdata = data_list$X_train))
      )
      
      metrics <- calc_metrics(data_list$y_train, y_pred)
      scores[[name]] <- metrics
      
      # store trained model in outer list
      trained_models[[name]] <<- model
    }
    
    # Save metrics to CSV (in-sample scores)
    scores_df <- do.call(
      rbind,
      lapply(names(scores), function(nm) {
        data.frame(
          model = nm,
          mse   = scores[[nm]]$mse,
          rmse  = scores[[nm]]$rmse,
          mae   = scores[[nm]]$mae,
          r2    = scores[[nm]]$r2,
          row.names = NULL
        )
      })
    )
    write.csv(scores_df, file.path(models_dir, "scores.csv"), row.names = FALSE)
    
    scores_df
  }
  
  # -------------------------
  # evaluate_models()
  # -------------------------
  evaluate_models <- function() {
    if (length(trained_models) == 0) {
      stop("No models have been trained. Call train_models() first.")
    }
    
    test_metrics <- list()
    
    # copy so we can attach predictions
    test_df_local <- test_df
    
    for (name in names(trained_models)) {
      model <- trained_models[[name]]
      
      y_pred <- switch(
        name,
        linear = as.numeric(predict(model, newdata = as.data.frame(data_list$X_test))),
        ridge  = as.numeric(predict(model, newx = data_list$X_test, s = "lambda.min")),
        lasso  = as.numeric(predict(model, newx = data_list$X_test, s = "lambda.min")),
        xgb    = as.numeric(predict(model, newdata = data_list$X_test))
      )
      
      metrics <- calc_metrics(data_list$y_test, y_pred)
      test_metrics[[name]] <- metrics
      
      # store predictions as new column
      pred_col <- paste0("y_pred_", name)
      test_df_local[[pred_col]] <- y_pred
    
    }
    
    test_df_local$y_true <- data_list$y_test
    
    # Save test metrics to CSV
    test_scores_df <- do.call(
      rbind,
      lapply(names(test_metrics), function(nm) {
        data.frame(
          model = nm,
          mse   = test_metrics[[nm]]$mse,
          rmse  = test_metrics[[nm]]$rmse,
          mae   = test_metrics[[nm]]$mae,
          r2    = test_metrics[[nm]]$r2,
          row.names = NULL
        )
      })
    )
    
    write.csv(
      test_scores_df,
      file.path(models_dir, "oos_test_scores.csv"),
      row.names = FALSE
    )
    
    # Save test set with predictions
    write_parquet(
      test_df_local,
      file.path(models_dir, "oos_test_set.parquet")
    )
    
    test_metrics
  }
  
  # Object-style API: like a class instance in Python
  list(
    train_models   = train_models,
    evaluate_model = evaluate_models
  )
}




# 1. Choose a year
year <- 2018
gc()

# Load training and test data
train_df <- read_parquet(paste(training_data_dir, "/", as.character(year), ".parquet", sep = ""))
test_df  <- read_parquet(paste(test_data_dir, "/", as.character(year), ".parquet", sep = ""))

cat("Train set shape:", dim(train_df), "\n")
cat("Test set shape:",  dim(test_df),  "\n")




target_col <- "imbalance"
train_df[[target_col]] <- train_df$imbalance_in_1d_100000_0
test_df[[target_col]]  <- test_df$imbalance_in_1d_100000_0

imbalance_cols = c("imbalance_in_0d_100000_0", "imbalance_in_-1d_100000_0", "imbalance_in_-2d_100000_0", "imbalance_in_-3d_100000_0", "imbalance_in_-4d_100000_0", "imbalance_in_-5d_100000_0")
return_cols = c("cum_ret_1d", "cum_ret_2d", "cum_ret_3d", "cum_ret_4d", "cum_ret_5d", "cum_ret_22d", "cum_ret_126d", "cum_ret_252d")
volume_cols = c("vol_in_0d", "vol_in_-1d", "vol_in_-2d", "vol_in_-3d", "vol_in_-4d", "vol_in_-5d")

# Define feature columns
feature_cols <- c(grep("^f_", names(train_df), value = TRUE), imbalance_cols, return_cols, volume_cols)

# Get rid of all rows where at list one column is NA
train_df <- train_df %>% filter(if_all(all_of(feature_cols), ~ !is.na(.)),!is.na(!!sym(target_col)))
test_df <- test_df %>% filter(if_all(all_of(feature_cols), ~ !is.na(.)),!is.na(!!sym(target_col)))



# Create the framework object
ml <- MLFramework(
  train_df   = train_df,
  test_df    = test_df,
  target_col = target_col,
  feature_cols = feature_cols,
  output_dir = file.path("/home/ubuntu/academic_data_download/Forecasting/output_data", "ml_run_imbalance", as.character(year))
)




# 6. Train models + evaluate on test set
train_scores <- ml$train_models()
test_scores  <- ml$evaluate_model()

print(train_scores)
print(test_scores)





