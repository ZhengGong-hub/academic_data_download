# Load packages
for (i in c("arrow", "dplyr", "tidyr", "lfe", "lubridate", "ggplot2", "rlang", "purrr", "broom", "sandwich", "lmtest")) {
  library(i, character.only = TRUE)
}


##############################
# Define functions
##############################
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


##############################
# Load data
##############################

analyst <- read_parquet("/home/ubuntu/academic_data_download/Analysis/analyst.parquet")


# Set print to max
options(max.print = .Machine$integer.max)
#options(width = 10000)


##############################
# Manipulate data frame
##############################



# Filter data frame and calculate variables
df <- df %>% filter(!is.na(eps_act), !is.na(eps_est), !is.na(et)) %>% 
  mutate(
    # Change to date format
    pends = as.Date(pends, format = "%Y-%m-%d"), et = as.Date(et, format = "%Y-%m-%d"), earnings_deemed_date = as.Date(earnings_deemed_date, format = "%Y-%m-%d"),
    n1q_pends = as.Date(n1q_pends, format = "%Y-%m-%d"), n1y_pends = as.Date(n1y_pends, format = "%Y-%m-%d"), eps_est_ann = as.Date(eps_est_ann, format = "%Y-%m-%d"), ex_pt_ann = as.Date(ex_pt_ann, format = "%Y-%m-%d"),
    post_pt_ann = as.Date(post_pt_ann, format = "%Y-%m-%d"), ex_n1q_eps_est_ann = as.Date(ex_n1q_eps_est_ann, format = "%Y-%m-%d"),
    post_n1q_eps_est_ann = as.Date(post_n1q_eps_est_ann, format = "%Y-%m-%d"), ex_n1y_eps_est_ann = as.Date(ex_n1y_eps_est_ann, format = "%Y-%m-%d"),
    post_n1y_eps_est_ann = as.Date(post_n1y_eps_est_ann, format = "%Y-%m-%d"), month = format(et, "%Y-%m"),
    
    # Time between earnings call and (i) estimate and (ii) update
    after_earning_pt = post_pt_ann - et, before_earning_pt = et - ex_pt_ann,
    after_earning_eps_1q = post_n1q_eps_est_ann - et, before_earning_eps_1q = et - ex_n1q_eps_est_ann,
    after_earning_eps_1y = post_n1y_eps_est_ann - et, before_earning_eps_1y = et - ex_n1y_eps_est_ann,
    
    # Revision variables
    eps_beat = (eps_act-eps_est)/abs(eps_est), eps_linear = eps_act-eps_est, eps_linear_abs = abs(eps_act-eps_est),
    revision_pt = (post_pt-ex_pt)/ex_pt,
    revision_eps_1q = (post_n1q_eps_est-ex_n1q_eps_est)/ex_n1q_eps_est,
    revision_eps_1y = (post_n1y_eps_est-ex_n1y_eps_est)/ex_n1y_eps_est)

# Control for inconsistent analysts
df <- df %>% mutate(Inconsistent = ifelse(post_n1y_eps_est_ann == post_pt_ann, ifelse(revision_pt * revision_eps_1y < 0,1,0), NA))

# Create data frames for (i) price target and (ii) EPS
df_pt <- df %>% filter(is.finite(revision_pt), is.finite(eps_beat))
df_eps_1q <- df %>% filter(is.finite(revision_eps_1q), is.finite(eps_beat))
df_eps_1y <- df %>% filter(is.finite(revision_eps_1y), is.finite(eps_beat))


##############################
# Regressions
##############################

# f_roa Return on asset
# f_pm Profit margin
# f_ep earnings to price
# f_ig investment growth
# f_dtm debt to market cap
# f_cfp cashflow to price
# f_btm book to market



##############################
# Regressions: Price Target
##############################

df_regression <- df_pt %>% filter(after_earning_pt < 30, before_earning_pt < 60) %>% mutate(Post = ifelse(et >= as.Date("2018-01-01"),1,0), bucket = dplyr::ntile(eps_linear, 5), direction = ifelse(eps_act - eps_est > 0,1,0), revision_pt = 100*revision_pt)
df_regression$bucket <- factor(df_regression$bucket, levels = 1:5)

# Create buckets for time of revision
df_regression <- df_regression %>% mutate(after_earning_pt_bucket = case_when(after_earning_pt %in% c(0) ~ "0", after_earning_pt %in% 1:5 ~ "1-5", after_earning_pt %in% 6:10 ~ "6-10", after_earning_pt %in% 11:20 ~ "11-20",
                                                                              after_earning_pt %in% 21:30 ~ "21-30", TRUE ~ NA_character_), after_earning_pt_bucket = factor(after_earning_pt_bucket, levels = c("0","1-5","6-10","11-20","21-30"))) %>% filter(!is.na(after_earning_pt_bucket))
# Transform variables
vars_ret   <- c("cum_ret_252d", "cum_ret_126d", "cum_ret_22d", "cum_ret_1d", "fwd_ret_1d", "fwd_ret_5d", "fwd_ret_22d", "fwd_ret_126d", "fwd_ret_252d")
vars_level <- c("vol", "marketcap_x", "prc")

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
cols_to_winsor <- union(paste0(union(vars_ret, vars_level), "_log"), c("revision_pt", "eps_linear", "eps_beat", "eps_act", "eps_est", "f_roa", "f_btm", "f_pm", "f_ep", "f_ig", "f_dtm", "f_cfp"))

df_regression <- df_regression %>% mutate(across(.cols = all_of(make.names(cols_to_winsor)), .fns  = ~ winsorize_vec(.x, probs = c(0.01, 0.99))))

# Standardization
df_regression <- standardise_vars(df_regression, setdiff(cols_to_winsor, "revision_pt"))

pt1 <- felm(revision_pt ~ eps_linear | 0 | 0 | permno + et, data = df_regression)
summary(pt1)

pt2 <- felm(revision_pt ~ eps_linear +
              cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt2)

pt3 <- felm(revision_pt ~ eps_linear + eps_beat +
              cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt3)

pt4 <- felm(revision_pt ~ eps_linear + eps_beat + prc_log + eps_act +
              cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt4)

pt5 <- felm(revision_pt ~ eps_linear + eps_beat + prc_log + eps_est +
              cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt5)

# Correlation between price and reported EPS (standardized)
cor(df_regression$prc_log, df_regression$eps_act, use = "complete.obs") # 0.6216799
cor(df_regression$eps_act, df_regression$eps_est, use = "complete.obs") # 0.9552305


pt6 <- felm(revision_pt ~ eps_linear + eps_beat + i(after_earning_pt_bucket, eps_linear, ref = 0) +
              cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt6)



# Get number of rows per revision day
df_regression %>% group_by(after_earning_pt_bucket) %>% summarize(Rows = n())

# Return regressions
pt11 <- felm(fwd_ret_1d ~ eps_linear + eps_beat + prc_log + eps_est +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt11)

pt12 <- felm(fwd_ret_5d ~ eps_linear + eps_beat + prc_log + eps_est +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt12)

pt13 <- felm(fwd_ret_22d ~ eps_linear + eps_beat + prc_log + eps_est +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt13)

pt14 <- felm(fwd_ret_126d ~ eps_linear + eps_beat + prc_log + eps_est +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt14)

pt15 <- felm(fwd_ret_252d ~ eps_linear + eps_beat + prc_log + eps_est +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt15)


##############################
# Robustness Checks
##############################


# (1) Change time period around earnings call
horizons <- c(5, 10, 120)

for (h in horizons) {
  df_regression <- df_pt %>% filter(after_earning_pt < h, before_earning_pt < h) %>% mutate(Post = ifelse(et >= as.Date("2018-01-01"),1,0), bucket = dplyr::ntile(eps_linear, 5), direction = ifelse(eps_act - eps_est > 0,1,0), revision_pt = 100*revision_pt)
  df_regression$bucket <- factor(df_regression$bucket, levels = 1:5)
  
  # Create buckets for time of revision
  df_regression <- df_regression %>% mutate(after_earning_pt_bucket = case_when(after_earning_pt %in% c(0) ~ "0", after_earning_pt %in% 1:5 ~ "1-5", after_earning_pt %in% 6:10 ~ "6-10", after_earning_pt %in% 11:20 ~ "11-20",
                                                                                after_earning_pt %in% 21:30 ~ "21-30", TRUE ~ NA_character_), after_earning_pt_bucket = factor(after_earning_pt_bucket, levels = c("0","1-5","6-10","11-20","21-30"))) %>% filter(!is.na(after_earning_pt_bucket))
  # Transform variables
  vars_ret   <- c("cum_ret_252d", "cum_ret_126d", "cum_ret_22d", "cum_ret_1d", "fwd_ret_1d", "fwd_ret_5d", "fwd_ret_22d", "fwd_ret_126d", "fwd_ret_252d")
  vars_level <- c("vol", "marketcap_x", "prc")
  
  df_regression <- log_vars(df_regression, vars_ret, add_one = TRUE, suffix = "_log")
  df_regression <- log_vars(df_regression, vars_level, add_one = FALSE, suffix = "_log")
  
  # Handle infinite values
  for (v in intersect(paste0(union(vars_ret, vars_level), "_log"), names(df_regression))) {
    bad <- !is.finite(df_regression[[v]])  # Inf, -Inf, NaN
    df_regression[[v]][bad] <- NA
  }
  
  # Winsorization
  cols_to_winsor <- union(paste0(union(vars_ret, vars_level), "_log"), c("revision_pt", "eps_linear", "eps_beat", "eps_act", "eps_est"))
  
  df_regression <- df_regression %>% mutate(across(.cols = all_of(make.names(cols_to_winsor)), .fns  = ~ winsorize_vec(.x, probs = c(0.01, 0.99))))
  
  # Standardization
  df_regression <- standardise_vars(df_regression, setdiff(cols_to_winsor, "revision_pt"))
  
  # Run regressions
  assign(paste0("h1_", h),   
         felm(revision_pt ~ eps_linear + eps_beat +
                cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression))
  
  assign(paste0("h2_", h),   
         felm(revision_pt ~ eps_linear + eps_beat + prc_log + eps_est +
                cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression))
  
  assign(paste0("h3_", h),    
         felm(revision_pt ~ eps_linear + eps_beat + prc_log + eps_act +
                cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression))
  
}


# (2) Add financial statement figures
pt2r <- felm(revision_pt ~ eps_linear + f_roa + f_btm + f_pm + f_ep + f_ig + f_dtm + f_cfp +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt2r)

pt3r <- felm(revision_pt ~ eps_linear + eps_beat + f_roa + f_btm + f_pm + f_ep + f_ig + f_dtm + f_cfp +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt3r)

pt4r <- felm(revision_pt ~ eps_linear + eps_beat + prc_log + eps_act + f_roa + f_btm + f_pm + f_ep + f_ig + f_dtm + f_cfp +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt4r)

pt5r <- felm(revision_pt ~ eps_linear + eps_beat + prc_log + eps_est + f_roa + f_btm + f_pm + f_ep + f_ig + f_dtm + f_cfp +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt5r)





# Build cross-sectional portfolios. Double sort

# Did bias decrease with Mifid II starting in 2018?
pt21 <- felm(revision_pt ~ eps_linear*Post + eps_beat + log(prc) +
               log(1+cum_ret_252d) + log(1+cum_ret_126d) + log(1+cum_ret_22d) + log(1+cum_ret_1d) + log(vol)  + log(marketcap_x) | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt21)

# Is the effect driven by inconsistent analysts?
pt31 <- felm(revision_pt ~ eps_linear*Inconsistent + eps_beat + log(prc) +
               log(1+cum_ret_252d) + log(1+cum_ret_126d) + log(1+cum_ret_22d) + log(1+cum_ret_1d) + log(vol)  + log(marketcap_x) | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(pt31)

analyze_felm_correlations(pt4, df_regression)

df_regression <- df_regression %>% group_by(et) %>% mutate(beat_dec = ntile(log(eps_beat), 5)) %>% group_by(et, beat_dec) %>% mutate(bias_q = ntile(eps_linear, 5), quintile = factor(bias_q, 1:5, paste0("Q",1:5))) %>% ungroup()

# Remove dates with less than 10 observations

View(df_regression[,c("permno", "et", "bias_q")])

# crsp_daily: permno, date, ret
# ff_factors: date, mkt, smb, hml, rmw, cma, mom

h <- 63L  # holding horizon (~3 months)

# Expand each event to its next h trading days for that permno
holdings <- df_events_q %>%
  select(permno, et, quintile) %>%
  inner_join(crsp_daily, by = "permno") %>%
  filter(date >= et) %>%
  group_by(permno, et, quintile) %>%
  arrange(date, .by_group = TRUE) %>%
  slice_head(n = h) %>%                     # use filter(date > et) to skip announcement day
  ungroup() %>%
  mutate(weight = 1)

# Daily portfolio returns by quintile
port_daily <- holdings %>%
  group_by(date, quintile) %>%
  summarise(n = sum(!is.na(ret)),
            port_ret = mean(ret, na.rm = TRUE),
            .groups = "drop") %>%
  filter(n >= 20) %>%                        # optional liquidity filter
  left_join(ff_factors, by = "date")

# Calendar-time alphas with Newey–West (lag = h-1)
library(fixest)

get_alpha <- function(df) {
  m <- feols(port_ret ~ mkt + smb + hml + rmw + cma + mom,
             data = df, vcov = ~ nw(date, lag = h - 1L))
  c(alpha = coef(m)["(Intercept)"], t = coef(m)["(Intercept)"]/se(m)["(Intercept)"])
}

alphas <- port_daily %>%
  group_by(quintile) %>%
  do({
    a <- get_alpha(.)
    tibble(alpha = unname(a["alpha"]), t = unname(a["t"]))
  }) %>% ungroup()

# Long–short (Q5−Q1)
ls_daily <- port_daily %>%
  select(date, quintile, port_ret) %>%
  tidyr::pivot_wider(names_from = quintile, values_from = port_ret) %>%
  mutate(ls = Q5 - Q1) %>%
  left_join(ff_factors, by = "date")

mod_ls <- feols(ls ~ mkt + smb + hml + rmw + cma + mom,
                data = ls_daily, vcov = ~ nw(date, lag = h - 1L))

ls_alpha <- coef(mod_ls)["(Intercept)"]
ls_t     <- ls_alpha / se(mod_ls)["(Intercept)"]




library(data.table); library(tidyr)

setDT(df_events); setkey(df_events, pt_date)

# 2a) Within each PT day, make quintiles of bias_total
df_events <- df_events[ , {
  qs <- quantile(bias_total, probs = c(.2,.4,.6,.8), na.rm = TRUE, type = 7)
  quint <- cut(bias_total, breaks = c(-Inf, qs, Inf), labels = paste0("Q",1:5), right = TRUE)
  .(permno, pt_date, bias_total, PTRev, quintile = quint)
}, by = pt_date]

# 2b) Expand to holding window (h days after PT date, **exclude** day 0 or include—your choice)
h <- 63L
# Create a calendar of trading days per permno; here we merge to CRSP to get actual trading dates
setDT(crsp_daily); setkey(crsp_daily, permno, date)

# For speed: pre-build a map from PT date to the next h trading dates for each permno
holdings <- df_events[ , {
  # get that stock’s trading dates >= pt_date
  dts <- crsp_daily[.(permno)][date >= pt_date, head(date, h)]
  .(date = dts, weight = 1.0)     # equal-weight; if multiple events overlap, they both appear
}, by = .(pt_date, permno, quintile)]

# Merge in daily returns
holdings <- merge(holdings, crsp_daily[, .(permno, date, ret)], by = c("permno","date"), all.x = TRUE)

# If you want to exclude the announcement day return, ensure your construction starts from the **next** trading day.








##############################
# Regressions: EPS (1y)
##############################


df_regression <- df_eps_1y %>% filter(after_earning_pt < 30, before_earning_pt < 60) %>% mutate(Post = ifelse(et >= as.Date("2018-01-01"),1,0), bucket = dplyr::ntile(eps_linear, 5), direction = ifelse(eps_act - eps_est > 0,1,0), revision_eps_1y = 100*revision_eps_1y)
df_regression$bucket <- factor(df_regression$bucket, levels = 1:5)

# Create buckets for time of revision
df_regression <- df_regression %>% mutate(after_earning_pt_bucket = case_when(after_earning_pt %in% c(0) ~ "0", after_earning_pt %in% 1:5 ~ "1-5", after_earning_pt %in% 6:10 ~ "6-10", after_earning_pt %in% 11:20 ~ "11-20",
                                                                              after_earning_pt %in% 21:30 ~ "21-30", TRUE ~ NA_character_), after_earning_pt_bucket = factor(after_earning_pt_bucket, levels = c("0","1-5","6-10","11-20","21-30"))) %>% filter(!is.na(after_earning_pt_bucket))
# Transform variables
vars_ret   <- c("cum_ret_252d", "cum_ret_126d", "cum_ret_22d", "cum_ret_1d", "fwd_ret_1d", "fwd_ret_5d", "fwd_ret_22d", "fwd_ret_126d", "fwd_ret_252d")
vars_level <- c("vol", "marketcap_x", "prc")

df_regression <- log_vars(df_regression, vars_ret, add_one = TRUE, suffix = "_log")
df_regression <- log_vars(df_regression, vars_level, add_one = FALSE, suffix = "_log")

# Handle infinite values
for (v in intersect(paste0(union(vars_ret, vars_level), "_log"), names(df_regression))) {
  bad <- !is.finite(df_regression[[v]])  # Inf, -Inf, NaN
  df_regression[[v]][bad] <- NA
}

# Winsorization
cols_to_winsor <- union(paste0(union(vars_ret, vars_level), "_log"), c("revision_eps_1y", "eps_linear", "eps_beat", "eps_act", "eps_est"))

df_regression <- df_regression %>% mutate(across(.cols = all_of(make.names(cols_to_winsor)), .fns  = ~ winsorize_vec(.x, probs = c(0.01, 0.99))))

# Standardization
df_regression <- standardise_vars(df_regression, setdiff(cols_to_winsor, "revision_eps_1y"))


eps1 <- felm(revision_eps_1y ~ eps_linear | 0 | 0 | permno + et, data = df_regression)
summary(eps1)

eps2 <- felm(revision_eps_1y ~ eps_linear +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(eps2)

eps3 <- felm(revision_eps_1y ~ eps_linear + eps_beat +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(eps3)

eps4 <- felm(revision_eps_1y ~ eps_linear + eps_beat + prc_log + eps_act +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(eps4)

eps5 <- felm(revision_eps_1y ~ eps_linear + eps_beat + prc_log + eps_est +
               cum_ret_252d_log + cum_ret_126d_log + cum_ret_22d_log + cum_ret_1d_log + vol_log + marketcap_x_log | permno + et + amaskcd | 0 | permno + et, data = df_regression)
summary(eps5)







# --- Helper: compute mean and NW t-stat for a return series ---
mean_and_t <- function(x, nw_lags = 6) {
  x <- x[is.finite(x)]
  if (length(x) < 3) return(c(mean = NA_real_, t = NA_real_))
  # Mean via intercept-only regression, NW standard errors
  fit <- lm(x ~ 1)
  se  <- sqrt(vcovHAC(fit, order = nw_lags)[1,1])
  tval <- coef(fit)[1] / se
  c(mean = mean(x, na.rm = TRUE), t = as.numeric(tval))
}

# --- Main: build buckets & long-short (same logic as before), now with paper summary ---
build_long_short_with_summary <- function(df,
                                          stock_col   = "permno",
                                          date_col    = "date",
                                          signal_col  = "eps_beat",
                                          ret_col     = "fwd_return_5d",
                                          percentile  = 0.20,      # 0.2 = quintiles
                                          agg         = c("median","mean"),
                                          min_stocks  = 20,
                                          nw_lags     = 6) {       # Newey–West lags for monthly data
  agg <- match.arg(agg)
  stopifnot(percentile > 0, percentile <= 0.5)
  K <- round(1 / percentile)
  
  df_local <- df %>%
    mutate(
      .stock = .data[[stock_col]],
      .date  = as.Date(.data[[date_col]]),
      .sig   = .data[[signal_col]],
      .ret   = .data[[ret_col]]
    ) %>%
    select(.stock, .date, .sig, .ret) %>%
    filter(!is.na(.stock), !is.na(.date), is.finite(.sig), is.finite(.ret)) %>%
    mutate(
      obs_month  = floor_date(.date, "month"),
      form_month = obs_month + months(1)  # use previous month’s observations
    )
  
  # Aggregate signal within the observation month per stock
  stock_month <- df_local %>%
    group_by(obs_month, .stock) %>%
    summarise(
      sig_agg = if (agg == "median") median(.sig, na.rm = TRUE) else mean(.sig, na.rm = TRUE),
      # average of subsequent returns per stock across obs in that month
      ret_avg = mean(.ret, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(form_month = obs_month + months(1))
  
  # Bucket by aggregated signal within each formation month
  buckets <- stock_month %>%
    group_by(form_month) %>%
    filter(n() >= min_stocks) %>%
    mutate(bucket = ntile(sig_agg, K)) %>%            # 1 = worst ... K = best
    ungroup()
  
  # Equal-weight return per bucket per month
  buckets_ts <- buckets %>%
    group_by(form_month, bucket) %>%
    summarise(ew_return = mean(ret_avg, na.rm = TRUE), .groups = "drop") %>%
    arrange(form_month, bucket) %>%
    rename(month = form_month)
  
  # Long/short and L-S each month
  legs_ts <- buckets_ts %>%
    group_by(month) %>%
    summarise(
      long  = ew_return[bucket == K] %>% ifelse(length(.) == 0, NA_real_, .),
      short = ew_return[bucket == 1] %>% ifelse(length(.) == 0, NA_real_, .),
      long_short = long - short,
      .groups = "drop"
    )
  
  # --- Paper-style table: average returns by bucket + long-short ---
  # Per-bucket means & t-stats
  per_bucket <- buckets_ts %>%
    group_by(bucket) %>%
    summarise(
      mean  = mean(ew_return, na.rm = TRUE),
      t     = mean_and_t(ew_return, nw_lags = nw_lags)["t"],
      .groups = "drop"
    ) %>%
    mutate(bucket = as.integer(bucket))
  
  # Long, Short, and L-S summary (means & NW t-stats)
  ls_summary <- tibble(
    portfolio = c("Short (1)", "Long (K)", "Long–Short (K-1)"),
    mean = c(
      mean(buckets_ts$ew_return[buckets_ts$bucket == 1], na.rm = TRUE),
      mean(buckets_ts$ew_return[buckets_ts$bucket == K], na.rm = TRUE),
      mean(legs_ts$long_short, na.rm = TRUE)
    ),
    t = c(
      mean_and_t(buckets_ts$ew_return[buckets_ts$bucket == 1], nw_lags)["t"],
      mean_and_t(buckets_ts$ew_return[buckets_ts$bucket == K], nw_lags)["t"],
      mean_and_t(legs_ts$long_short, nw_lags)["t"]
    )
  )
  
  # Nicely formatted table (bucket 1..K, then LS line)
  table_paper <- per_bucket %>%
    mutate(Bucket = paste0("Q", bucket)) %>%
    select(Bucket, Mean = mean, `t-Stat (NW)` = t) %>%
    bind_rows(
      tibble(Bucket = "Long–Short",
             Mean = ls_summary$mean[ls_summary$portfolio == "Long–Short (K-1)"],
             `t-Stat (NW)` = ls_summary$t[ls_summary$portfolio == "Long–Short (K-1)"])
    )
  
  # Also provide annualized means (assuming monthly data)
  ann_factor <- 12
  table_paper_annualized <- table_paper %>%
    mutate(`Mean (ann.)` = Mean * ann_factor) %>%
    relocate(`Mean (ann.)`, .after = Mean)
  
  list(
    buckets_ts = buckets_ts,       # time series of bucket returns
    legs_ts    = legs_ts,          # time series of long/short/L-S
    table_paper = table_paper,     # monthly means & t-stats by bucket + L-S
    table_paper_annualized = table_paper_annualized # annualized means + t-stats
  )
}

# ---------------------------
# Example usage
# ---------------------------
res <- build_long_short_with_summary(
  df             = df_regression,
  stock_col      = "permno",
  date_col       = "et",           # change if your column differs
  signal_col     = "cum_ret_5d",
  ret_col        = "fwd_ret_22d",  # your forward return
  percentile     = 0.20,             # quintiles
  agg            = "mean",         # or "mean"
  min_stocks     = 20,
  nw_lags        = NULL                 # NW lags for monthly data
)

# Paper-style tables:
res$table_paper
res$table_paper_annualized

# If you want deciles, set percentile = 0.10 (K=10).

colnames(df_regression)





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
  data        = df_regression,
  date_col    = et,        # or trade_date
  test_var    = eps_linear,     # X (e.g., PT upside)
  control_var = eps_beat,         # Y (e.g., log mktcap)
  outcome     = revision_eps_1y,      # e.g., next-252d return
  weight      = NULL,    # NULL for equal-weight
  n_x = 5, n_y = 5,
  agg = "mean",
  axis_agg = "mean",
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
  data    = df_regression,
  date_col= et,   # your daily date column
  formula = revision_eps_1y ~ eps_beat + eps_linear + cum_ret_252d + cum_ret_126d + cum_ret_22d + cum_ret_1d,
  weight  = NULL,              # or "marketcap" for WLS
  nw_lag  = 5,                 # ~ one trading week
  min_n   = NULL               # defaults to (#pred+1)
)

print(res_daily, n = Inf, width = Inf)






