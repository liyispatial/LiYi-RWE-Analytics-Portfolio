# =============================================================================
#
# SCRIPT: IPW Analysis for Longitudinal Exposure Effects
#
# DESCRIPTION: This script performs an end-to-end causal inference analysis
#              using Inverse Probability Weighting (IPW) to fit a Marginal
#              Structural Model (MSM). It estimates the causal effect of
#              long-term greenspace exposure on adiposity outcomes and includes
#              a comprehensive sensitivity analysis across different exposure
#              definitions.
#
# WORKFLOW:
#   1.  Load and preprocess raw data.
#   2.  Define and calculate inverse probability weights for censoring and exposure.
#   3.  Combine and truncate weights to create final stabilized weights.
#   4.  Loop through all exposures and outcomes, running a bootstrapped MSM for each.
#   5.  Summarize and visualize all results in a publication-quality forest plot.
#
# =============================================================================


# =============================================================================
# 0. SETUP & CONFIGURATION
# =============================================================================

# Auto-install and load required packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  haven,       # For reading SAS files
  tidyverse,   # For data manipulation (dplyr, ggplot2, etc.)
  janitor,     # For cleaning variable names
  gtsummary,   # For descriptive tables
  boot         # For bootstrapping
)

# Centralized configuration list for all analysis parameters.
# This makes the script reusable and easy to modify.
CONFIG <- list(
  paths = list(
    raw_data = "ly_120624.sas7bdat" # Assumes data is in the same directory
  ),
  variables = list(
    id_var = "aid",
    # List all exposure prefixes here for the main analysis and sensitivity analyses.
    exposures = c(
      "greenspace_100", "greenspace_500", "greenspace_1000",
      "trees_100", "trees_500", "trees_1000",
      "grass_500", "othgreen_500",
      "ndvi_270", "ndvi_1230"
    ),
    outcomes = c("bmi_17y", "waist_circum_17y", "tot_fat_mass_17y", "tot_fmi_17y", "trunk_fmi_17y"),
    outcome_shortnames = c("bmi", "wcir", "bfp", "tfmi", "wfmi"),
    baseline_covars = "re_white + age_mom_enroll_d + bmi_mom_prepreg_d + coll_grad",
    L0_covars = "hhincome_0y + medhhinc_2000_y0_q3 + pop_sqkm_2000_y0_q3",
    L1_covars = "hhincome_7y + medhhinc_2000_y7_q3 + pop_sqkm_2000_y7_q3",
    L2_covars = "hhincome_12y + medhhinc_2010_y12_q3 + pop_sqkm_2010_y12_q3"
  ),
  bootstrap = list(
    n_replicates = 100, # Set to a higher number (e.g., 500-1000) for final results
    seed_start = 123
  )
)


# =============================================================================
# 1. DATA PREPARATION & ETL
# =============================================================================

#' Load and preprocess the raw SAS data into an analysis-ready format.
#' @param path File path to the raw SAS dataset.
#' @return A preprocessed tibble.
load_and_prep_data <- function(path) {
  # Read and perform initial cleaning
  dt_raw <- haven::read_sas(path)

  dt <- dt_raw %>%
    janitor::clean_names() %>%
    # Select only necessary columns to keep the dataset lean
    select(
      aid, age_mom_enroll_d, re_momc, bmi_mom_prepreg_d, coll_grad, married_cohab,
      mom_bmi_ma17, waist_cm_ma17, wbtot_fat_mdxa17, wbtot_pfat_mdxa17, trunk_fat_mdxa17,
      height_cm_ma17, gt70k, starts_with("hincome"), starts_with("medhhinc"),
      starts_with("pop_sqkm"), starts_with("ndvi"), starts_with("greenspace"),
      starts_with("trees"), starts_with("grass"), starts_with("othgreen"),
      starts_with("sidewalk")
    ) %>%
    # Standardize timepoint suffixes for consistency
    rename_with(~ gsub("birth$", "0y", .x)) %>%
    rename_with(~ gsub("year7$", "7y", .x)) %>%
    rename_with(~ gsub("year12$", "12y", .x)) %>%
    rename_with(~ gsub("mt$", "17y", .x)) %>%
    # Derive time-varying income variables and outcomes
    mutate(
      hhincome_3y = if_else(hincome_tyq %in% 1:5, 0, 1),
      hhincome_7y = if_else(hincome_qu7y %in% 1:4, 0, 1),
      hhincome_12y = if_else(hincome_qu12 %in% 1:4, 0, 1),
      hhincome_17y = if_else(hincome_qu17 %in% 1:2, 0, 1),
      tot_fat_mass_17y = wbtot_pfat_mdxa17,
      tot_fmi_17y = wbtot_fat_mdxa17 / (height_cm_ma17^2),
      trunk_fmi_17y = trunk_fat_mdxa17 / (height_cm_ma17^2),
      bmi_mom_prepreg_d = if_else(bmi_mom_prepreg_d >= 25, 1, 0)
    ) %>%
    # Standardize final variable names
    rename(
      waist_circum_17y = waist_cm_ma17,
      bmi_17y = mom_bmi_ma17,
      hhincome_0y = gt70k
    ) %>%
    # Create categorical versions of exposures and confounders
    mutate(
      across(
        .cols = c(starts_with("ndvi"), starts_with("greenspace"), starts_with("trees"),
                  starts_with("grass"), starts_with("othgreen"), starts_with("sidewalk")),
        .fns = list(
          q4 = ~ cut(.x, breaks = quantile(.x, probs = 0:4/4, na.rm = TRUE), include.lowest = TRUE, labels = FALSE),
          q2 = ~ as.integer(.x > quantile(.x, probs = 0.75, na.rm = TRUE))
        ),
        .names = "{.col}_{.fn}"
      ),
      across(
        .cols = c(starts_with("medhhinc"), starts_with("pop_sqkm")),
        .fns = list(
          q3 = ~ cut(.x, breaks = quantile(.x, probs = 0:3/3, na.rm = TRUE), include.lowest = TRUE, labels = FALSE)
        ),
        .names = "{.col}_{.fn}"
      )
    )

  return(dt)
}


# =============================================================================
# 2. CAUSAL INFERENCE: IPW & MSM FUNCTIONS
# =============================================================================

#' Calculate Inverse Probability of Censoring Weights (IPCW) for all outcomes.
#' @param data The analysis dataframe.
#' @param cfg The configuration list.
#' @return A dataframe with participant ID and IPCWs for each outcome.
calculate_censor_weights <- function(data, cfg) {
  
  V <- cfg$variables$baseline_covars
  L_covars <- c(cfg$variables$L0_covars, cfg$variables$L1_covars, cfg$variables$L2_covars)
  
  # Create censoring indicators for exposures/covariates at each wave
  data <- data %>%
    mutate(
      censor1 = if_else(if_any(all_of(c("greenspace_500_7y", "hhincome_7y", "medhhinc_2000_y7", "pop_sqkm_2000_y7"))), ~ is.na(.), 1L, 0L),
      censor2 = if_else(censor1 == 1L | if_any(all_of(c("greenspace_500_12y", "hhincome_12y", "medhhinc_2010_y12", "pop_sqkm_2010_y12"))), ~ is.na(.), 1L, 0L)
    )
  
  # Loop through each outcome to create outcome-specific censoring and weights
  outcome_weights <- map_dfc(cfg$variables$outcomes, function(outcome_var) {
    censor3_var <- paste0("censor3_", outcome_var)
    data[[censor3_var]] <- if_else(data$censor2 == 1L | is.na(data[[outcome_var]]), 1L, 0L)
    
    # Define formulas for numerator and denominator models
    formula_c1_den <- as.formula(paste("censor1 == 0 ~", V, "+", L_covars))
    formula_c2_den <- as.formula(paste("censor2 == 0 ~", V, "+", L_covars, "+", L_covars))
    formula_c3_den <- as.formula(paste(censor3_var, "== 0 ~", V, "+", paste(L_covars, collapse=" + ")))
    
    # Numerator models for stabilized weights only adjust for baseline confounders
    formula_c1_num <- as.formula("censor1 == 0 ~ 1")
    formula_c2_num <- as.formula("censor2 == 0 ~ 1")
    formula_c3_num <- as.formula(paste(censor3_var, "== 0 ~ 1"))
    
    # Fit models
    fit_c1_den <- glm(formula_c1_den, data = data, family = binomial)
    fit_c2_den <- glm(formula_c2_den, data = data, family = binomial)
    fit_c3_den <- glm(formula_c3_den, data = data, family = binomial)
    fit_c1_num <- glm(formula_c1_num, data = data, family = binomial)
    fit_c2_num <- glm(formula_c2_num, data = data, family = binomial)
    fit_c3_num <- glm(formula_c3_num, data = data, family = binomial)
    
    # Calculate cumulative probabilities and weights
    tibble(
      den_p1 = predict(fit_c1_den, type = "response"),
      den_p2 = predict(fit_c2_den, type = "response"),
      den_p3 = predict(fit_c3_den, type = "response"),
      num_p1 = predict(fit_c1_num, type = "response"),
      num_p2 = predict(fit_c2_num, type = "response"),
      num_p3 = predict(fit_c3_num, type = "response")
    ) %>%
      mutate(
        cw_d = if_else(data$censor1 == 0, den_p1, 1) *
               if_else(data$censor2 == 0, den_p2, 1) *
               if_else(data[[censor3_var]] == 0, den_p3, 1),
        cw_n = if_else(data$censor1 == 0, num_p1, 1) *
               if_else(data$censor2 == 0, num_p2, 1) *
               if_else(data[[censor3_var]] == 0, num_p3, 1)
      ) %>%
      select(cw_d, cw_n) %>%
      rename_with(~ paste0(., "_", sub("_.*", "", outcome_var)))
  })
  
  bind_cols(select(data, aid), outcome_weights)
}

#' Calculate Inverse Probability of Treatment Weights (IPTW).
#' @param data The analysis dataframe.
#' @param exposure_prefix The prefix for the exposure variable (e.g., "greenspace_500").
#' @param cfg The configuration list.
#' @return A dataframe with participant ID and IPTWs.
calculate_expo_weights <- function(data, exposure_prefix, cfg) {
  
  V <- cfg$variables$baseline_covars
  L0 <- cfg$variables$L0_covars
  L1 <- cfg$variables$L1_covars
  L2 <- cfg$variables$L2_covars
  
  v <- paste0(exposure_prefix, c("_0y_q2", "_7y_q2", "_12y_q2"))
  
  # Numerator models (depend only on past exposure)
  fit_exp_num_0y <- glm(as.formula(paste(v, "~ 1")), data = data, family = binomial)
  fit_exp_num_7y <- glm(as.formula(paste(v, "~", v)), data = data, family = binomial)
  fit_exp_num_12y <- glm(as.formula(paste(v, "~", v, "+", v)), data = data, family = binomial)
  
  # Denominator models (depend on past exposure and confounders)
  fit_exp_den_0y <- glm(as.formula(paste(v, "~", V, "+", L0)), data = data, family = binomial)
  fit_exp_den_7y <- glm(as.formula(paste(v, "~", V, "+", L0, "+", L1, "+", v)), data = data, family = binomial)
  fit_exp_den_12y <- glm(as.formula(paste(v, "~", V, "+", L0, "+", L1, "+", L2, "+", v, "+", v)), data = data, family = binomial)
  
  # Calculate cumulative probabilities and weights
  data %>%
    mutate(
      p0_den = predict(fit_exp_den_0y, type = "response"),
      p7_den = predict(fit_exp_den_7y, type = "response"),
      p12_den = predict(fit_exp_den_12y, type = "response"),
      p0_num = predict(fit_exp_num_0y, type = "response"),
      p7_num = predict(fit_exp_num_7y, type = "response"),
      p12_num = predict(fit_exp_num_12y, type = "response"),
      
      ew_n = if_else(.data[[v]] == 1, p0_num, 1 - p0_num) *
             if_else(.data[[v]] == 1, p7_num, 1 - p7_num) *
             if_else(.data[[v]] == 1, p12_num, 1 - p12_num),
      
      ew_d = if_else(.data[[v]] == 1, p0_den, 1 - p0_den) *
             if_else(.data[[v]] == 1, p7_den, 1 - p7_den) *
             if_else(.data[[v]] == 1, p12_den, 1 - p12_den)
    ) %>%
    select(aid, ew_n, ew_d)
}

#' Combine censoring and exposure weights and perform truncation.
#' @param data The analysis dataframe.
#' @param censor_weights_df Dataframe of censor weights.
#' @param expo_weights_df Dataframe of exposure weights.
#' @param cfg The configuration list.
#' @return A dataframe with final, truncated stabilized weights for each outcome.
create_final_weights <- function(data, censor_weights_df, expo_weights_df, cfg) {
  
  full_data <- data %>%
    left_join(censor_weights_df, by = "aid") %>%
    left_join(expo_weights_df, by = "aid")
  
  # Calculate stabilized weights and truncate them for each outcome
  for (s_name in cfg$variables$outcome_shortnames) {
    cw_n_col <- paste0("cw_n_", s_name)
    cw_d_col <- paste0("cw_d_", s_name)
    stabw_col <- paste0("stabw_", s_name)
    trunc_col <- paste0(stabw_col, "_trunc")
    
    # Calculate stabilized weight
    full_data <- full_data %>%
      mutate(!!stabw_col := (!!sym(cw_n_col) * ew_n) / (!!sym(cw_d_col) * ew_d))
    
    # Calculate truncation limits (ignoring NAs and non-positives)
    valid_weights <- full_data[[stabw_col]][!is.na(full_data[[stabw_col]]) & full_data[[stabw_col]] > 0]
    lo <- quantile(valid_weights, 0.01, na.rm = TRUE)
    hi <- quantile(valid_weights, 0.99, na.rm = TRUE)
    
    # Apply truncation
    full_data <- full_data %>%
      mutate(!!trunc_col := case_when(
        is.na(!!sym(stabw_col)) ~ NA_real_,
        !!sym(stabw_col) < lo ~ lo,
        !!sym(stabw_col) > hi ~ hi,
        TRUE ~ !!sym(stabw_col)
      ))
  }
  
  return(full_data)
}

#' Run the MSM and bootstrap procedure for a single exposure and outcome.
#' @param data The weighted dataframe.
#' @param exposure_prefix The exposure to model.
#' @param outcome_var The outcome to model.
#' @param outcome_s_name The short name for the outcome (for weight column).
#' @param cfg The configuration list.
#' @return A boot object containing the bootstrap results.
run_msm_bootstrap <- function(data, exposure_prefix, outcome_var, outcome_s_name, cfg) {
  
  weight_col <- paste0("stabw_", outcome_s_name, "_trunc")
  expo_vars <- paste0(exposure_prefix, c("_0y_q4", "_7y_q4", "_12y_q4"))
  msm_formula <- as.formula(paste(outcome_var, "~", paste(expo_vars, collapse = " + ")))
  
  # Ensure exposure variables are factors for the model
  data <- data %>%
    mutate(across(all_of(expo_vars), ~factor(., levels = 1:4)))
  
  # Define the statistic function for bootstrapping
  msm_statistic <- function(d, indices) {
    boot_data <- d[indices, ]
    
    # Fit the MSM on the bootstrap sample
    msm_fit <- glm(msm_formula, data = boot_data, weights = boot_data[[weight_col]])
    
    # Create counterfactual datasets
    arm1 <- boot_data %>% mutate(across(all_of(expo_vars), ~factor(4, levels = 1:4))) # High exposure
    arm2 <- boot_data %>% mutate(across(all_of(expo_vars), ~factor(1, levels = 1:4))) # Low exposure
    
    # Predict outcomes under each counterfactual
    y1 <- predict(msm_fit, newdata = arm1)
    y2 <- predict(msm_fit, newdata = arm2)
    
    # Return the difference in means (Average Treatment Effect)
    return(mean(y1, na.rm = TRUE) - mean(y2, na.rm = TRUE))
  }
  
  # Run the bootstrap
  boot_results <- boot::boot(
    data = data,
    statistic = msm_statistic,
    R = cfg$bootstrap$n_replicates
  )
  
  return(boot_results)
}


# =============================================================================
# 3. MAIN ANALYSIS WORKFLOW
# =============================================================================

# Step 1: Load and preprocess the data
full_dt <- load_and_prep_data(CONFIG$paths$raw_data)

# Step 2: Create the analysis-ready cohort (complete baseline data)
analysis_dt <- full_dt %>%
  filter(if_all(all_of(c("greenspace_500_0y", "age_mom_enroll_d", "re_momc", 
                         "bmi_mom_prepreg_d", "coll_grad", "hhincome_0y", 
                         "medhhinc_2000_y0", "pop_sqkm_2000_y0")), ~ !is.na(.x))) %>%
  mutate(
    re_white = if_else(re_momc == "White", 1, 0)
  )

# Step 3: Calculate censoring weights (done once for all analyses)
message("Calculating censoring weights...")
censor_weights <- calculate_censor_weights(analysis_dt, CONFIG)

# Step 4: Loop through all exposures to run sensitivity analyses
all_results <- list()

for (expo in CONFIG$variables$exposures) {
  
  message(paste("Processing exposure:", expo))
  
  # Calculate exposure-specific weights
  expo_weights <- calculate_expo_weights(analysis_dt, expo, CONFIG)
  
  # Combine and truncate all weights
  weighted_dt <- create_final_weights(analysis_dt, censor_weights, expo_weights, CONFIG)
  
  # Loop through outcomes to run the MSM for this exposure
  for (i in seq_along(CONFIG$variables$outcomes)) {
    outcome <- CONFIG$variables$outcomes[i]
    s_name <- CONFIG$variables$outcome_shortnames[i]
    
    message(paste("  - Modeling outcome:", outcome))
    
    # Run bootstrap
    boot_res <- run_msm_bootstrap(weighted_dt, expo, outcome, s_name, CONFIG)
    
    # Store results
    result_key <- paste(expo, s_name, sep = "_")
    all_results[[result_key]] <- boot_res
  }
}

# Step 5: Summarize and Tidy Results for Visualization
message("\n--- Summarizing Bootstrap Results ---")
msm_results_df <- purrr::map_dfr(names(all_results), function(res_name) {
  boot_obj <- all_results[[res_name]]
  
  # Calculate 95% confidence intervals using the percentile method
  ci <- tryCatch({
    boot::boot.ci(boot_obj, type = "perc")
  }, error = function(e) { NULL })
  
  # Extract exposure and outcome names from the result key
  name_parts <- stringr::str_split_fixed(res_name, "_", 2)
  exposure_name <- name_parts
  outcome_s_name <- name_parts
  
  tibble::tibble(
    exposure = exposure_name,
    outcome = outcome_s_name,
    estimate = boot_obj$t0,
    ci_low = if (!is.null(ci)) ci$percent else NA,
    ci_high = if (!is.null(ci)) ci$percent else NA
  )
})

# Print the final results table
print("Final MSM Results:")
print(msm_results_df)


# =============================================================================
# 4. DESCRIPTIVE STATISTICS
# =============================================================================

#' Generate a descriptive summary table (Table 1).
#' @param data The analysis-ready dataframe.
#' @return A gtsummary table object.
generate_table1 <- function(data) {
  data %>%
    select(
      age_mom_enroll_d, re_momc, coll_grad, bmi_mom_prepreg_d,
      hhincome_0y, medhhinc_2000_y0_q3, pop_sqkm_2000_y0_q3,
      greenspace_500_0y_q4
    ) %>%
    tbl_summary(
      by = greenspace_500_0y_q4,
      label = list(
        age_mom_enroll_d ~ "Age at Enrollment",
        re_momc ~ "Race/Ethnicity",
        coll_grad ~ "College Graduate",
        bmi_mom_prepreg_d ~ "Pre-pregnancy Overweight/Obese",
        hhincome_0y ~ "Household Income > $70k",
        medhhinc_2000_y0_q3 ~ "Neighborhood Income (Tertile)",
        pop_sqkm_2000_y0_q3 ~ "Population Density (Tertile)"
      )
    ) %>%
    add_overall() %>%
    modify_header(label ~ "**Baseline Characteristic**")
}

# Example of generating and printing the table
message("\n--- Generating Descriptive Table 1 ---")
table1 <- generate_table1(analysis_dt)
print(table1)


# =============================================================================
# 5. VISUALIZE MAIN RESULTS
# =============================================================================

#' Create a Forest Plot of MSM Results, Grouped for Sensitivity Analyses.
#' @param results_df A tidy data frame of bootstrap results.
#' @return A ggplot object.
plot_msm_forest <- function(results_df) {
  
  # Create more descriptive labels for plotting
  plot_data <- results_df %>%
    mutate(
      # Extract buffer size and exposure type for grouping and labeling
      buffer_size = as.numeric(stringr::str_extract(exposure, "\\d+")),
      exposure_type = stringr::str_remove(exposure, "_\\d+"),
      
      # Create clean, ordered labels for faceting by outcome
      outcome_label = factor(case_when(
        outcome == "bmi"   ~ "BMI (kg/m²)",
        outcome == "wcir"  ~ "Waist Circumference (cm)",
        outcome == "bfp"   ~ "Body Fat Mass (%)",
        outcome == "tfmi"  ~ "Total Fat Mass Index (kg/m²)",
        outcome == "wfmi"  ~ "Trunk Fat Mass Index (kg/m²)",
        TRUE               ~ outcome
      ), levels = c("BMI (kg/m²)", "Waist Circumference (cm)", "Body Fat Mass (%)", 
                    "Total Fat Mass Index (kg/m²)", "Trunk Fat Mass Index (kg/m²)"))
    )
  
  # Generate the plot
  ggplot(plot_data, aes(x = estimate, y = fct_reorder(exposure, buffer_size), xmin = ci_low, xmax = ci_high)) +
    geom_vline(xintercept = 0, linetype = "dashed", color = "grey50") +
    geom_errorbarh(height = 0.2, aes(color = factor(buffer_size))) +
    geom_point(size = 3, shape = 21, aes(fill = factor(buffer_size))) +
    
    # Use facet_grid to create a structured grid of plots
    facet_grid(exposure_type ~ outcome_label, scales = "free_y", space = "free_y", switch = "y") +
    
    labs(
      title = "Sensitivity Analysis: Causal Effect of Greenspace Exposure on Adiposity",
      subtitle = "Comparing different exposure definitions and buffer sizes (e.g., 100m, 500m)",
      x = "Estimated Average Treatment Effect (ATE) with 95% Confidence Interval",
      y = "",
      color = "Buffer Size (m)",
      fill = "Buffer Size (m)"
    ) +
    
    # Apply a clean, professional theme
    theme_bw(base_size = 14) +
    theme(
      panel.grid.major.y = element_blank(),
      panel.spacing = unit(1.0, "lines"),
      strip.text.y.left = element_text(face = "bold", angle = 0),
      strip.text.x = element_text(face = "bold"),
      strip.background = element_rect(fill = "grey90", color = NA),
      plot.title.position = "plot",
      axis.text.y = element_blank(),
      axis.ticks.y = element_blank(),
      legend.position = "top"
    )
}

# --- Create and display the plot ---
message("\n--- Generating Forest Plot of Final Results ---")
forest_plot <- plot_msm_forest(msm_results_df)

# Print the plot to the viewer
print(forest_plot)

# Optionally, save the plot to a file
# ggsave("msm_forest_plot.png", plot = forest_plot, width = 12, height = 14, dpi = 300)