# =============================================================================
#
# SCRIPT: Create Parkinson's Disease Analysis-Ready Dataset
#
# DESCRIPTION:
#   This script is a complete, production-ready ETL pipeline that processes
#   raw, multi-year Medicare claims data. It identifies the first-ever
#   hospitalization for Parkinson's Disease (PD) for a cohort of continuously
#   enrolled beneficiaries, integrates this event data with their longitudinal
#   follow-up records, and aggregates the final data into a format suitable
#   for a Cox-equivalent Poisson model.
#
# WORKFLOW:
#   PART 1: Identify First-Ever PD Hospitalization
#     1.1: Load all annual raw admission files.
#     1.2: Filter admissions to keep only PD-related hospitalizations.
#     1.3: Load annual cohort enrollment files to identify valid person-years.
#     1.4: Merge admissions with enrollment to keep only valid events.
#     1.5: For each patient, identify the date of their first-ever PD hospitalization.
#
#   PART 2: Create Aggregated Person-Year Dataset for Modeling
#     2.1: Merge the first-event data back into the full person-year cohort data.
#     2.2: Apply follow-up rules, calculate person-time, and flag incident events.
#     2.3: Aggregate the person-year data into modeling strata (by demo, geo, etc.).
#     2.4: Merge the aggregated data with external covariate datasets (e.g., SES).
#     2.5: Save the final, analysis-ready dataset.
#
# =============================================================================


# =============================================================================
# 0. SETUP & CONFIGURATION
# =============================================================================

# Auto-install and load required packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  fst,         # For fast data reading
  data.table,  # For high-performance data manipulation
  lubridate    # For easy date handling
)

# Centralized configuration list for all analysis parameters.
CONFIG <- list(
  years_to_process = 2000:2016,
  paths = list(
    admissions_dir = "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/targeted_conditions/cache_data/admissions_by_year/",
    cohort_dir     = "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/Klompmaker/cache_data/correct_follow_up_age_entry2/",
    covariates     = "/nfs/home/J/jok8845/shared_space/ci3_analysis/nature_medicare/data/confounders/merged_covariates.fst",
    regions        = "/nfs/home/J/jok8845/shared_space/ci3_analysis/medicare_temperature_humidity/data/USregions/zip_regions.fst",
    output_dir     = "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/Klompmaker/analysis_ready_data/pd/"
  ),
  file_templates = list(
    admissions_prefix = "admissions_",
    cohort_prefix     = "confounder_exposure_merged_nodups_health_"
  ),
  columns = list(
    admissions = c("QID", "ADATE", "DDATE", "Parkinson_pdx2dx_10"),
    cohort     = c("zip", "age", "year", "qid", "statecode", "race", "sex",
                   "dual", "entry_age.y", "entry_year.y", "entry_age_break.y")
  ),
  excluded_states = c("AK", "PR", "HI", "VI", "MP", "GU", "MH", "AS", "FM", "PW", "")
)


# =============================================================================
# 1. HELPER FUNCTIONS
# =============================================================================

#' Programmatically load and combine multiple annual .fst files.
load_yearly_data <- function(base_path, years, file_prefix, columns_to_keep) {
  message(sprintf("Loading data from %s...", base_path))
  all_files <- lapply(years, function(year) {
    file_path <- file.path(base_path, paste0(file_prefix, year, ".fst"))
    if (file.exists(file_path)) {
      dt <- fst::read_fst(file_path, columns = columns_to_keep, as.data.table = TRUE)
      message(sprintf("  - Loaded %s: %d rows", basename(file_path), nrow(dt)))
      return(dt)
    } else {
      warning(sprintf("File not found and will be skipped: %s", file_path))
      return(NULL)
    }
  })
  combined_dt <- data.table::rbindlist(all_files, fill = TRUE)
  message(sprintf("Total rows loaded: %d", nrow(combined_dt)))
  return(combined_dt)
}


# =============================================================================
# PART 1: IDENTIFY FIRST-EVER PD HOSPITALIZATION
# =============================================================================

#' Main function to execute Part 1 of the pipeline.
run_first_event_etl <- function(cfg) {
  message("\n--- STARTING PART 1: IDENTIFYING FIRST-EVER PD HOSPITALIZATION ---")

  # --- Step 1.1: Load and Process Raw Admissions Data ---
  admissions_dt <- load_yearly_data(
    base_path = cfg$paths$admissions_dir,
    years = cfg$years_to_process,
    file_prefix = cfg$file_templates$admissions_prefix,
    columns_to_keep = cfg$columns$admissions
  )
  admissions_dt[, `:=`(ADATE = lubridate::dmy(ADATE), DDATE = lubridate::dmy(DDATE), year = lubridate::year(ADATE))]
  
  # Filter to keep only PD-related hospitalizations
  admissions_dt <- admissions_dt[Parkinson_pdx2dx_10 == TRUE]
  message(sprintf("Filtered to %d PD-related hospitalizations.", nrow(admissions_dt)))
  admissions_dt[, Parkinson_pdx2dx_10 := NULL]

  # --- Step 1.2: Load Cohort Enrollment Data ---
  cohort_dt <- load_yearly_data(
    base_path = cfg$paths$cohort_dir,
    years = cfg$years_to_process,
    file_prefix = cfg$file_templates$cohort_prefix,
    columns_to_keep = cfg$columns$cohort
  )
  setnames(cohort_dt, "qid", "QID")

  # --- Step 1.3: Filter Admissions to Valid Person-Years ---
  n_admissions_before <- nrow(admissions_dt)
  valid_admissions_dt <- merge(admissions_dt, cohort_dt, by = c("QID", "year"))
  n_admissions_after <- nrow(valid_admissions_dt)
  message(sprintf("Kept %d valid admissions. Dropped %d admissions outside of valid follow-up.",
                  n_admissions_after, n_admissions_before - n_admissions_after))

  # --- Step 1.4: Identify First-Ever Hospitalization for Each Person ---
  setkey(valid_admissions_dt, QID, ADATE)
  first_hosp_dt <- valid_admissions_dt[, .SD[1], by = QID]
  message(sprintf("Identified %d unique patients with a first-ever PD hospitalization.", nrow(first_hosp_dt)))
  
  return(first_hosp_dt)
}


# =============================================================================
# PART 2: CREATE AGGREGATED PERSON-YEAR DATASET FOR MODELING
# =============================================================================

#' Main function to execute Part 2 of the pipeline.
run_aggregation_etl <- function(first_events_dt, cfg) {
  message("\n--- STARTING PART 2: CREATING AGGREGATED ANALYSIS DATASET ---")
  
  # --- Step 2.1: Load full cohort person-year data ---
  person_years_dt <- load_yearly_data(
    base_path = cfg$paths$cohort_dir,
    years = cfg$years_to_process,
    file_prefix = cfg$file_templates$cohort_prefix,
    columns_to_keep = cfg$columns$cohort
  )
  setnames(person_years_dt, "year", "year_medicare")
  person_years_dt[, qid := as.character(qid)]
  
  # --- Step 2.2: Merge first-event data into person-year data ---
  events_to_merge <- first_events_dt[, .(qid = as.character(QID),
                                         pd_hosp = 1L,
                                         pd_hosp_date = ADATE,
                                         year_hosp = year(ADATE))]
  
  person_years_dt <- merge(person_years_dt, events_to_merge, by = "qid", all.x = TRUE)
  
  # --- Step 2.3: Apply follow-up rules and define incident events ---
  setorder(person_years_dt, qid, year_medicare)
  
  # Censor person-years that occur after the first event
  person_years_dt <- person_years_dt[is.na(year_hosp) | year_hosp >= year_medicare]
  
  # Create a binary indicator for an incident hospitalization in a given year
  person_years_dt[, incident_hosp := fifelse(!is.na(pd_hosp) & year_hosp == year_medicare, 1L, 0L)]
  person_years_dt[is.na(incident_hosp), incident_hosp := 0L]
  
  person_years_dt[, followup_year := seq_len(.N), by = qid]
  
  # --- Step 2.4: Aggregate person-years into modeling strata ---
  person_years_dt[, race_cat := fcase(race == 1, 1L, race == 2, 2L, default = 3L)]
  person_years_dt[, person_time := 1] # Each row represents one person-year
  
  agg_dt <- person_years_dt[, .(
    hosp_count = sum(incident_hosp),
    time_count = sum(person_time)
  ), by = .(sex, race_cat, entry_age_break.y, dual, followup_year, zip, year_medicare)]
  
  setnames(agg_dt,
           old = c("race_cat", "entry_age_break.y", "year_medicare"),
           new = c("race", "age_entry", "year"))
  
  message(sprintf("Aggregated %d person-year records into %d unique strata.",
                  nrow(person_years_dt), nrow(agg_dt)))
  
  # --- Step 2.5: Merge with external covariate datasets ---
  covar_dt <- read_fst(cfg$paths$covariates, as.data.table = TRUE)
  region_dt <- read_fst(cfg$paths$regions, columns = c("zip", "region"), as.data.table = TRUE)
  
  final_dt <- merge(agg_dt, covar_dt, by = c("zip", "year"), all.x = TRUE)
  final_dt <- merge(final_dt, region_dt, by = "zip", all.x = TRUE)
  
  return(final_dt)
}


# =============================================================================
# 3. MAIN EXECUTION
# =============================================================================

#' Main orchestrator function to run the full pipeline.
run_pipeline <- function(cfg) {
  
  # Ensure output directory exists
  dir.create(cfg$paths$output_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Execute Part 1
  first_events <- run_first_event_etl(cfg)
  
  # Execute Part 2
  analysis_ready_dt <- run_aggregation_etl(first_events, cfg)
  
  # Save the final, analysis-ready dataset
  output_path <- file.path(cfg$paths$output_dir, "pd_analysis_ready_aggregated.fst")
  write_fst(analysis_ready_dt, output_path)
  message(sprintf("\nSUCCESS: Final analysis-ready dataset saved to %s", output_path))
  
  # Clean up memory
  gc()
}

# This standard R construct ensures the code only runs when the script is
# executed directly (e.g., via Rscript), not when sourced.
if (sys.nframe() == 0) {
  run_pipeline(CONFIG)
}