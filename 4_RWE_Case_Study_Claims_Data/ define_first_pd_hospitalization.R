# =============================================================================
#
# SCRIPT: Define First-Ever Parkinson's Disease Hospitalization
#
# DESCRIPTION:
#   This script is a production-ready ETL pipeline that processes raw, multi-year
#   Medicare claims data to identify the first-ever hospitalization for
#   Parkinson's Disease (PD) for a cohort of continuously enrolled beneficiaries.
#
# WORKFLOW:
#   1.  Load all annual raw admission files for a specified date range.
#   2.  Process admissions to identify all PD-related hospitalizations.
#   3.  Load all annual cohort enrollment files.
#   4.  Merge admissions with enrollment data to keep only valid person-years.
#   5.  For each patient, identify the date of their first-ever PD hospitalization.
#   6.  Save the final, analysis-ready dataset.
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
# This makes the script reusable and easy to modify for different projects.
CONFIG <- list(
  years_to_process = 2000:2016,
  paths = list(
    admissions_dir = "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/targeted_conditions/cache_data/admissions_by_year/",
    cohort_dir     = "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/Klompmaker/cache_data/correct_follow_up_age_entry2/",
    output_file    = "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/Klompmaker/admission_data/first_pd_hosp_2000_2016.fst"
  ),
  file_templates = list(
    admissions_prefix = "admissions_",
    cohort_prefix     = "confounder_exposure_merged_nodups_health_"
  ),
  columns = list(
    admissions = c("QID", "ADATE", "DDATE", "Parkinson_pdx2dx_10"),
    cohort     = c("qid", "year")
  )
)


# =============================================================================
# 1. HELPER FUNCTION TO LOAD MULTI-YEAR DATA
# =============================================================================

#' Programmatically load and combine multiple annual .fst files into a single data.table.
#'
#' @param base_path The directory containing the annual files.
#' @param years A numeric vector of years to load (e.g., 2000:2016).
#' @param file_prefix The prefix of the annual files (e.g., "admissions_").
#' @param columns_to_keep A character vector of column names to select.
#'
#' @return A single data.table containing the combined data for all years.
load_yearly_data <- function(base_path, years, file_prefix, columns_to_keep) {
  
  message(sprintf("Loading data from %s...", base_path))
  
  # This loop programmatically generates the file path for each year,
  # reads the data, and stores it in a list. This is robust and scalable.
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
  
  # Efficiently combine all the individual yearly data.tables into one
  combined_dt <- data.table::rbindlist(all_files, fill = TRUE)
  message(sprintf("Total rows loaded: %d", nrow(combined_dt)))
  
  return(combined_dt)
}


# =============================================================================
# 2. MAIN ETL WORKFLOW
# =============================================================================

# --- Step 2.1: Load and Process Raw Admissions Data ---
message("--- Starting Step 2.1: Loading and Processing Admissions Data ---")
admissions_dt <- load_yearly_data(
  base_path = CONFIG$paths$admissions_dir,
  years = CONFIG$years_to_process,
  file_prefix = CONFIG$file_templates$admissions_prefix,
  columns_to_keep = CONFIG$columns$admissions
)

# Convert date strings to date objects and create a year variable
admissions_dt[, ADATE := lubridate::dmy(ADATE)]
admissions_dt[, DDATE := lubridate::dmy(DDATE)]
admissions_dt[, year := lubridate::year(ADATE)]

# Filter to keep only PD-related hospitalizations
admissions_dt <- admissions_dt[Parkinson_pdx2dx_10 == TRUE]
message(sprintf("Filtered to %d PD-related hospitalizations.", nrow(admissions_dt)))

# Keep only necessary columns
admissions_dt[, Parkinson_pdx2dx_10 := NULL]


# --- Step 2.2: Load Cohort Enrollment Data ---
message("\n--- Starting Step 2.2: Loading Cohort Enrollment Data ---")
cohort_dt <- load_yearly_data(
  base_path = CONFIG$paths$cohort_dir,
  years = CONFIG$years_to_process,
  file_prefix = CONFIG$file_templates$cohort_prefix,
  columns_to_keep = CONFIG$columns$cohort
)

# Standardize ID column name for merging
setnames(cohort_dt, "qid", "QID")


# --- Step 2.3: Filter Admissions to Valid Person-Years ---
message("\n--- Starting Step 2.3: Merging Admissions with Cohort ---")
n_admissions_before <- nrow(admissions_dt)

# An inner join ensures we only keep admissions that occurred in a year
# where the beneficiary was part of the active, continuously enrolled cohort.
valid_admissions_dt <- merge(admissions_dt, cohort_dt, by = c("QID", "year"))

n_admissions_after <- nrow(valid_admissions_dt)
message(sprintf("Kept %d valid admissions. Dropped %d admissions that occurred outside of valid follow-up time.",
                n_admissions_after, n_admissions_before - n_admissions_after))


# --- Step 2.4: Identify First-Ever Hospitalization for Each Person ---
message("\n--- Starting Step 2.4: Identifying First-Ever Hospitalization ---")

# For each person (QID), find the earliest admission date (ADATE)
setkey(valid_admissions_dt, QID, ADATE)
first_hosp_dt <- valid_admissions_dt[, .SD[1], by = QID]

message(sprintf("Identified %d unique patients with a first-ever PD hospitalization during follow-up.", nrow(first_hosp_dt)))


# --- Step 2.5: Save Final Analysis-Ready Dataset ---
message("\n--- Starting Step 2.5: Saving Final Dataset ---")
write_fst(first_hosp_dt, CONFIG$paths$output_file)
message(sprintf("Final dataset saved successfully to: %s", CONFIG$paths$output_file))

# Clean up memory
gc()

message("\nETL Pipeline Complete.")