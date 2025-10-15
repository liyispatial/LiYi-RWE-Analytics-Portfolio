# =============================================================================
#
# create_parkinsons_analytical_file.R
#
# Description:
# This script performs the midstream integration step for the Parkinson's study.
# It takes two key inputs:
#   1. The clean, pre-processed hospital admissions data (with Parkinson's flags).
#   2. The final, analysis-ready study cohort data (with enrollment and confounders).
# It then joins these two datasets to produce the final analytical file, ensuring
# that we only include admissions for valid members of our study cohort.
#
# Author: Li Yi
# Date: 2024-10-15
#
# =============================================================================

# -- Load necessary libraries
library(fst)
library(data.table)
library(lubridate)

# --- Configuration -----------------------------------------------------------
# All file paths and parameters are defined here for easy modification.

# -- Input Paths
ADMISSIONS_BASE_PATH <- "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/targeted_conditions/cache_data/admissions_by_year/"
COHORT_BASE_PATH <- "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/Klompmaker/cache_data/correct_follow_up_age_entry2/"

# -- Output Path
OUTPUT_PATH <- "/nfs/home/J/jok8845/shared_space/ci3_health_data/medicare/gen_admission/1999_2016/Klompmaker/admission_data/all_par_20002016_refactored.fst"

# -- Parameters
STUDY_YEARS <- 2000:2016
ADMISSIONS_COLS <- c("QID", "ADATE", "DDATE", "Parkinson_pdx2dx_10")
COHORT_COLS <- c("qid", "year")


# --- Helper Functions --------------------------------------------------------

#' Load and combine yearly FST files into a single data.table.
#'
#' @param base_path The directory containing the yearly FST files.
#' @param file_prefix The prefix of the file names (e.g., "admissions_").
#' @param years A numeric vector of years to load.
#' @param columns A character vector of columns to select.
#' @return A single data.table containing the combined data for all years.
load_yearly_data <- function(base_path, file_prefix, years, columns) {
  
  message(sprintf("Loading yearly data with prefix '%s'...", file_prefix))
  
  # Programmatically generate the full path for each yearly file
  file_paths <- file.path(base_path, sprintf("%s%d.fst", file_prefix, years))
  
  # Check which files actually exist to avoid errors
  existing_files <- file_paths[file.exists(file_paths)]
  if (length(existing_files) == 0) {
    stop("No data files found at the specified path and prefix.")
  }
  
  # Use lapply to read all existing files into a list of data.tables
  data_list <- lapply(existing_files, read_fst, columns = columns, as.data.table = TRUE)
  
  # Efficiently bind the list of data.tables into a single one
  combined_dt <- rbindlist(data_list)
  
  message(sprintf("Successfully loaded and combined %d files.", length(existing_files)))
  return(combined_dt)
}


# --- Main ETL Function -------------------------------------------------------

#' Create the final analytical file for the Parkinson's study.
#'
#' @param adm_path Base path for admissions data.
#' @param cohort_path Base path for cohort data.
#' @param out_path Path to save the final FST file.
#' @param years Numeric vector of study years.
#' @param adm_cols Columns to select from admissions data.
#' @param cohort_cols Columns to select from cohort data.
create_parkinsons_analytical_file <- function(adm_path, cohort_path, out_path, years, adm_cols, cohort_cols) {
  
  # 1. Load and process all hospital admissions data
  message("--- Step 1: Processing Admissions Data ---")
  admissions_dt <- load_yearly_data(adm_path, "admissions_", years, adm_cols)
  
  # Convert dates and define admission year
  admissions_dt[, ADATE := dmy(ADATE)]
  admissions_dt[, DDATE := dmy(DDATE)]
  admissions_dt[, year := year(ADATE)]
  
  # Filter for only Parkinson's-related admissions
  admissions_dt <- admissions_dt[Parkinson_pdx2dx_10 == TRUE]
  admissions_dt[, par := TRUE]
  admissions_dt[, Parkinson_pdx2dx_10 := NULL] # Clean up
  
  message(sprintf("Identified %d total Parkinson's-related admissions.", nrow(admissions_dt)))
  
  # 2. Load the final study cohort data
  message("\n--- Step 2: Loading Study Cohort Data ---")
  cohort_dt <- load_yearly_data(cohort_path, "confounder_exposure_merged_nodups_health_", years, cohort_cols)
  setnames(cohort_dt, "qid", "QID")
  
  # 3. Perform the integration (inner join)
  message("\n--- Step 3: Integrating Admissions with Cohort ---")
  nobs_before_merge <- admissions_dt[, uniqueN(QID)]
  
  # Merge to keep only admissions for valid members in valid years
  final_dt <- merge(admissions_dt, cohort_dt, by = c("QID", "year"), all = FALSE)
  
  nobs_after_merge <- final_dt[, uniqueN(QID)]
  n_dropped <- nobs_before_merge - nobs_after_merge
  
  message(sprintf("Dropped %d individuals who were not in the final study cohort for the year of admission.", n_dropped))
  
  # 4. Save the final output
  message("\n--- Step 4: Saving Final Analytical File ---")
  write_fst(final_dt, out_path, compress = 75)
  message(sprintf("Successfully saved final analytical file with %d observations to:\n%s", nrow(final_dt), out_path))
  
  return(final_dt)
}

# --- Main Execution Block ----------------------------------------------------

if (sys.nframe() == 0) {
  message("Starting Parkinson's ETL Integration Script...")
  
  final_data <- create_parkinsons_analytical_file(
    adm_path = ADMISSIONS_BASE_PATH,
    cohort_path = COHORT_BASE_PATH,
    out_path = OUTPUT_PATH,
    years = STUDY_YEARS,
    adm_cols = ADMISSIONS_COLS,
    cohort_cols = COHORT_COLS
  )
  
  message("Script finished successfully.")
}