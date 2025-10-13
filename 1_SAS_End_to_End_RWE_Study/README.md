# Project: End-to-End RWE Survival Analysis in SAS

## Business Problem & Objective

A primary goal in real-world evidence is to understand the long-term impact of environmental and behavioral factors on chronic disease. This requires analyzing large, longitudinal datasets to model time-to-event outcomes while accounting for time-varying exposures and confounders.

This project demonstrates a complete, publication-quality workflow for this type of study using SAS. The specific research question was to conduct a time-to-event analysis assessing the association between long-term exposure to street-view greenspace and the incidence of depression.

## Data & Cohort

This analysis was conducted on data from the **Nurses' Health Study II (NHS2)**, one of the largest and longest-running prospective cohort studies in the world. The analytical dataset is a powerful example of real-world data integration:

*   **Longitudinal Data:** It includes data from over 80,000 participants followed biennially from 2001 to 2017.
*   **Rich Phenotyping:** It integrates rich, participant-level data from questionnaires, including demographics, lifestyle factors, clinical diagnoses, and medication use.
*   **Data Fusion:** This cohort data was linked at the residential address level to multiple geospatial datasets, including novel street-view greenspace metrics (the primary exposure) and other environmental data like NDVI and air pollution.

## Methodological Approach & Structure

The analysis is designed as an automated, reproducible system orchestrated by the **`master_run.sas`** script. This "driver" script manages the project's execution by calling each of the following modular scripts in sequence:

1.  **`01_etl.sas`:** Ingests all raw data sources, performs complex data cleaning and imputation (including last-observation-carried-forward for longitudinal variables), and transforms the data from a "wide" (one row per person) to a "long" **person-period format** suitable for survival analysis.
2.  **`04_create_table1.sas`:** Programmatically generates descriptive baseline characteristics tables (Table 1), both for the overall cohort and stratified by key exposure variables.
3.  **`05_run_survival_models.sas`:** Automatically executes the full suite of primary and sensitivity analyses, iterating through dozens of **Cox proportional hazards models (`PROC PHREG`)** for different exposure definitions and outcomes.
4.  **`06_run_stratified_analysis.sas`:** Performs all pre-specified stratified analyses and interaction tests to evaluate potential effect modification by factors like Census Region.

## Relevance to AbbVie & RWE

This project is a direct demonstration of my ability to lead a rigorous observational study using the industry-standard tool for RWE, with a strong focus on automation and efficiency.

*   **SAS Proficiency & RWE Methods:** It showcases deep expertise in SAS for managing complex longitudinal data and applying core RWE methods like survival analysis (`PROC PHREG`). My experience with this large-scale cohort is directly transferable to analyzing patient-level data from claims or EMRs.

*   **Production-Minded Workflow:** This is the key strength of the project. The code is not a one-off script; it is an automated system. It demonstrates a commitment to the **DRY (Don't Repeat Yourself) principle** through the extensive use of **advanced macro programming**. Repetitive tasks—like running sensitivity analyses or generating stratified tables—are handled by automated loops, which drastically reduces the risk of error and makes the entire project more efficient and maintainable. This directly addresses the JD's focus on "standardizing processes" and "reducing operational complexity."

*   **End-to-End Ownership:** It demonstrates my ability to manage the entire analytical lifecycle—from raw data ingestion to the final, formatted Excel reports of the primary, sensitivity, and stratified analyses. This proves I can function as a true "analytics lead" who can deliver a complete, robust, and well-documented project from start to finish.