# RWE Case Study: A Modular ETL Pipeline for Medicare Claims Data

## Project Overview

This case study details the architecture of a large-scale, end-to-end ETL (Extract, Transform, Load) pipeline built in R. The primary objective was to process and integrate massive, disparate Medicare administrative datasets to create a single, analysis-ready "source of truth." This foundational dataset enabled a series of peer-reviewed epidemiological studies investigating the impact of environmental factors on health outcomes for over 65 million US seniors.

The principles and architecture demonstrated here are directly applicable to the challenges of Value-Based Care (VBC) analytics, where building a reliable, longitudinal patient record is the critical first step to measuring program ROI and performance guarantees.

## Methodology: A Modular, Multi-Pipeline Architecture

To handle the complexity and scale of the data, my team architected the project as a "research factory" composed of three distinct upstream pipelines that create core data assets, a midstream pipeline for integration, and a downstream pipeline for modeling. This modular design ensures each stage is maintainable, testable, and independently executable.

### Upstream Pipeline A: The Cohort Builder (The "Who")
*   **Input:** Raw Medicare denominator (enrollment) files.
*   **Process:** A seven-step process that ingested raw data, calculated age, applied strict inclusion/exclusion criteria (e.g., removing HMO person-time), and established a continuous enrollment timeline for every beneficiary.
*   **Output:** A clean, longitudinal cohort dataset defining exactly who was in our study and for how long.

### Upstream Pipeline B: The Health Outcome Definer (The "What")
*   **Input:** Raw Medicare inpatient claims files (MedPAR).
*   **Process:** This pipeline processed terabytes of claims data, applying ICD-9 and ICD-10 logic to identify all hospital admissions for specific diseases of interest, such as Parkinson's Disease.
*   **Output:** A series of clean, high-performance boolean flag files, one for each health outcome.

### Upstream Pipeline C: The Environmental Exposure Modeler (The "Where")
*   **Input:** Raw geospatial and environmental datasets (e.g., satellite imagery, weather station data).
*   **Process:** This pipeline leveraged my epidemiological expertise to translate raw environmental data into validated, health-relevant exposure metrics for every US ZIP code.
*   **Output:** A clean, longitudinal exposure file.

### Midstream Pipeline: The Great Integration
*   **Input:** The outputs from the three upstream pipelines.
*   **Process:** This crucial stage performed the final, high-performance joins to link the "Who," the "What," and the "Where." It took the clean cohort, enriched it with the exposure data, and then filtered the health outcome data to only include events that occurred for valid members of the cohort.
*   **Output:** The final, analysis-ready dataset for statistical modeling.

## Key Principles & Technologies
*   **Performance at Scale:** The entire pipeline was built using high-performance R packages, primarily **`data.table`** for memory-efficient data manipulation and **`fst`** for rapid data serialization, which is essential for working with datasets too large for memory.
*   **Modularity & Maintainability:** By separating concerns into distinct pipelines and scripts, the codebase is easier to debug, update, and for new team members to understand.
*   **Reproducibility:** The environment was managed with **`renv`** to ensure long-term reproducibility of all findings.