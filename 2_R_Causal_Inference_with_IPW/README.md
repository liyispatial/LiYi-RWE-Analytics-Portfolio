# Project: Causal Inference with IPW for Longitudinal RWE

## Business Problem & Objective

In real-world evidence, we often need to estimate the long-term causal effect of a time-varying exposure on a health outcome. This is challenging because confounding factors can also change over time, and patients may be lost to follow-up (censoring).

This project demonstrates a robust, end-to-end analytical workflow to solve this problem using **Inverse Probability Weighting (IPW)** to fit a **Marginal Structural Model (MSM)**. The specific research question was to estimate the causal effect of long-term exposure to greenspace on adiposity outcomes in mid-life. The robustness of the findings was tested via a sensitivity analysis using multiple exposure definitions (e.g., different buffer sizes).

## Data & Cohort

This analysis was conducted on data from a large, deeply-phenotyped prospective cohort study following US women and their children from birth to mid-life between 2000-2025. The analytical dataset integrates rich longitudinal data, including clinical measures, participant surveys, and linked geospatial information on environmental exposures. The final analytical sample for this study included over 2,000 participants with complete data at baseline, demonstrating the application of these methods on a significant, real-world dataset.

## Methodological Approach

This script implements a complete causal inference analysis, including:

1.  **Data Preprocessing & ETL:** Ingesting the analytical dataset and performing necessary cleaning, variable derivation, and cohort definition.
2.  **Handling Missing Data (Censoring):** Building models to predict censoring at each follow-up wave and calculating **Inverse Probability of Censoring Weights (IPCW)**.
3.  **Handling Time-Varying Confounding:** Building models to predict exposure at each wave, conditional on past confounders and exposure history, to calculate **Inverse Probability of Treatment Weights (IPTW)**.
4.  **Stabilized Weights:** Creating stabilized weights by combining the censoring and exposure weights to control for confounding while maintaining statistical efficiency.
5.  **Weight Truncation:** Truncating extreme weights at the 1st and 99th percentiles to ensure model stability.
6.  **Marginal Structural Model (MSM):** Fitting a weighted `glm()` to the final dataset to estimate the causal effect of the exposure on the outcome.
7.  **Inference via Bootstrapping:** Implementing a non-parametric bootstrap to generate robust 95% confidence intervals.
8.  **Visualization:** Generating a publication-quality forest plot to summarize the results of all sensitivity analyses.

## Relevance for an RWE Role

This project is a direct demonstration of my ability to lead a methodologically complex study that meets the highest standards of causal inference in observational research.

*   **Technical Expertise:** It showcases proficiency in the advanced statistical methods required to generate defensible real-world evidence, a core requirement for any HEOR or advanced analytics team.

*   **Production-Minded Code:** The analysis is structured into logical, reusable functions and driven by a central configuration list. This demonstrates an understanding of how to write clean, maintainable, and efficient code suitable for a collaborative, production-oriented team environment.

*   **End-to-End Ownership:** As the first author of the resulting publication, this script proves my ability to manage the entire analytical lifecycle—from data preparation on a complex longitudinal cohort to the final, robust estimation and clear communication of a causal effect.