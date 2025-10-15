# RWE Case Study: Production-Ready ETL Pipeline for Medicare Claims

## Objective

This folder contains a production-ready R script that demonstrates a robust and scalable ETL (Extract, Transform, Load) pipeline for processing large-scale administrative claims data. The script's purpose is to identify the **first-ever hospitalization** for a specific condition (in this case, Parkinson's Disease) for a cohort of millions of Medicare beneficiaries, while correctly accounting for their continuous enrollment status.

This script is a tangible demonstration of the methods described in this case study.

## Associated Publication

The methods and logic demonstrated in this case study and the accompanying R script are based on the work for the following peer-reviewed publication, on which I am a co-author.

> Klompmaker, J.O., Mork, D., Zanobetti, A., et al. (2024). Associations of street-view greenspace with Parkinson’s disease hospitalizations in Medicare. *Environment International*.
>
> **[View on PubMed Central](https://pmc.ncbi.nlm.nih.gov/articles/PMC11199351/)**

---

## Technical Solution & Engineering Best Practices

This script has been refactored from a research script into a reusable and professional-grade tool, showcasing key software engineering principles:

1.  **Modularity and Reusability:** The core logic for loading the large, multi-year datasets is encapsulated in a single, reusable function (`load_yearly_data`). This eliminates code duplication and makes the pipeline easy to adapt for other data sources.

2.  **Configuration-Driven Design:** All file paths, years, and key variable names are stored in a `CONFIG` list at the top of the script. This separates the logic from the configuration, allowing the pipeline to be run on different data or for different years without changing the source code.

3.  **Efficiency and Scalability:** The script uses the highly efficient `data.table` and `fst` packages, which are designed for performance on massive datasets. The data loading is automated through a programmatic loop, not manual copy-pasting, demonstrating an ability to build scalable solutions.

4.  **Clarity and Maintainability:** The script is organized into a clear, logical workflow with numbered steps. Each step is explained with professional comments, making the code easy for a new team member to understand, review, and maintain.

## Relevance for an RWE Role

This ETL pipeline is a direct demonstration of the foundational skills required for any RWE data scientist. It proves my ability to:

*   **Handle Large-Scale Claims Data:** I can efficiently process and manipulate datasets containing tens of millions of records spanning multiple years.
*   **Build Automated Systems:** I write code that is not just for a single analysis but is a reusable tool that increases team efficiency and reproducibility.
*   **Implement Core RWE Logic:** I can translate a complex scientific need (like identifying an incident event while respecting enrollment periods) into a robust, accurate data processing workflow.