# RWE Case Study: Methodological Approach for a Claims-Based Study

## Objective

This document outlines the analytical strategy for a large-scale, longitudinal real-world evidence study using administrative claims data (e.g., Medicare, Medicaid). It is based on a peer-reviewed publication for which I was a co-author.

While I cannot share the final proprietary analysis code, **my role on this project involved a close collaboration with the first author on the core data engineering and cohort construction phases.** This case study demonstrates my conceptual and practical understanding of executing a rigorous RWE study from start to finish using this complex data source.

## Associated Publication

This case study is based on the methods and collaborative work for the following peer-reviewed publication. My contribution is acknowledged in the author list.

> Klompmaker, J.O., Mork, D., Zanobetti, A., et al. (2024). Associations of street-view greenspace with Parkinson’s disease hospitalizations in Medicare. *Environment International*.
>
> **[View on PubMed Central](https://pmc.ncbi.nlm.nih.gov/articles/PMC11199351/)**

---

## The Core Challenge: From Messy Data to Causal Questions

Unlike a structured clinical trial or a prospective cohort, claims data is not collected for research; it is a byproduct of the billing system. The primary challenge is to translate a scientific question into a robust analysis by creating a valid analytical cohort from this "messy" data. This requires a series of deliberate, pre-specified decisions.

---

### **Phase 1: Cohort Construction & Data ETL**

This is the most critical and labor-intensive phase, and it was the primary focus of my contribution to the project. The goal is to identify the correct patient population and define a clear "time zero" for the analysis.

1.  **Defining the Study Population:**
    *   The process begins with the full enrollment files for millions of beneficiaries.
    *   A key first step is to require a period of **continuous enrollment** (e.g., 12 months) *before* the index date. This "look-back" period is essential to ensure a complete baseline history of a patient's comorbidities and prior treatments is available.

2.  **Identifying the Disease Cohort (Case Definition):**
    *   Identifying patients with a specific disease is a major challenge due to the risk of misclassification from "rule-out" diagnoses. A single diagnosis code is not sufficient for a robust study.
    *   A best-practice approach involves creating a case definition that requires a combination of evidence:
        *   **Diagnosis Codes:** At least two medical claims with a specific **ICD-10 code** (e.g., G20 for Parkinson's) separated by a pre-defined time period.
        *   **Pharmacy Claims:** At least one prescription fill for a disease-specific medication, identified by its **NDC code**.
    *   Only patients meeting these multiple criteria are included in the final cohort, which greatly increases the validity of the study population.

3.  **Defining the Index Date:**
    *   "Time zero" must be a discrete, unambiguous event. A diagnosis date in claims can be unreliable.
    *   A more robust method is to define the **index date** as the date of the *first qualifying prescription fill* for a disease-specific medication, as this is a concrete and reliable anchor point for the start of follow-up.

---

### **Phase 2: Endpoint Definition & Statistical Modeling**

Once the cohort is built, the next step is to define the outcomes and select the appropriate statistical model for the research question and data structure.

1.  **Defining Endpoints:**
    *   Outcomes like "hospitalization" or "time to next treatment" must be carefully defined using specific codes from the claims files (e.g., **place-of-service codes** for inpatient stays).
    *   It's also crucial to understand the scientific nuance of the endpoint. For example, a "disease-related hospitalization" in claims is often a proxy for **accelerated disease progression**, not necessarily disease onset.

2.  **Modeling Strategy for Massive Datasets:**
    *   For extremely large datasets like Medicare, running a patient-level survival model can be computationally prohibitive.
    *   An advanced and efficient alternative is to use a **Cox-equivalent re-parameterized Poisson model**. This involves aggregating the data into cells (e.g., by age, sex, and geography) and modeling the `count of events` with the `log of person-time` as an offset. This approach is mathematically equivalent to a time-varying Cox model but is far more scalable.

3.  **Covariate Engineering & Robust Inference:**
    *   Covariates for risk adjustment, like the **Charlson Comorbidity Index**, must be engineered by scanning all diagnosis codes in the look-back period.
    *   To account for correlated data (e.g., patients within the same geographic area), standard confidence intervals can be misleading. A more robust approach is to use a **bootstrap method**, resampling by the geographic unit (e.g., ZIP code) to calculate valid confidence intervals.

---

### **Conclusion & Relevance for an RWE Role**

My experience on this project was invaluable. **By working closely with the first author on the cohort construction and ETL, I gained hands-on experience with the fundamental challenges of claims-based RWE.**

While my first-author SAS script showcases my ability to lead a full study on cohort data, this case study proves my practical ability to navigate the unique complexities of real-world payer data. I am confident in my ability to apply this knowledge to answer critical RWE questions in any therapeutic area.
