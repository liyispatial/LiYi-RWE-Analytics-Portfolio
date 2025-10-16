# Project: Scalable Pipeline for Unstructured Image Data Analysis

## Business Problem & Objective

A core challenge in modern RWE is the need to extract quantitative insights from non-tabular, unstructured data sources like medical images or digital pathology slides. Answering critical research questions often requires a solution that can process hundreds of thousands of these images in a robust, automated, and reproducible manner.

This project demonstrates a production-minded data processing pipeline built in Python. Its purpose was to perform semantic segmentation on a massive dataset of street-level images to create a novel, quantitative dataset of environmental features for epidemiological analysis.

## Associated Publication

The pipeline demonstrated in this folder is the core technical engine that was used to generate the novel geospatial data for the following peer-reviewed publication, on which I am a co-author.

> Larkin, A., Huang, T., Chen, L., et al. (2025). Developing Nationwide Estimates of Built Environment Quality Characteristics using Street View Imagery and Computer Vision. *Environmental Science & Technology*.
>
> **[View on PubMed](https://pubmed.ncbi.nlm.nih.gov/39636637/)**


## Technical Solution & Engineering Best Practices

This is not a one-off analysis script; it is a reusable, command-line-driven tool that showcases professional software development practices.

1.  **Object-Oriented Design:** The core logic is encapsulated in an `ImageSegmenter` class. This separates concerns, making the code cleaner, easier to maintain, and simpler to test. The model, configuration, and processing logic are all managed within this class.
2.  **Scalable Batch Processing:** The pipeline is designed to ingest a manifest file (a CSV) and process images in a loop, demonstrating its capability to handle large-scale batch jobs.
3.  **Configuration Driven:** The script is driven by a YAML configuration file and command-line arguments (`argparse`), allowing it to be run with different models or parameters without changing the source code.
4.  **Robustness and Maintainability:** The code includes professional-grade features like comprehensive logging (`logging`), clear function and method docstrings, and error handling (`try...except`) to ensure reliability during long-running jobs.

## Deployment & Scalability on High-Performance Computing (HPC)

This pipeline is not just a local script; it is designed for large-scale, automated deployment on a High-Performance Computing (HPC) cluster.

The included `run_pipeline.sh` script is a production-ready example of how a batch processing job is submitted to a **SLURM workload manager**. This submission script handles:

*   **Resource Allocation:** Requesting the necessary CPU cores and memory for the job.
*   **Environment Management:** Activating the correct Conda environment to ensure all dependencies are correctly loaded.
*   **Automated Execution:** Calling the main Python script with the correct command-line arguments to run the pipeline non-interactively.

This demonstrates a complete, end-to-end workflow, from the core Python application to its deployment and execution in a scalable, high-performance environment.

## Relevance to AbbVie & RWE

This project is a direct demonstration of my ability to build the efficient, automated systems required by a modern RWE team.

*   **Systems Builder Mindset:** It proves I can do more than just analyze data; I can build the tools and platforms that increase a team's efficiency and unlock new capabilities. This directly addresses the JD's focus on "standardizing processes" and "reducing operational complexity."
*   **Handling Unstructured Data:** This methodology of building a system to process and quantify a non-tabular data source is directly transferable to high-value RWE data types, such as digital pathology slides, MRIs, or other medical imaging.
*   **Production-Ready Skills:** It showcases my proficiency with the engineering best practices required to build reliable, maintainable, and collaborative code in Python.