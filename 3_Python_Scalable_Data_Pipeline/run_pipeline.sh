#!/bin/bash

# =============================================================================
#
# SLURM JOB SUBMISSION SCRIPT
#
# DESCRIPTION: This script submits the Python image segmentation pipeline
#              to a SLURM-managed High-Performance Computing (HPC) cluster.
#              It handles resource allocation, environment setup, and execution.
#
# USAGE:
#   sbatch run_pipeline.sh
#
# =============================================================================

# --- SLURM Directives ---
#SBATCH -c 4                               # Request 4 CPU cores
#SBATCH -t 0-03:00:00                      # Runtime in D-HH:MM:SS format (3 hours)
#SBATCH -p short                           # Partition to run in
#SBATCH --mem=100G                         # Memory total in GB (e.g., 100GB)
#SBATCH -o logs/segmentation_job_%j.out    # Standard output log file
#SBATCH -e logs/segmentation_job_%j.err    # Standard error log file
#SBATCH --job-name=image-segmentation      # A descriptive name for the job

# --- Shell Best Practices ---
# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u

# --- Configuration ---
# Define parameters at the top for easy modification.
CONFIG_FILE="config/ade20k/ade20k_pspnet50.yaml"
IMAGE_DIR="data/mturk/imgs"
MANIFEST_FILE="data/mturk/meta_short.csv"
OUTPUT_FILE="results/meta_short_processed.csv"
CONDA_ENV="pytorch"

# --- Job Execution ---
echo "========================================================"
echo "Starting Image Segmentation Job"
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "Start Time: $(date)"
echo "========================================================"

# Create log directory if it doesn't exist
mkdir -p logs

# Activate the Conda environment
echo "Activating Conda environment: $CONDA_ENV"
# Note: Ensure conda is initialized in your shell environment, or use the full path.
source activate "$CONDA_ENV"

# Run the Python pipeline
echo "Executing Python pipeline..."
python gsv_processing_pipeline.py \
    --config "$CONFIG_FILE" \
    --image_dir "$IMAGE_DIR" \
    --manifest_file "$MANIFEST_FILE" \
    --output_file "$OUTPUT_FILE"

echo "========================================================"
echo "Job Finished Successfully"
echo "End Time: $(date)"
echo "========================================================"