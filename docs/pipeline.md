Processing Flow

The project follows a structured pipeline:

raw data → ETL process → file parsing → transformed outputs → backtesting package integration
Step 1 — ETL Process

Raw files and source inputs are extracted, transformed, and organized into a format suitable for processing.

This stage is responsible for preparing the data and reducing inconsistencies before parsing.

Step 2 — File Parsing

Structured and semi-structured files are parsed into usable internal representations.

This includes reading logs, splitting content when necessary, and organizing file-based inputs for downstream use.

Step 3 — Data Transformation

Parsed data is normalized and transformed into the expected structures required by the processing workflow.

This stage improves consistency and prepares the data for integration with the backtesting package.

Step 4 — Backtesting Package Integration

The processed outputs are connected to the external backtesting package, which is responsible for strategy execution and result generation.

This repository does not perform the full backtesting execution independently. Instead, it supports and prepares the workflow that feeds the backtesting environment.