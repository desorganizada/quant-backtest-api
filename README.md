# Quant Backtest API

## Overview

This project is a portfolio demonstration of a quantitative data-processing workflow designed to prepare financial data, parse structured inputs, and connect them to a backtesting package.

---

## What This Project Demonstrates

* Modular backend design in Python
* Data processing pipelines (ETL)
* File parsing and normalization
* Integration layer for backtesting workflows
* Containerized environment setup using Docker

---

## Processing Flow

The project follows a structured processing pipeline:

### 1. ETL Process

Raw input data is extracted, transformed, and organized for analysis.

### 2. File Parsing

Input files are parsed and normalized into structured formats.

### 3. Backtesting Package Integration

The processed outputs are prepared and routed to the backtesting package responsible for strategy execution and result generation.

---

## Repository Structure

```
quant-backtest-api/
├── src/
├── docs/
├── examples/
├── config/
├── Dockerfile
├── docker-compose.yml
├── environment.yml
└── README.md
```

---

## Examples

The `examples/` directory contains simplified and sanitized outputs from the system, including sample processed files and representative result artifacts.

---

## Notes

This project is presented as a portfolio artifact. Some components depend on private data sources, restricted environments, and an external backtesting package, so full execution may not be reproducible externally.

---

## Author

Developed as a demonstration of backend and quantitative engineering skills.
