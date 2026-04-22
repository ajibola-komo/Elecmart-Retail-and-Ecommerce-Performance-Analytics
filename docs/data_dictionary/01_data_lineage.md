# Data Lineage

## Overview

This project follows a modern ELT architecture:

**Python → S3 → Snowflake → dbt → BI(Tableau)**

---

## Pipeline Stages

### 1. Data Generation (Python)
- Synthetic data generated using Faker & NumPy
- Embedded business rules for realism
- Output: Parquet files

---

### 2. Data Lake (S3)
- Raw storage layer
- Immutable source data

---

### 3. Bronze Layer (Snowflake)
- Raw ingestion (1:1 mapping from source)
- No transformations applied

---

### 4. Silver Layer (dbt)
- Data cleaning and standardization
- Handling missing values
- Data type normalization

---

### 5. Gold Layer (dbt)
- Star schema implementation
- Business-ready tables
- Aggregations and denormalization

---

### 6. Consumption Layer
- BI tools (e.g., Tableau)
- Dashboards and reporting