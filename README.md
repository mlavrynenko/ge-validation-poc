### Python version

This project requires **Python 3.11**.
Python 3.12+ is not supported due to NumPy / Pandas compatibility.

# Data Validation Engine

A containerized, template-driven data quality validation service for validating
tabular datasets (CSV, Excel, Parquet, Iceberg) from Amazon S3 and table catalogs
using Great Expectations.

## Overview

This service acts as a data quality layer in data pipelines.

## Main Capabilities

- Load datasets from Amazon S3 and table-based sources
- Support multiple data formats:
  - CSV
  - Excel (multi-sheet)
  - Parquet
  - Apache Iceberg (logical tables)
- Template-driven structural validation (schema, required columns)
- Rule-based validation using Great Expectations
- Compute data quality metrics (null ratio, duplicates, quality score, etc.)
- Persist validation results to PostgreSQL
- Enable observability and dashboards via Grafana

## Supported Input Types

| Type     | Source                  | Notes |
|---------|--------------------------|------|
| CSV     | S3 object                | Header-based |
| Excel   | S3 object                | Multi-sheet support |
| Parquet | S3 object                | Columnar, schema-aware |
| Iceberg | Catalog-backed table     | Snapshot-aware, schema evolution support |

Iceberg tables are treated as logical tables, not files.

## Architecture

### High-Level Flow

```
S3 / Iceberg Table
     ↓
Template Resolution
     ↓
Dataset Reader (CSV / Excel / Parquet / Iceberg)
     ↓
Structural Validation
     ↓
Great Expectations Validation
     ↓
Quality Metrics Computation
     ↓
PostgreSQL (dq schema)
```

## Database Tables
```
dq.validation_runs
dq.validation_rule_results
dq.structural_validation_results
```

## Project Structure
```
.
├── main.py
├── Dockerfile
├── requirements.txt
├── README.md
│
├── core/
│ └── logging_config.py
│
├── db/
│ └── connection.py
│
├── repository/
│ ├── validation_run_repository.py
│ ├── validation_rule_repository.py
│ └── structural_validation_repository.py
│
├── data_loader/
│ └── s3_loader.py
│
├── file_parser/
│ ├── base.py
│ ├── csv.py
│ ├── excel.py
│ ├── parquet.py
│ └── iceberg.py
│
├── template_engine/
│ ├── models.py
│ ├── registry.py
│ └── resolver.py
│
├── validation_engine/
│ ├── handler.py
│ ├── structural.py
│ └── validation.py
│
├── gx/               # Great Expectations context
└── templates/        # YAML validation templates
```

## Template-Driven Validation

Datasets are validated based on YAML templates that define:

- File matching rules (regex)
- File type (csv, excel, parquet, iceberg)
- Sheets (for Excel)
- Expected columns and requirements
- Associated Great Expectations suites

Example template:

```yaml
template_id: example_tabs
version: 1
file_type: excel
file_pattern: ".*incoming/Excel.*\\.xlsx$"

sheets:
  - name: Tab A
    header_row: 1
    columns:
      col 1:
        required: true
      col 2:
        required: false
    expectations:
      - tab_a_checks
```        

## Prerequisites

- Docker
- AWS credentials with S3 access
- PostgreSQL database
- Network access from container to DB

## Build Docker Image
```bash
docker build -t data-validation-engine .
```

## Environment Variables
```bash
export DB_HOST=<db-host>
export DB_NAME=<database>
export DB_USER=<username>
export DB_PASSWORD=<password>
```

## Run Validation
```bash
docker run --rm \
  -e DB_HOST=$DB_HOST \
  -e DB_NAME=$DB_NAME \
  -e DB_USER=$DB_USER \
  -e DB_PASSWORD=$DB_PASSWORD \
  data-validation-engine \
  --dataset s3://input-bucket/path/data.xlsx
```

## Command Line Arguments
| Argument | Description |
|----------|------------|
| --dataset | S3 path to input dataset |
| --expectations | Expectation suite name |
| --results-bucket | S3 bucket for outputs |

## Execution Steps

1. Dataset downloaded from S3
2. Validated using Great Expectations
3. Metrics calculated (quality score, null ratio, duplicates, etc.)
4. Run metadata stored in PostgreSQL
5. JSON validation report saved to S3
6. Dataset routed to:
- passes/ if validation succeeded
- failed/ if validation failed

## Output Structure in S3
```
s3://<results-bucket>/

validation-results/
    <timestamp>_<file>.validation.json

passes/
    <timestamp>_<file>

failed/
    <timestamp>_<file>
```

## Exit Codes
Code	Meaning
0   	Validation PASSED
1	    Validation FAILED

Designed to be used in Airflow, CI/CD pipelines, or automated quality gates.

## Creating an Expectation Suite
```
Expectation suites are generated from templates and sample datasets.

```bash
python -m scripts.create_expectation_suite \
  --dataset s3://input-bucket/sample.xlsx \
  --template-id example_tabs \
  --sheet-name "Tab A" \
  --suite-name tab_a_checks
```

```yml
Example Console Output
1. expect_column_to_exist
   Result : PASS

2. expect_column_values_to_not_be_null
   Result : FAIL
   Details: {'unexpected_count': 14}
```

## Update Database Schema section (schema-aware)

### Database Schema (PostgreSQL)

All tables are stored in the `dq` schema.

### dq.validation_runs

| Column | Description |
|--------|------------|
| run_id | Unique validation run ID |
| dataset | S3 object key |
| success | Overall validation result |
| validated_at | Execution timestamp |
| row_count | Number of rows in dataset |
| rules_total | Total expectations executed |
| rules_passed | Passed expectations |
| rules_failed | Failed expectations |
| quality_score | rules_passed / rules_total |
| null_ratio | Ratio of null values |
| duplicate_ratio | Ratio of duplicate rows |
| schema_changed | Boolean flag |
| invalid_row_count | Aggregated unexpected count |

---

### dq.validation_rule_results

| Column | Description |
|--------|------------|
| run_id | Foreign key to validation_runs |
| validated_at | Execution timestamp |
| dataset | S3 object key |
| expectation_type | Rule name |
| column_name | Column validated |
| success | Rule result |
| unexpected_count | Number of unexpected values |

### dq.structural_validation_results

| Column | Description |
|------|------------|
| run_id | Validation run ID |
| sheet_name | Sheet validated |
| passed | Structural validation result |
| error_count | Number of errors |
| warning_count | Number of warnings |
| errors | JSON list of errors |
| warnings | JSON list of warnings |
| validated_at | Timestamp |

## Common Errors

| Problem | Cause |
|------|------|
| No template matches file | Dataset path does not match any template regex |
| expectation_suite not found | Suite not created in gx/expectations |
| permission denied for schema dq | Missing GRANT USAGE on schema |
| Unsupported file type | file_type not registered in parser registry |
| Iceberg table not found | Catalog or table identifier incorrect |

## Tech Stack
- Storage: Amazon S3
- Table Formats: Apache Iceberg
- Validation: Great Expectations
- Processing: Pandas, PyArrow
- Metadata Store: PostgreSQL
- Visualization: Grafana
- Containerization: Docker
