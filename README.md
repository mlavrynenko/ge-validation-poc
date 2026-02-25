### Python version

This project requires **Python 3.11**.
Python 3.12+ is not supported due to NumPy / Pandas compatibility.

The Docker image also uses Python 3.11 to ensure consistency between local and container execution.

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
|---------|-------------------------|------|
| CSV     | S3 object               | Header-based |
| Excel   | S3 object               | Multi-sheet support |
| Parquet | S3 object               | Columnar, schema-aware |
| Iceberg | Catalog table           | Logical table, not a file |

## Iceberg Datasets

Iceberg datasets are treated as **logical tables**, not files.

Example dataset identifier: iceberg://glue.dq_iceberg_dev.orders

Key differences:
- No S3 download
- No header_row
- Schema comes from the catalog
- DATE columns may arrive as `datetime.date`

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
    expectation_suite:
      - tab_a_checks
```        
### Logical Sheets

For non-Excel formats (Parquet, Iceberg), templates still define a logical `sheet`:

```yaml
sheets:
  - name: data
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
  -e RESULTS_BUCKET=dataquality-poc-validation-results \
  data-validation-engine \
  --dataset s3://input-bucket/path/data.xlsx
```

## Command Line Arguments
| Argument            | Description |
|---------------------|------------|
| --dataset           | S3 path to input dataset |
| --results-bucket    | S3 bucket for outputs |

## Execution Steps

1. Dataset downloaded from S3
2. Validated using Great Expectations
3. Metrics calculated (quality score, null ratio, duplicates, etc.)
4. Run metadata stored in PostgreSQL
5. JSON validation report saved to S3
6. Dataset routed to:
- passes/ if validation succeeded
- failed/ if validation failed

### Output structure

```text
s3://<RESULTS_BUCKET>/

validation-results/
  <timestamp>__<dataset>__<sheet>.json

passes/
  <timestamp>__<dataset>__dataset.<ext>

failed/
  <timestamp>__<dataset>__dataset.<ext>
```

## Rules vs Expectation Suites (Important)

This framework distinguishes clearly between:

### Template Rules
Rules are declared in templates to describe **validation intent**:

- not_null_required
- positive
- unique
- date_format
- date_type

Rules are used **only when generating expectation suites**
(via `scripts.create_expectation_suite`).

They are **NOT executed at runtime**.

### Expectation Suites
Expectation suites are the **only executable validation units**.

At runtime:
- The engine loads expectation suites from `gx/`
- Template rules are ignored
- A warning is logged if rules exist in the template

```text
Rules → used once → create expectation suite
Suites → executed every run
```

---

### Rule Declaration (Template-Level)

Expectation rules are defined per sheet in the validation template:

```yaml
template_id: orders_parquet
version: 1
file_type: parquet
file_pattern: ".*orders.*\\.parquet$"

sheets:
  - name: data
    required: true
    header_row: 1

    columns:
      id:
        required: true
        type: int
      order_amount:
        required: true
        type: decimal
      created_at:
        required: true
        type: date

    rules:
      - name: not_null_required
      - name: positive
        columns: [order_amount]
      - name: unique
        columns: [id]
      - name: date_format
        columns: [created_at]
        params:
          format: "%Y-%m-%d"

    expectation_suite:
      - orders_parquet_checks
``` 

### Iceberg-Specific Behavior

Some rules behave differently for Iceberg datasets:

- `date_type`
  - Skips validation if column dtype is `object` (Iceberg DATE)
- `expect_column_values_to_be_of_type`
  - Automatically removed when generating Iceberg suites

This avoids false failures caused by Iceberg → Pandas type coercion.

## Creating an Expectation Suite

Expectation suites must exist before validation runs.

Suites can be generated from templates and sample datasets.

### PowerShell (Windows)

```powershell
python -m scripts.create_expectation_suite `
  --dataset s3://input-bucket/path/data.parquet `
  --template-id orders_parquet `
  --sheet-name data `
  --suite-name orders_parquet_checks
```

## Validation Lifecycle

1. Template written (rules optional)
2. Expectation suite generated (rules applied)
3. Dataset validated (suites only)
4. Results stored in PostgreSQL
5. Artefacts optionally written to S3

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

| Error | Cause | Fix |
|-----|------|-----|
| No template matches file | Regex does not match dataset path | Update `file_pattern` |
| expectation_suite not found | Suite not created | Run `create_expectation_suite` |
| Unsupported file type | Parser not registered | Add parser support |
| numpy import error | Python > 3.11 | Downgrade to Python 3.11 |
| S3 artefacts not written | RESULTS_BUCKET not set | Set env var or CLI override |

## Tech Stack
- Storage: Amazon S3
- Table Formats: Apache Iceberg
- Validation: Great Expectations
- Processing: Pandas, PyArrow
- Metadata Store: PostgreSQL
- Visualization: Grafana
- Containerization: Docker
