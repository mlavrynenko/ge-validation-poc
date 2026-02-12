# Data Validation Engine

A containerized data quality validation service that validates datasets from Amazon S3 using Great Expectations, computes quality metrics, stores results in PostgreSQL, and routes validated files based on success or failure.

## Overview

This service acts as a data quality layer in data pipelines.

## Main capabilities

- Load datasets from S3

- Validate data using Great Expectations

- Compute additional data quality metrics

- Store validation run metadata in PostgreSQL

- Save validation reports back to S3

- Route datasets into passes/ or failed/ folders

## Architecture

### High-Level Flow

```
S3 Dataset 
     ↓
Validation Engine  
     ↓
Great Expectations Validation
     ↓
Quality Metrics Computation
     ↓
PostgreSQL
├── validation_runs (run-level summary)
└── validation_rule_results (rule-level results)
     ↓
S3 Outputs
├── validation-results/
├── passes/
└── failed/
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
│ ├── init.py
│ └── connection.py
│
├── repository/
│ ├── validation_run_repository.py
│ └── validation_rule_repository.py
│
├── data_loader/
│ └── s3_loader.py
│
├── validation_engine/
│ └── validation.py
│
├── gx/ # Great Expectations project
└── test_data/ # Sample datasets
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
  --dataset s3://input-bucket/path/data.csv \
  --expectations my_expectation_suite \
  --results-bucket validation-results-bucket
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
python create_suite.py \
  --dataset s3://input-bucket/sample.csv \
  --suite-name my_expectation_suite
```

```yml
Example Console Output
1. expect_column_to_exist
   Result : PASS

2. expect_column_values_to_not_be_null
   Result : FAIL
   Details: {'unexpected_count': 14}
```

## Database Schema

### validation_runs

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

### validation_rule_results

| Column | Description |
|--------|------------|
| run_id | Foreign key to validation_runs |
| validated_at | Execution timestamp |
| dataset | S3 object key |
| expectation_type | Rule name |
| column_name | Column validated |
| success | Rule result |
| unexpected_count | Number of unexpected values |

## Common Errors
| Problem | Cause |
|---------|------|
| DB WRITE FAILED | Incorrect DB credentials or DB unreachable |
| Unsupported file format | Only CSV and Excel supported |
| Suite not found | Expectation suite missing in gx/ |
| Access denied to S3 | IAM permissions missing |

## Tech Stack
- Storage: Amazon S3
- Validation: Great Expectations
- Processing: Pandas
- Observability: PostgreSQL
- Containerization: Docker