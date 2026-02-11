Data Validation Engine

A containerized data quality validation service that validates datasets from Amazon S3 using Great Expectations, computes quality metrics, stores results in PostgreSQL, and routes validated files based on success or failure.

Overview

This service acts as a data quality layer in data pipelines.

Main capabilities

Load datasets from S3

Validate data using Great Expectations

Compute additional data quality metrics

Store validation run metadata in PostgreSQL

Save validation reports back to S3

Route datasets into passes/ or failed/ folders

Architecture

Flow

S3 Dataset → Validation Engine → Great Expectations
                                        ↓
                                Quality Metrics
                                        ↓
                            Postgres (validation_runs)
                                        ↓
                    S3 validation-results/ + passes/failed/

Project Structure
.
├── main.py
├── create_suite.py
├── Dockerfile
├── requirements.txt
│
├── data_loader/
│   └── s3_loader.py
│
├── validation_engine/
│   └── validation.py
│
├── gx/          # Great Expectations project
└── test_data/   # Sample datasets

Prerequisites

Docker

AWS credentials with S3 access

PostgreSQL database

Network access from container to DB

Build Docker Image
docker build -t data-validation-engine .

Environment Variables
export DB_HOST=<db-host>
export DB_NAME=<database>
export DB_USER=<username>
export DB_PASSWORD=<password>

Run Validation
docker run --rm \
  -e DB_HOST=$DB_HOST \
  -e DB_NAME=$DB_NAME \
  -e DB_USER=$DB_USER \
  -e DB_PASSWORD=$DB_PASSWORD \
  data-validation-engine \
  --dataset s3://input-bucket/path/data.csv \
  --expectations my_expectation_suite \
  --results-bucket validation-results-bucket

Command Line Arguments
Argument	Description
--dataset	S3 path to input dataset
--expectations	Expectation suite name
--results-bucket	S3 bucket for outputs
Execution Steps

Dataset downloaded from S3

Validated using Great Expectations

Metrics calculated (quality score, null ratio, duplicates, etc.)

Run metadata stored in PostgreSQL

JSON validation report saved to S3

Dataset routed to:

passes/ if validation succeeded

failed/ if validation failed

Output Structure in S3
s3://<results-bucket>/

validation-results/
    <timestamp>_<file>.validation.json

passes/
    <timestamp>_<file>

failed/
    <timestamp>_<file>

Exit Codes
Code	Meaning
0	Validation PASSED
1	Validation FAILED

Designed to be used in Airflow, CI/CD pipelines, or automated quality gates.

Creating an Expectation Suite
python create_suite.py \
  --dataset s3://input-bucket/sample.csv \
  --suite-name my_expectation_suite

Example Console Output
1. expect_column_to_exist
   Result : PASS

2. expect_column_values_to_not_be_null
   Result : FAIL
   Details: {'unexpected_count': 14}

Common Errors
Problem	Cause
DB WRITE FAILED	Incorrect DB credentials or DB unreachable
Unsupported file format	Only CSV and Excel supported
Suite not found	Expectation suite missing in gx/
Access denied to S3	IAM permissions missing
Tech Stack

Storage: Amazon S3

Validation: Great Expectations

Processing: Pandas

Observability: PostgreSQL

Containerization: Docker