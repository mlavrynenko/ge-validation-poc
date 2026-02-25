# 📘 Onboarding a New Dataset into the Data Quality System

This document describes the **required steps** to onboard a new **CSV, Excel, Parquet, or Iceberg** dataset into the Data Quality (DQ) validation system.

The process is **deterministic, auditable, and production-safe**.

---

## 🧭 Overview

For **every new dataset**, you must complete the following steps:

0. Identify dataset type
1. Understand the dataset
2. Create a validation template (YAML)
3. Create a Great Expectations suite
4. Run validation
5. Verify results (PostgreSQL + S3)

---

## 🧭 File-Based vs Iceberg Datasets

### File-Based (CSV / Excel / Parquet)
- Dataset is downloaded from S3
- Structural validation checks headers & columns
- Data archived to passes/ or failed/

### Iceberg
- Dataset is read from catalog
- No file download
- No archiving of original data
- Validation is logical-table based

---

## 🟡 Step 0 — Identify Dataset Type

Determine how the dataset is accessed:

| Type     | Identifier example |
|---------|--------------------|
| CSV     | s3://bucket/path/file.csv |
| Excel   | s3://bucket/path/file.xlsx |
| Parquet | s3://bucket/path/file.parquet |
| Iceberg | iceberg://glue.db.table |

This choice affects:
- Template fields
- Parser used
- Valid rules

## ✅ Step 1 — Understand the dataset

Before making any changes, collect the following information:

### Required inputs

| Item | Excel | Parquet |
|----|------|--------|
| S3 dataset path | ✅ | ✅ |
| File type | `.xlsx` | `.parquet` |
| Sheet / logical dataset | Sheet name(s) | Always `data` |
| Header row | Required | ❌ Not applicable |
| Expected columns | Required | Required |
| Mandatory columns | Required | Required |
| Validation rules | Optional | Optional |

---

## ✅ Step 2 — Create a template (YAML)

Templates define the **expected structure** of the dataset.

📁 Location: templates/

---

### 🟦 Example: Excel template

```yaml
template_id: sales_excel
version: 1

file_type: excel
file_pattern: ".*incoming/sales_.*\\.xlsx$"

sheets:
  - name: Sheet1
    required: true
    header_row: 1

    columns:
      order_id:
        required: true
        type: integer
      amount:
        required: true
        type: decimal
      created_at:
        required: true
        type: date

    expectation_suite:
      - sales_excel_checks
```
### 🟦 Example: Parquet template

```yaml
template_id: sales_parquet
version: 1

file_type: parquet
file_pattern: ".*incoming/sales_.*\\.parquet$"

sheets:
  - name: data
    required: true

    columns:
      order_id:
        required: true
      amount:
        required: true
      created_at:
        required: true

    expectation_suite:
      - sales_parquet_checks
```

### 🟦 Example: Iceberg template

```yaml
template_id: orders_iceberg
version: 2
file_type: iceberg
file_pattern: "^iceberg://(glue\\.)?dq_iceberg_dev\\.[a-zA-Z0-9_]+$"

sheets:
  - name: data
    required: true

    columns:
      id:
        required: true
        type: int
      order_amount:
        required: true
        type: decimal

    rules:
      - name: not_null_required
      - name: positive
        columns: [order_amount]
      - name: unique
        columns: [id]

    expectation_suite:
      - orders_iceberg_checks_v2
```

## Create the expectation suite

### Generate the expectation suite from dataset

### Excel
```powershell
python -m scripts.create_expectation_suite `
  --dataset s3://bucket/path/file.xlsx `
  --template-id sales_excel `
  --sheet-name Sheet1 `
  --suite-name sales_excel_checks
```

### Parquet
```powershell
python -m scripts.create_expectation_suite `
  --dataset s3://bucket/path/file.parquet `
  --template-id sales_parquet `
  --sheet-name data `
  --suite-name sales_parquet_checks
```
### Iceberg

```bash
python -m scripts.create_expectation_suite \
  --dataset iceberg://glue.dq_iceberg_dev.orders \
  --template-id orders_iceberg \
  --sheet-name data \
  --suite-name orders_iceberg_checks_v2
```  

⚠️ Important

Rules defined in templates are **not executed during validation**.

If you update rules:
- You MUST regenerate the expectation suite
- Otherwise runtime behaviour will not change

---

## ✅ Step 4 — Run validation

Run the validation engine against the dataset.

### Example

```bash
docker run --rm \
  -e DB_HOST=$DB_HOST \
  -e DB_NAME=$DB_NAME \
  -e DB_USER=$DB_USER \
  -e DB_PASSWORD=$DB_PASSWORD \
  -e RESULTS_BUCKET=dataquality-poc-validation-results \
  data-validation-engine \
  --dataset s3://bucket/path/file.parquet
```  

For Iceberg datasets:

```bash
docker run --rm \
  -e DB_HOST=$DB_HOST \
  -e DB_NAME=$DB_NAME \
  -e DB_USER=$DB_USER \
  -e DB_PASSWORD=$DB_PASSWORD \
  data-validation-engine \
  --dataset iceberg://glue.dq_iceberg_dev.orders
```

## ✅ Step 5 — Verify results

After execution, verify validation results in:

### PostgreSQL
- `dq.validation_runs`
- `dq.validation_rule_results`
- `dq.structural_validation_results`

### S3 (file-based datasets only)
- `validation-results/` – JSON validation reports
- `passes/` or `failed/` – archived input datasets

⚠️ Iceberg datasets do **not** produce archived input files.

---

## Common Iceberg Pitfalls

| Issue | Cause | Fix |
|-----|------|-----|
| DATE column fails type check | Pandas object dtype | Use `date_type` rule |
| Unexpected type expectations | GE auto-generated | Suite auto-cleans these |
| File regex doesn’t match | Missing iceberg:// prefix | Update file_pattern |
