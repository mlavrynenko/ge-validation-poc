# 📘 Onboarding a New Dataset into the Data Quality System

This document describes the **required steps** to onboard a new **Excel or Parquet** dataset into the Data Quality (DQ) validation system.

The process is **deterministic, auditable, and production-safe**.

---

## 🧭 Overview

For **every new dataset**, you must complete the following steps:

1. Understand the dataset
2. Create a validation template (YAML)
3. Create a Great Expectations suite
4. Run validation
5. Verify results (PostgreSQL + S3)

---

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

    expectations:
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

    expectations:
      - sales_parquet_checks
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
