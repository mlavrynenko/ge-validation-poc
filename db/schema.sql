CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.structural_validation_results (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    dataset TEXT NOT NULL,
    template_id TEXT NOT NULL,
    template_version INTEGER NOT NULL,
    sheet_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    error_count INTEGER NOT NULL,
    warning_count INTEGER NOT NULL,
    errors JSONB NOT NULL,
    warnings JSONB NOT NULL,
    validated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS dq.validation_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    dataset TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    validated_at TIMESTAMP NOT NULL,
    row_count INTEGER,
    validation_duration_ms INTEGER,
    rules_total INTEGER,
    rules_passed INTEGER,
    rules_failed INTEGER,
    quality_score NUMERIC,
    null_ratio NUMERIC,
    duplicate_ratio NUMERIC,
    schema_changed BOOLEAN,
    invalid_row_count INTEGER
);

CREATE TABLE IF NOT EXISTS dq.validation_rule_results (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    validated_at TIMESTAMP NOT NULL,
    dataset TEXT NOT NULL,
    expectation_type TEXT NOT NULL,
    column_name TEXT,
    success BOOLEAN NOT NULL,
    unexpected_count INTEGER
);

CREATE OR REPLACE VIEW dq.v_validation_runs AS
SELECT
    vr.run_id,
    vr.dataset,
    vr.success,
    vr.validated_at,
    vr.row_count,
    vr.validation_duration_ms,
    vr.rules_total,
    vr.rules_passed,
    vr.rules_failed,
    vr.quality_score,
    vr.null_ratio,
    vr.duplicate_ratio,
    vr.schema_changed,
    vr.invalid_row_count,
    vr.dataset AS input_dataset
FROM dq.validation_runs vr;

CREATE OR REPLACE VIEW dq.v_validation_metrics AS
SELECT
    vr.run_id,
    vr.dataset,
    vr.validated_at,

    -- Structural validation
    BOOL_AND(svr.passed)                    AS structural_passed,
    SUM(svr.error_count)                    AS structural_error_count,
    SUM(svr.warning_count)                  AS structural_warning_count,

    -- GE metrics
    vr.rules_total,
    vr.rules_passed,
    vr.rules_failed,
    vr.quality_score,
    vr.schema_changed,
    vr.invalid_row_count,

    -- Health flags
    CASE
        WHEN vr.rules_failed = 0
         AND BOOL_AND(svr.passed)
        THEN true
        ELSE false
    END AS overall_passed

FROM dq.validation_runs vr
LEFT JOIN dq.structural_validation_results svr
    ON vr.run_id = svr.run_id
GROUP BY
    vr.run_id,
    vr.dataset,
    vr.validated_at,
    vr.rules_total,
    vr.rules_passed,
    vr.rules_failed,
    vr.quality_score,
    vr.schema_changed,
    vr.invalid_row_count;

CREATE OR REPLACE VIEW dq.v_validation_rule_stats AS
SELECT
    run_id,
    dataset,
    expectation_type,
    column_name,
    COUNT(*) FILTER (WHERE success)     AS passed_count,
    COUNT(*) FILTER (WHERE NOT success) AS failed_count,
    SUM(unexpected_count)               AS unexpected_total
FROM dq.validation_rule_results
GROUP BY
    run_id,
    dataset,
    expectation_type,
    column_name;
