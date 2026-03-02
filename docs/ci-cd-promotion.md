# CI/CD and Promotion Model

This document describes the Continuous Integration (CI) and Continuous Delivery (CD)
model used by the Data Quality Validation Framework.

The design is intentionally **environment-safe**, **auditable**, and **infrastructure-agnostic**.
It does not assume any specific database, BI tool, or deployment platform.

---

## Goals

- Validate code and templates automatically on every change
- Prevent accidental deployments to higher environments
- Enable controlled, manual promotion of verified artifacts
- Maintain a clear audit trail of what was promoted and when
- Support multiple customer deployment models (ECS, Kubernetes, on-prem, etc.)

---

## Environments

The framework supports three logical environments:

| Environment | Purpose | Trigger |
|-----------|--------|--------|
| **dev** | Continuous validation and feedback | Automatic (PR / push) |
| **staging** | Pre-production validation | Manual promotion |
| **prod** | Production | Manual promotion + approval |

---

## CI Flow (Automatic)

### Trigger
- Pull request
- Push to `main`

### Scope
- **Dev only**

### Actions
- Lint Python and YAML
- Validate template structure
- Resolve templates against example datasets
- Build Docker image
- Smoke test container startup

### What does *not* happen
- No Docker image push
- No deployment
- No release creation
- No staging or production interaction

This guarantees that CI is **safe by default**.

---

## Promotion Flow (Manual)

Promotion to higher environments is always explicit and controlled.

### Promotion Characteristics

- Triggered manually using GitHub Actions
- Requires an explicit Git reference (commit SHA or tag)
- Reuses the same CI pipeline logic
- Produces immutable deployment metadata

---

## Promotion to Staging

### How
- Run **Promote to Staging** workflow manually
- Provide the Git commit or tag to promote

### Actions
- Re-run validation on the selected commit
- Build Docker image
- Push Docker image to registry
- Generate deployment metadata artifact

### Safeguards
- No automatic triggers
- No production access
- Fully auditable

---

## Promotion to Production

### How
- Run **Promote to Production** workflow manually
- Provide the Git commit or tag to promote
- Requires environment approval (GitHub Environments)

### Actions
- Re-run validation on the selected commit
- Build Docker image
- Push Docker image to registry
- Generate deployment metadata
- Create a GitHub Release

### Safeguards
- Manual trigger
- Explicit approval
- Immutable release record

---

## Deployment Metadata (Artifacts)

For staging and production, each promotion produces a deployment artifact:

```json
{
  "service": "data-quality-validation-engine",
  "environment": "prod",
  "image": "data-quality-validation-engine",
  "image_tag": "<git-sha>",
  "git_ref": "<commit-or-tag>",
  "build_time": "2026-03-01T18:47:07Z",
  "ci_run_id": "<github-run-id>"
}
```
