---

# Promotion Guide

This document explains **how to promote a validated version**
of the Data Quality Validation Framework to staging or production.

Promotion is always **manual and explicit**.

---

## When Should I Promote?

You should promote when:

- CI has passed on `main`
- Templates and rules are validated
- A specific commit or release is approved for the next environment

---

## Promote to Staging

### Steps

1. Open GitHub Actions
2. Select **Promote to Staging**
3. Click **Run workflow**
4. Enter:
   - Commit SHA or tag to promote

### What Happens

- Validation is re-run for the selected commit
- Docker image is built
- Docker image is pushed to registry
- Deployment metadata artifact is generated

### Result

- Staging-ready artifact
- No production impact
- Full audit trail

---

## Promote to Production

### Steps

1. Open GitHub Actions
2. Select **Promote to Production**
3. Click **Run workflow**
4. Enter:
   - Commit SHA or tag to promote
5. Approve deployment (if required)

### What Happens

- Validation is re-run
- Docker image is built and pushed
- Deployment metadata is generated
- GitHub Release is created

### Result

- Production-ready artifact
- Immutable release record
- Auditable deployment evidence

---

## Required Inputs

| Input | Description |
|----|-----------|
| `git-ref` | Commit SHA or Git tag to promote |

---

## Safety Guarantees

- No promotion happens automatically
- No environment is modified without intent
- Production requires explicit approval
- Each promotion is traceable to a commit

---

## Frequently Asked Questions

### Can promotion deploy to infrastructure automatically?
No. Deployment is intentionally decoupled.  
The framework produces artifacts that downstream systems deploy.

---

### Can we integrate with our own platform?
Yes. The promotion model works with:
- Kubernetes
- ECS
- VM-based systems
- On-prem deployments
- GitOps workflows

---

### Can we skip staging?
That is a policy decision.  
The framework supports it but does not enforce it.

---

## Summary

Promotion is:
- Manual
- Controlled
- Auditable
- Reversible

This ensures confidence when moving between environments.
