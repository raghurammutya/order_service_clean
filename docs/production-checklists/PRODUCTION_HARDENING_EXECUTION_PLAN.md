# Production Hardening Execution Plan

Last updated: 2026-02-14 (UTC)
Scope: `user_service`, `config_service`, `ticker_service`, `token_manager`, `instrument_registry`, `comms_service`, `alert_service`, `message_service`, `calendar_service`

This file is the session-to-session source of truth for hardening progress.

## How to use this file each session

1. Read this file first.
2. Pick top pending item from `Immediate` then `Short Term`.
3. Execute, validate, and update status/evidence in this file.
4. Commit updates with code changes in the same PR/commit.

## Status Legend

- `[ ]` Not started
- `[~]` In progress
- `[x]` Completed
- `[-]` Deferred / accepted risk

## Immediate (0-7 days)

- [ ] Enforce clean-tree release rule and document release branch policy. (Issue: #1)
  - Owner: DevOps
  - Evidence: CI/pre-release check output attached in commit/PR
- [ ] Add mandatory CI gates: unit tests, smoke tests, runtime import checks, migration check. (Issue: #2)
  - Owner: Dev Lead + QA
  - Evidence: workflow files + passing run links
- [ ] Run secret scan across repo and rotate exposed credentials. (Issue: #3)
  - Owner: Security + Platform
  - Evidence: scan report + rotation checklist
- [ ] Formalize `comms_service` vs `message_service` boundary and update docs/contracts. (Issue: #4)
  - Owner: Architect + Dev Lead
  - Evidence: architecture doc update + API/event contract notes
- [ ] Publish top-10 incident runbook with exact commands and rollback paths. (Issue: #5)
  - Owner: Support + SRE
  - Evidence: runbook markdown under `docs/`
- [ ] Add critical business-flow daily checks (not only `/health`). (Issue: #6)
  - Owner: QA + SRE
  - Evidence: scheduled check script + alerting config

## Short Term (2-6 weeks)

- [ ] Define service SLOs (availability, p95/p99, error budget) and alerts. (Issue: #7)
  - Owner: Architect + SRE
  - Evidence: SLO doc + dashboard links
- [ ] Build mandatory E2E suite for: (Issue: #8)
  - `user_service -> token_manager` credential flow
  - token refresher schedule/recovery
  - comms delivery (SMTP + Telegram)
  - calendar/ticker market-session path
  - Owner: QA + Dev Lead
  - Evidence: CI job + test reports
- [ ] Standardize DB migrations + rollback scripts for all scoped services. (Issue: #9)
  - Owner: Dev Lead + DBA
  - Evidence: migration directories + dry-run logs
- [ ] Centralize observability (structured logs, correlation IDs, traces, actionable alerts). (Issue: #10)
  - Owner: SRE + Cloud
  - Evidence: dashboards and alert policies
- [ ] Enforce release signoff checklist (Architecture, QA, Security, DevOps). (Issue: #11)
  - Owner: PM + Engineering Managers
  - Evidence: signed checklist artifact per release

## Medium Term (6-12 weeks)

- [ ] Replace static service API keys with workload identity (mTLS or short-lived service JWT). (Issue: #12)
  - Owner: Security + Platform
  - Evidence: auth design doc + rollout report
- [ ] Implement resilience patterns consistently (timeouts, retries, circuit breakers, bulkheads). (Issue: #13)
  - Owner: Dev Lead
  - Evidence: code/config references and resilience test results
- [ ] Validate horizontal scaling + autoscaling behavior under load. (Issue: #14)
  - Owner: Cloud + SRE
  - Evidence: load test and scaling report
- [ ] Reduce synchronous coupling via event-driven patterns for non-blocking flows. (Issue: #15)
  - Owner: Architect + Dev Lead
  - Evidence: architecture updates + latency comparison
- [ ] Enforce encrypted off-host backups and monthly restore drills. (Issue: #16)
  - Owner: SRE + Security
  - Evidence: backup replication logs + drill report

## Long Term (3-6 months)

- [ ] Capacity program: peak, stress, soak, and failure-injection testing cadence. (Issue: #17)
  - Owner: QA Perf + SRE
  - Evidence: quarterly performance pack
- [ ] Multi-zone failover validation against RTO/RPO. (Issue: #18)
  - Owner: Cloud + SRE
  - Evidence: DR test report
- [ ] Data scaling improvements (cache strategy, partitioning/read replicas where needed). (Issue: #19)
  - Owner: Architect + DBA
  - Evidence: data architecture decision records
- [ ] Security maturity uplift (continuous vuln scanning, dependency governance, access anomaly detection). (Issue: #20)
  - Owner: Security
  - Evidence: monthly security scorecard
- [ ] Platform governance (artifact immutability, API compatibility policy, formal change management cadence). (Issue: #21)
  - Owner: PMO + Engineering Leadership
  - Evidence: governance docs and release audit trail

## Current Session Queue (next 5)

1. Add CI runtime import/dependency validation for `instrument_registry`. (Issue: #2)
2. Add repo-level secret scan workflow and baseline suppression policy. (Issue: #3)
3. Create `comms_service` and `message_service` responsibility contract doc. (Issue: #4)
4. Add daily synthetic critical-flow check script and schedule. (Issue: #6)
5. Prepare release signoff checklist template with required approvers. (Issue: #11)

## Weekly Review Cadence

- Every Friday UTC:
  - Update status checkboxes
  - Move completed evidence links into this file
  - Re-rank top 5 queue by production risk
