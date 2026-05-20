# instant-worker — Claude Code Project Config

Background-jobs service. Runs River (Postgres-native queue) on top of the
platform DB. Owns every async / scheduled side-effect that the agent-facing
API would block on if it tried to do them in-request.

## Quick reference

- `make build` — compile all packages
- `make test` — full test run (race, count=1)
- `make gate` — **pre-push/pre-commit gate**. Runs the EXACT command
  sequence CI's `.github/workflows/deploy.yml` runs as its test step
  (`go build ./... && go vet ./... && go test ./... -short -count=1`).
  A green `make gate` locally == a green CI test step. Per CLAUDE.md
  (root) rule 23, this must pass before commit/push.
- `make docker-build` — Docker image with `GIT_SHA` / `BUILD_TIME` /
  `VERSION` build args wired into `instant.dev/common/buildinfo`.
- `make smoke-buildinfo` — proves the `-ldflags` injection path still
  works end-to-end (CI runs this on every PR).
- `make chaostest-propagation` — propagation_runner retry / dead-letter
  drill (live cluster; see `CHAOS-DRILL-2026-05-20.md` at repo root).
- `make chaostest-lease-recovery` — worker pod OOMKill / lease-takeover
  drill (see same doc).

## What lives where

```
worker/
├── cmd/                       ← per-binary mains (smoke-buildinfo helper, etc.)
├── docs/                      ← per-job design notes
├── internal/
│   ├── jobs/                  ← ~50 source files. Every job ends in *_job.go-style
│   │                            with a worker.River pattern + companion *_test.go.
│   ├── ...                    ← (logctx wrappers, k8s client, db helpers)
│   └── ...
├── sql/                       ← internal SQL helpers (worker-side, not migrations;
│                                 migrations live in api/internal/db/migrations/)
└── main.go                    ← River boot + job registration
```

Source-of-truth headline jobs (in scope for routine edits):

| Job | File | Purpose |
|---|---|---|
| `propagation_runner` | `internal/jobs/propagation_runner.go` | Out-of-band side-effect runner with maxAttempts + dead-letter table. **Chaos-tested 2026-05-20.** `instant_propagation_dead_lettered_total` Prometheus counter wired. |
| `expire` / `expire_imminent` / `expire_stacks` | `internal/jobs/expire*.go` | Anonymous + Free-tier 24h TTL reaper + 6/2/1h warning emails. Comprehensive Go-rendered email per Rule 12 (no Brevo templates). |
| `entitlement_reconciler` | `internal/jobs/entitlement_reconciler.go` | Re-grade resources after tier change. Reads from `resource.tier`, NOT `team.plan_tier` (the PR #175 fix). All arms (Postgres / Redis / Mongo) implemented. |
| `quota` / `quota_infra` / `quota_redis_eviction` / `quota_wall_nudge` | `internal/jobs/quota*.go` | Per-resource usage scans against shared infra. The 80% upsell nudge fires once per resource. |
| `billing_reconciler` / `checkout_reconcile` | `internal/jobs/billing*.go` `internal/jobs/checkout_reconcile.go` | Razorpay subscription state-machine repair + pending-checkout reconciliation. |
| `deploy_status_reconcile` / `deploy_notify_webhook` / `deploy_failure_autopsy` / `deployment_expirer` / `deployment_reminder` | `internal/jobs/deploy*.go` `internal/jobs/deployment*.go` | Deploy lifecycle: status sync, webhook fan-out, failure-cause classification, app expiry. |
| `orphan_sweep_reconciler` / `orphan_sweep_canceler` | `internal/jobs/orphan_sweep*.go` | k8s namespace reaper. PASS 3 enhanced reasons + PASS 6 stuck-build state (covers ImagePullBackOff at 9h). |
| `event_email_forwarder` / `event_email_mapping` / `lifecycle_emails` / `expiry_reminder` | `internal/jobs/event_email*.go` `internal/jobs/lifecycle_emails.go` `internal/jobs/expiry_reminder.go` | Comprehensive Go-rendered transactional email. All 18+ kinds. See `expiry_reminder.brevo-template.md` for the historical (now-deprecated) Brevo template. |
| `customer_backup_runner` / `customer_backup_scheduler` / `customer_restore_runner` / `backup_audit` / `backup_s3` / `platform_db_backup` | `internal/jobs/customer_backup*.go` `internal/jobs/backup*.go` `internal/jobs/platform_db_backup*.go` | Per-tenant backup ladder + platform DB backup. |
| `team_deletion_executor` (+ audit_kinds, s3_adapter) / `pending_deletion_expirer` | `internal/jobs/team_deletion*.go` `internal/jobs/pending_deletion_expirer.go` | Purge orchestration. k8s namespace teardown lives here (PR #135). |
| `magic_link_reconciler` / `payment_grace_reminder` / `payment_grace_terminator` | `internal/jobs/magic_link_reconciler.go` `internal/jobs/payment_grace*.go` | Auth + payment-failure flows. |
| `provisioner_reconciler` / `razorpay_webhook_prune` / `chaos_lease_recovery` / `resource_heartbeat` | `internal/jobs/provisioner_reconciler.go` `internal/jobs/razorpay_webhook_prune.go` `internal/jobs/chaos_lease_recovery.go` `internal/jobs/resource_heartbeat.go` | Operational reconciliation. |
| `prober` / `real_prober` / `uptime_prober` | `internal/jobs/prober.go` `internal/jobs/real_prober.go` `internal/jobs/uptime_prober.go` | Synthetic health checks. |
| `churn_predictor` | `internal/jobs/churn_predictor.go` | Heuristic risk scoring. |
| `geodb` | `internal/jobs/geodb.go` | MaxMind GeoLite2 refresh. |
| `custom_domain_reconcile` | `internal/jobs/custom_domain_reconcile.go` | cert-manager / ingress reconciliation for Pro+ custom hostnames. |
| `storage` / `storage_minio` | `internal/jobs/storage.go` `internal/jobs/storage_minio.go` | Quota scan against object-store backend (DO Spaces live; MinIO legacy). |

## Conventions (worker-specific, on top of root CLAUDE.md)

1. **Idle ticks are DEBUG.** A job that wakes up on its schedule, finds
   nothing to do, and exits should log at DEBUG. INFO is reserved for
   work performed. See `worker/internal/jobs/quota.go` and the W4 ticket
   pattern (`entitlement_reconciler.Mongo arm` was the historical noisy
   surface — silenced 2026-05-19).

2. **Resource bearer tokens MUST be masked in logs.** Worker T21 P1-2.
   Use the masking helper, never raw `resource.token`.

3. **Job timeout via River JobTimeout.** Worker T20 P1. Every job gets a
   timeout so a stuck Postgres/HTTP call cannot wedge the River pool.

4. **Drain before cancel.** Worker T20 P0-2 / P1-3. `Workers.Stop` calls
   `Drain` first so in-flight work commits before the context cancels.

5. **`resource.tier`, not `team.plan_tier`.** Entitlement reconciler reads
   per-resource tier (Worker T8 P1-1). A user with old Pro resources and
   a downgraded team-tier-Hobby keeps the Pro grade on those resources
   until the next provision.

6. **Forwarder claim-after-2xx.** Worker MR-P1-16. The forwarder claims
   the row only AFTER a 2xx from Brevo, not before. This avoids the
   "ledger marked sent, network call failed" hole.

7. **Email masking in worker providers.** Worker T22 P1-1. Stub the
   right side of the `@`.

## Chaos drills (run on real cluster)

- `make chaostest-propagation` — flushes the propagation queue with
  malformed kinds, asserts dead-letter ceiling + the `unknown_kind`
  escape route (CHAOS F2 fix).
- `make chaostest-lease-recovery` — SIGKILL a worker pod, measure RTO
  for River lease takeover by the surviving pod. See CHAOS F5 ticket
  for live RTO measurement gating on the next image rebuild.

## Auto-deploy

Worker auto-deploys on push to `master` via `.github/workflows/deploy.yml`.
Verify with `kubectl get pod -n instant-infra -l app=instant-worker -o jsonpath='{.items[0].spec.containers[0].image}'` after rollout. The image
tag (e.g. `master-<sha>`) must match `git rev-parse --short HEAD`.

## When in doubt

The root `/Users/manassrivastava/Documents/InstaNode/CLAUDE.md` covers
shared conventions, agent-reliability rules (1–23), and the four-pass
deploy ritual. **This file is the worker-specific delta only.** When a
rule appears in both, root wins.
