# worker — InstaNode background-job processor

Part of the [InstaNode](https://instanode.dev) platform. This repository runs the asynchronous side of the platform — the jobs that don't fit in a request handler because they take too long, need to retry on failure, or run on a schedule.

## What this does

- Reaps anonymous + free-tier resources at their TTL, with warning emails at 6h / 2h / 1h
- Reconciles tier-grade changes after a Razorpay subscription event (`entitlement_reconciler`)
- Scans per-resource storage usage every minute and emits 80% upgrade nudges (`quota_wall_nudge`)
- Reaps orphan deploy/stack namespaces left behind by failed builds (`orphan_sweep_reconciler`, 6 passes)
- Runs the eager-retry consumer for `pending_propagations` with bounded backoff + dead-letter (`propagation_runner`)
- Backs up customer postgres + mongodb databases on a schedule (`customer_backup_runner`); restores on demand (`customer_restore_runner`)
- Forwards every `audit_log` row that has an associated email template to Brevo + records the delivery classification on the `forwarder_sent` ledger
- Repairs Razorpay subscription/checkout state (`billing_reconciler`, `checkout_reconcile`) and handles payment-grace expiry (`payment_grace_terminator`)
- Synthesises uptime probes against customer Postgres/Redis/Mongo (`real_prober`, `uptime_prober`)

Job-runner is [River](https://riverqueue.com/) (Postgres-native queue), scheduled by River cron + manual triggers from the api repo.

## Architecture

```
api/  ───emits──>  audit_log + pending_propagations
                          │
                          ▼
worker/  ───consumes────  River queue + scheduled jobs
                          │
                          ├──> Brevo (email)
                          ├──> Razorpay (subscription state)
                          ├──> provisioner gRPC (k8s namespace teardown)
                          └──> DigitalOcean Spaces (backup storage)
```

worker shares Go types and helpers with `api` and `provisioner` through the [`common`](https://github.com/InstaNode-dev/common) repo (queue providers, storage providers, plans registry, readiness checks, logging context).

## Quick start

```bash
git clone https://github.com/InstaNode-dev/worker
cd worker
go build ./...
go vet ./...
go test ./... -short -p 1
```

Local development requires postgres (platform DB + customer DB), redis, and access to the api repo's `customer_backup_scheduler` table schema. See `infra/docker-compose.yml` for a one-command local stack.

## Configuration

Driven entirely by environment variables — `internal/config/config.go` is the source of truth. Key ones:

- `DATABASE_URL` — platform DB connection string (sslmode=require for managed PG)
- `CUSTOMER_DATABASE_URL` — customer DB connection (separate cluster)
- `REDIS_URL` — platform Redis
- `PROVISIONER_ADDR` — gRPC endpoint of the provisioner service
- `BREVO_API_KEY` — transactional email
- `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` — billing
- `OBJECT_STORE_BACKEND` + `OBJECT_STORE_*` — DO Spaces / R2 / S3 / MinIO selection (see `common/storageprovider/`)
- `NEW_RELIC_LICENSE_KEY` — observability

See `.env.example` for the full list and safe development defaults.

## Running locally

```bash
DATABASE_URL=postgres://localhost/instant_platform?sslmode=disable \
  REDIS_URL=redis://localhost:6379 \
  go run .
```

The worker starts a River pool, registers ~50 job kinds, and serves a deep `/readyz` on `:8091` (covers platform_db, redis-provision, brevo reachability, River health) and a shallow `/healthz`.

## Observability

- Prometheus metrics at `/metrics` on `:8091` — see `infra/observability/METRICS-CATALOG.md` for the full registry
- structured JSON logs via slog with `request_id` + `commit_id` propagation (`common/logctx`)
- OpenTelemetry traces exported to New Relic (when `NEW_RELIC_LICENSE_KEY` is set)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). License is MIT — see [LICENSE](LICENSE).

For platform-wide issues, file at the [api repo](https://github.com/InstaNode-dev/api/issues). For worker-specific bugs, file here.

## Security

See [SECURITY.md](SECURITY.md) for the responsible-disclosure policy.
