# Runbook: Platform DB Backup & Restore

**Owner:** Platform / infra on-call
**Last reviewed:** 2026-05-13
**Severity if missing:** P0 — without this backup, a managed-Postgres incident on `instant_platform` makes the entire product unrecoverable.

This runbook covers the nightly platform-DB backup job (`platform_db_backup`), how to restore from a backup, how to drill the restore monthly, and what to do when something goes wrong.

---

## 1. What's in scope

| In backup | Not in backup |
|---|---|
| `instant_platform` Postgres — teams, users, resources, audit_log, onboarding_events, billing rows, custom_domains, deployments, stacks, vault, every other platform table | Customer Postgres DBs (`instant_customers` cluster) — covered by **W5-B-worker** in a separate nightly job |
|  | Customer Redis caches — by design ephemeral; treated as cache, not source of truth |
|  | Customer MongoDB collections — covered by W5-B-worker |
|  | Object storage data (DO Spaces buckets) — bucket has lifecycle rules; the data IS the customer copy and cannot be restored from us |
|  | Application secrets (k8s Secrets, OBJECT_STORE creds, etc.) — restored from secrets-management vault, NOT from this dump |

**If the platform DB is lost AND any of the items in the right column are also lost, you do NOT have full coverage from this backup alone.** Track 5C (multi-region replication) is the long-term answer; this job is the immediate floor.

---

## 2. Where the backups live

- **Bucket:** `BACKUP_S3_BUCKET` env var on the worker (default `instant-shared`)
- **Endpoint:** `OBJECT_STORE_ENDPOINT` env var (production: `nyc3.digitaloceanspaces.com`)
- **Prefix:** `${BACKUP_S3_PATH_PREFIX}${PLATFORM_BACKUP_S3_PREFIX}` — production resolves to `platform-backups/`
- **Object key pattern:** `platform-backups/YYYY-MM-DD/platform.dump.gz`
- **Format:** `pg_dump --format=custom --compress=9` (zlib-compressed pg_restore-compatible binary). The `.gz` suffix is operationally conventional; `gunzip` will NOT work — go straight to `pg_restore`.

### How to list available backups

```bash
# DO Spaces / AWS S3 CLI
aws --endpoint-url https://nyc3.digitaloceanspaces.com \
    s3 ls s3://instant-shared/platform-backups/ --recursive | sort

# Most recent backup:
aws --endpoint-url https://nyc3.digitaloceanspaces.com \
    s3 ls s3://instant-shared/platform-backups/ --recursive \
    | sort | tail -1
```

---

## 3. How to restore (production scenario)

**Estimated wall time at current DB size (~2 GB compressed → ~12 GB uncompressed, as of 2026-05-13):** 8-15 minutes end-to-end.

Steps:

```bash
# 0. Pre-flight: confirm you have the target DB credentials and the
# target is EMPTY (or you have permission to --clean it). Make sure
# you have psql 16+ and pg_restore 16+ available locally.

# 1. Find the backup you want.
aws --endpoint-url https://nyc3.digitaloceanspaces.com \
    s3 ls s3://instant-shared/platform-backups/ --recursive | sort | tail -10

# 2. Download it.
DATE=2026-05-13   # or whichever
aws --endpoint-url https://nyc3.digitaloceanspaces.com \
    s3 cp s3://instant-shared/platform-backups/${DATE}/platform.dump.gz \
    ./platform.dump.gz

# 3. Verify file looks sane.
ls -lh platform.dump.gz
# Expect: 100MB-2GB range for a healthy production DB.
# If it's < 1MB, that is almost certainly a torn / failed upload —
# pick an earlier date.

# 4. Restore directly via pg_restore. NOTE: the file is in pg_dump
# custom format which is zlib-compressed; do NOT gunzip first.
TARGET_DATABASE_URL="postgres://user:pass@host:5432/instant_platform"
pg_restore \
    --clean --if-exists \
    --no-owner --no-acl \
    --dbname="${TARGET_DATABASE_URL}" \
    --jobs=4 \
    --verbose \
    ./platform.dump.gz

# 5. Sanity check.
psql "${TARGET_DATABASE_URL}" <<'SQL'
SELECT COUNT(*) AS teams FROM teams;
SELECT COUNT(*) AS resources FROM resources WHERE status = 'active';
SELECT COUNT(*) AS audit_rows_24h FROM audit_log
    WHERE created_at > NOW() - INTERVAL '24 hours';
SELECT MAX(created_at) AS most_recent_audit FROM audit_log;
SQL
```

**Critical post-restore tasks (in order):**

1. **Pause / drain the worker** until you've decided whether to replay any audit rows or webhook events that landed between the backup time and the restore time. Otherwise you risk a double-fire on retention/expiry jobs.
2. **Re-run gRPC migrations** if the platform code is ahead of the backup's schema — `migrate up`.
3. **Verify the live agent API** can hit the DB before un-pausing dashboard traffic.
4. **Tell the user** if the gap between backup and restore exceeds 1 hour — any provisioning, claim, or webhook event landed in that window is lost. Look in `agent_api` logs / Razorpay webhook DLQ for what may need replaying.

---

## 4. Monthly restore drill (REQUIRED — first Wednesday of each month)

The point of a backup is the restore. An untested backup is a wish.

### Drill procedure

```bash
# 1. Pick yesterday's backup (today's may not have landed yet at 02:00).
DATE=$(date -u -v-1d +%Y-%m-%d 2>/dev/null || date -u -d 'yesterday' +%Y-%m-%d)

# 2. Spin a temporary Postgres in the sandbox namespace.
kubectl create namespace backup-drill-$(date +%s) || true
NS=backup-drill-$(date +%s)
kubectl run pg-drill -n ${NS} \
    --image=postgres:16 \
    --env=POSTGRES_PASSWORD=drill \
    --env=POSTGRES_DB=drill \
    --port=5432
kubectl wait --for=condition=Ready pod/pg-drill -n ${NS} --timeout=120s
kubectl port-forward -n ${NS} pod/pg-drill 5440:5432 &
PF_PID=$!
sleep 3

# 3. Download + restore yesterday's backup.
aws --endpoint-url https://nyc3.digitaloceanspaces.com \
    s3 cp s3://instant-shared/platform-backups/${DATE}/platform.dump.gz \
    /tmp/drill-${DATE}.dump.gz

pg_restore \
    --clean --if-exists --no-owner --no-acl \
    --dbname="postgres://postgres:drill@localhost:5440/drill" \
    --jobs=2 \
    /tmp/drill-${DATE}.dump.gz

# 4. Sanity-check counts. Expected: each count > 0 and within
# 10% of yesterday's production-DB equivalents from the
# dashboard.
psql "postgres://postgres:drill@localhost:5440/drill" <<'SQL'
SELECT 'teams' AS what, COUNT(*) FROM teams
UNION ALL
SELECT 'resources', COUNT(*) FROM resources
UNION ALL
SELECT 'audit_log', COUNT(*) FROM audit_log
UNION ALL
SELECT 'users', COUNT(*) FROM users;
SQL

# 5. Record drill outcome in #ops-runbooks Slack channel.
# Template: "Drill ${DATE}: teams=N, resources=N, audit=N, users=N.
# Drift from prod: X%. Time-to-first-row: Ns."

# 6. Tear down.
kill ${PF_PID} 2>/dev/null
kubectl delete namespace ${NS}
rm -f /tmp/drill-${DATE}.dump.gz
```

If the drill fails (download error, restore error, wildly-off counts, missing critical tables):

1. **DO NOT** silence the New Relic alert that you may temporarily have muted for the drill.
2. Open a P1 issue against `instant-platform` repo with title "Platform backup drill FAILED — ${DATE}".
3. Investigate via Section 5 below.

---

## 5. What to do if a backup is missing or stale

### 5.1 New Relic alert paths

The worker emits three audit kinds that NR ingests via the dashboards in Section 6:

| Kind | NR widget | Alert threshold |
|---|---|---|
| `platform_backup.started` | "Backup runs/day" counter | None — informational |
| `platform_backup.succeeded` | "Time since last successful platform backup" | CRITICAL: > 36 hours → page on-call |
| `platform_backup.failed` | "Backup failures/24h" | CRITICAL: ≥ 1 failure in 24h → page on-call. ≥ 2 consecutive failures → page director |

### 5.2 If "no successful backup in 36 hours" fires

```bash
# 1. Is the worker pod healthy?
kubectl get pods -n instant-infra | grep instant-worker

# 2. Is OBJECT_STORE_* configured?
kubectl exec -n instant-infra deploy/instant-worker -- env | grep -E 'OBJECT_STORE|BACKUP_S3'

# 3. Did the last few attempts even fire? Look in worker logs for
# either platform_db_backup.* slog lines.
kubectl logs -n instant-infra deploy/instant-worker --tail=2000 \
    | grep -i platform_db_backup

# 4. Is pg_dump installed in the container?
kubectl exec -n instant-infra deploy/instant-worker -- which pg_dump
# If missing: the worker image dropped pg_dump. Roll forward with a
# patched Dockerfile that pins postgresql-client-16.

# 5. Can the worker reach the DB? Try a manual probe.
kubectl exec -n instant-infra deploy/instant-worker -- \
    pg_dump --format=custom --no-owner --no-acl --schema-only \
    "${DATABASE_URL}" > /tmp/probe.dump
ls -lh /tmp/probe.dump
# Non-zero size → DB connectivity is fine, audit the S3 path next.

# 6. Can the worker reach S3?
kubectl exec -n instant-infra deploy/instant-worker -- \
    sh -c 'aws --endpoint-url https://${OBJECT_STORE_ENDPOINT} \
    s3 ls s3://${BACKUP_S3_BUCKET}/'
```

### 5.3 Manual one-shot backup (emergency)

If the automated job is broken and you need a backup NOW:

```bash
# From any pod with DATABASE_URL and pg_dump:
DATE=$(date -u +%Y-%m-%d)
pg_dump --no-owner --no-acl --format=custom --compress=9 "${DATABASE_URL}" \
    | aws --endpoint-url https://nyc3.digitaloceanspaces.com \
      s3 cp - "s3://instant-shared/platform-backups/${DATE}-MANUAL/platform.dump.gz"
```

The `-MANUAL` suffix on the date directory keeps the retention sweep from
deleting your manual snapshot on its next monthly pass (the regex
`\d{4}-\d{2}-\d{2}/` matches your key, but the sweep keeps unparseable
date components conservatively when the day component doesn't parse —
see `computeKeepSet` in `platform_db_backup.go`).

### 5.4 Contact paths

- **First 30 min:** Platform on-call (PagerDuty: `platform-oncall`).
- **30-90 min, still broken:** Add infra-eng-lead via PagerDuty escalation.
- **>90 min, still broken:** Add CTO + CEO via Slack DM. By 90 min, the gap to NR widget #1 (time since last successful platform backup) is climbing and risk-of-data-loss-window calculations need to start.

---

## 6. New Relic widgets & alerts (the observability contract)

The worker is wired so an on-call can answer "is the backup healthy" from a single NR dashboard. Required widgets:

| Widget | NRQL | KPI |
|---|---|---|
| **Time since last successful platform backup** | `SELECT (max(timestamp) - now()) FROM Log WHERE message LIKE '%platform_db_backup.succeeded%'` | < 26 hours (one daily cycle + 2h headroom) |
| **Backup duration trend** | `SELECT average(duration_seconds) FROM Log WHERE message LIKE '%platform_db_backup.succeeded%' TIMESERIES 1 day` | Alert if doubles week-over-week — implies DB or network regression |
| **Backup size trend** | `SELECT average(size_bytes) FROM Log WHERE message LIKE '%platform_db_backup.succeeded%' TIMESERIES 1 day` | Alert if shrinks > 30% day-over-day (data loss?) or grows > 50% week-over-week (cost surprise) |
| **Backup failures / 24h** | `SELECT count(*) FROM Log WHERE message LIKE '%platform_db_backup.failed%' SINCE 24 hours ago` | Alert at ≥ 1; page director at ≥ 2 |
| **S3 storage cost (platform-backups)** | DO Spaces cost API or AWS Cost Explorer filtered by prefix | < $5/month at steady state |
| **Lock contention rate** | `SELECT count(*) FROM Log WHERE message LIKE '%lock_contended%' SINCE 7 days ago` | Expected: 0 in single-pod, low (< 7/week) in HA. Spike implies clock drift between pods |

**Critical alert** (page on-call): `time_since_last_succeeded > 36h` → wakes platform on-call.

### Log fields the widgets bind to

Every `platform_db_backup.succeeded` slog row carries:

```
size_bytes        int64       audit_log.metadata.size_bytes
duration_seconds  float64     audit_log.metadata.duration_seconds (rounded to 0.1s)
s3_key            string      audit_log.metadata.s3_key
swept_objects     int         audit_log.metadata.swept_objects
```

Every `platform_db_backup.failed` row additionally carries:
```
error             string      audit_log.metadata.error (trimmed pg_dump stderr if applicable)
```

If you want to query historical backup status without NR access, the `audit_log` table itself is the source of truth — `SELECT * FROM audit_log WHERE kind LIKE 'platform_backup.%' ORDER BY created_at DESC LIMIT 30`.

---

## 7. Config reference

| Env var | Default | Notes |
|---|---|---|
| `DATABASE_URL` | (required) | Platform DB — pg_dump talks to this |
| `OBJECT_STORE_ENDPOINT` | (required for backups to fire) | e.g. `nyc3.digitaloceanspaces.com` |
| `OBJECT_STORE_ACCESS_KEY` | (required) | DO Spaces / AWS / etc. master key |
| `OBJECT_STORE_SECRET_KEY` | (required) | DO Spaces / AWS / etc. master secret |
| `BACKUP_S3_BUCKET` | `instant-shared` | Shared with W5-B customer backup track |
| `BACKUP_S3_PATH_PREFIX` | `` | Optional outer prefix (e.g. `backups/`) |
| `PLATFORM_BACKUP_S3_PREFIX` | `platform-backups/` | Inner prefix — distinct from customer-backup prefix |
| `PG_DUMP_BIN` | `pg_dump` from `$PATH` | Pin to a specific binary in the image, e.g. `/usr/lib/postgresql/16/bin/pg_dump` |

---

## 8. Retention quick reference

| Band | What's kept | Limit |
|---|---|---|
| Daily | Every dump from the last 30 days | 30 objects |
| Monthly | The earliest-day dump for each of the last 12 months | up to 11 additional (current month overlaps daily) |
| Unparseable | Anything whose key doesn't have a `YYYY-MM-DD/` segment is kept conservatively | unbounded — use for `-MANUAL` snapshots |

Total steady-state footprint: ~41 objects × ~2 GB compressed each ≈ 80 GB.

The sweep runs ONLY after a successful upload — a failed backup can never trigger object deletion.

---

## 9. Known gaps / future work

1. **Restore-time PITR** is not part of this job. To roll forward to a specific minute within the day, you need either Postgres WAL archiving (Neon/RDS-managed) OR a logical replication slot. Out of scope here; track 5C will pair this nightly with continuous WAL shipping.
2. **Cross-region replication of the backup bucket** is not currently enabled. A DO `nyc3` outage takes both the production DB AND the backup with it. Track 5D.
3. **Backup encryption at rest** relies on DO Spaces' default SSE. We do not currently encrypt before upload — a leaked Spaces key reads the dump. Track CSO-7.
4. **Restore drill automation** — Section 4 is manual today. A scheduled k8s Job that runs the drill weekly + writes its outcome back to `audit_log` is the cheap, high-value follow-on. Track 5E.
