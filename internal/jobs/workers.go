package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	madmin "github.com/minio/madmin-go/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/config"
	"instant.dev/worker/internal/email"
	"instant.dev/worker/internal/provisioner"
)

// queueReconcile is the dedicated queue for fast periodic reconcilers
// (deploy-status every 30s, custom-domain every 5min). Isolated from the
// default queue so a fan-out backlog on bulk jobs (weekly_digest, etc.)
// cannot starve them — the previous symptom was deploy status staying in
// "building" indefinitely while 200K weekly_digest rows occupied every
// worker slot.
const queueReconcile = "reconcile"

// queueBilling is the dedicated queue for the billing reconciler (P1-L).
//
// The billing reconciler is itself a "reconciler" but it is NOT fast: a full
// sweep staggers ~100 teams × 2 Razorpay calls × 100ms ≈ 17 minutes, during
// which it occupied both queueReconcile worker slots. That starved the fast
// reconcilers (deploy-status every 30s, custom-domain every 5min) — deploy
// status stayed "building" for the whole billing sweep. Putting the billing
// sweep on its own single-worker queue keeps it fully isolated: it can run
// as long as it likes without ever touching queueReconcile's capacity.
const queueBilling = "billing"

// periodicUniqueOpts is the UniqueOpts EVERY periodic job must carry
// (P1-W3-07 / P1-W4-06 / P2-W5-05). The worker runs at replicas:2 — without
// a uniqueness guard, both pods' River clients independently enqueue the
// same periodic sweep on every tick, so the job runs TWICE per period
// (double lifecycle emails, double audit_log rows, double Razorpay spend).
//
// ByArgs:   uniqueness is scoped to the specific encoded args. The periodic
//           sweep jobs all use a zero-field args struct, so this is mostly a
//           belt-and-braces guard for any future job that varies its args.
// ByPeriod: a job is unique within one rounded period window — exactly the
//           "one run per tick, not one per replica" guarantee we need. The
//           period passed in is the job's own scheduling interval.
//
// River's default ByState set (available/completed/running/retryable/
// scheduled) is intentionally kept — a tick that has already completed in
// the current window still blocks a duplicate insert from the sibling pod.
func periodicUniqueOpts(period time.Duration) river.UniqueOpts {
	return river.UniqueOpts{ByArgs: true, ByPeriod: period}
}

// periodicInsertOpts is the InsertOpts builder for a default-queue periodic
// job. Every periodic job that previously returned `nil` InsertOpts (and so
// carried no uniqueness guard) now routes through here. queue is left empty
// → River QueueDefault.
func periodicInsertOpts(period time.Duration) *river.InsertOpts {
	return &river.InsertOpts{UniqueOpts: periodicUniqueOpts(period)}
}

// reconcileInsertOpts is the InsertOpts the fast-reconciler periodic-job
// builders must return. Extracted so a test can exercise the exact
// production value (asserting the closures embed the right Queue) without
// scraping source code. period is the job's scheduling interval — it feeds
// the UniqueOpts.ByPeriod window so a replicas:2 cluster runs each
// reconcile tick once, not once per pod.
func reconcileInsertOpts(period time.Duration) *river.InsertOpts {
	return &river.InsertOpts{Queue: queueReconcile, UniqueOpts: periodicUniqueOpts(period)}
}

// billingInsertOpts is the InsertOpts the billing-reconciler periodic-job
// builder must return — routes the long billing sweep to queueBilling so it
// cannot starve the fast reconcilers on queueReconcile (P1-L). Carries the
// same UniqueOpts guard so a replicas:2 cluster runs the ~17min sweep once
// per window, not twice (double Razorpay API spend).
func billingInsertOpts(period time.Duration) *river.InsertOpts {
	return &river.InsertOpts{Queue: queueBilling, UniqueOpts: periodicUniqueOpts(period)}
}

// Workers wraps a running River client.
type Workers struct {
	client  *river.Client[pgx.Tx]
	cancel  context.CancelFunc
	started bool // true only when riverClient.Start succeeded
}

// Started reports whether the River client started successfully.
// If false, no jobs are being processed — the caller should exit.
func (w *Workers) Started() bool { return w.started }

// Stop gracefully shuts down the background worker pool.
func (w *Workers) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	if w.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := w.client.Stop(ctx); err != nil {
			slog.Error("jobs.workers.stop_failed", "error", err)
		}
	}
}

// scheduleFunc is a river.PeriodicSchedule backed by an arbitrary func(time.Time) time.Time.
type scheduleFunc func(time.Time) time.Time

func (f scheduleFunc) Next(t time.Time) time.Time { return f(t) }

// entitlementRegraderAdapter bridges *provisioner.Client onto the
// entitlementRegrader interface the EntitlementReconcilerWorker depends on.
// The provisioner client returns provisioner.RegradeResult; the jobs package
// defines its own regradeOutcome so the worker's unit tests don't have to
// import the provisioner package. This adapter does the field-for-field copy.
type entitlementRegraderAdapter struct {
	client *provisioner.Client
}

func (a entitlementRegraderAdapter) RegradeResource(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType, tier, requestID string) (regradeOutcome, error) {
	res, err := a.client.RegradeResource(ctx, token, providerResourceID, resType, tier, requestID)
	if err != nil {
		return regradeOutcome{}, err
	}
	return regradeOutcome{
		Applied:          res.Applied,
		AppliedConnLimit: res.AppliedConnLimit,
		SkipReason:       res.SkipReason,
	}, nil
}

// weeklyPeriod / dailyPeriod are the nominal scheduling periods of the
// cron-style schedules below (mondayAt8UTCSchedule, dailyAt2/3UTCSchedule).
// river.PeriodicSchedule has no period accessor, so we name the periods
// here to feed UniqueOpts.ByPeriod — without that window a replicas:2
// cluster enqueues the cron job once per pod (P1-W3-07).
const (
	weeklyPeriod = 7 * 24 * time.Hour
	dailyPeriod  = 24 * time.Hour
)

// mondayAt8UTCSchedule implements river.PeriodicSchedule for every Monday 08:00 UTC.
type mondayAt8UTCSchedule struct{}

func (mondayAt8UTCSchedule) Next(t time.Time) time.Time {
	t = t.UTC()
	daysUntilMonday := (int(time.Monday) - int(t.Weekday()) + 7) % 7
	if daysUntilMonday == 0 && (t.Hour() > 8 || (t.Hour() == 8 && t.Minute() > 0)) {
		daysUntilMonday = 7
	}
	return time.Date(t.Year(), t.Month(), t.Day()+daysUntilMonday, 8, 0, 0, 0, time.UTC)
}

// dailyAt3UTCSchedule implements river.PeriodicSchedule for every day 03:00 UTC.
// Used by ChurnPredictorWorker: 03:00 UTC is mid-day in Asia / late-night in
// Europe / sleeping-hours in North America — a quiet slot that won't compete
// with peak-hour provisioning traffic on the platform DB.
type dailyAt3UTCSchedule struct{}

func (dailyAt3UTCSchedule) Next(t time.Time) time.Time {
	t = t.UTC()
	next := time.Date(t.Year(), t.Month(), t.Day(), 3, 0, 0, 0, time.UTC)
	if !next.After(t) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// dailyAt2UTCSchedule implements river.PeriodicSchedule for every day 02:00 UTC.
// Used by PlatformDBBackupWorker. 02:00 UTC is one hour earlier than the
// ChurnPredictor's 03:00 slot — both run during the same global off-peak
// band but the backup gets first crack at the DB so a slow scan can't
// be queued behind the churn sweep on the same single-pod worker.
// Concurrency between pods is handled by the advisory lock inside the
// worker, not by the cron offset.
type dailyAt2UTCSchedule struct{}

func (dailyAt2UTCSchedule) Next(t time.Time) time.Time {
	t = t.UTC()
	next := time.Date(t.Year(), t.Month(), t.Day(), 2, 0, 0, 0, time.UTC)
	if !next.After(t) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// StartWorkers initialises and starts the River background worker pool.
// It registers all job workers and schedules periodic jobs.
//
// deployStatusK8s is the k8s client used by DeployStatusReconciler to fetch
// live Deployment objects from the per-deployment "instant-deploy-<appID>"
// namespaces. Pass nil when the worker can't reach a cluster — the
// reconciler logs at WARN each run and other periodic jobs keep functioning.
// See worker/internal/jobs/deploy_status_reconcile.go for the SCOPE NOTE.
//
// deployAutopsyK8s is the extended k8s client used for Phase 0 failure autopsy
// capture (pod lastState, namespace events, log tail). Pass nil when the cluster
// is unreachable — the reconciler writes Unknown-reason autopsy rows so the api
// always surfaces a "failure" object on failed deployments.
//
// backupPlanRegistry is the BackupPlanRegistry surface used by
// CustomerBackupRunner. main.go wraps its *commonplans.Registry via
// NewBackupPlanRegistry; this lets StartWorkers stay free of a direct
// import on instant.dev/common/plans (the narrow PlanRegistry interface
// covers the existing quota workers' needs).
//
// Pass nil to fall back to the legacy hardcoded 7-day retention default
// — retentionDaysForTier WARNs in that case.
func StartWorkers(ctx context.Context, db *sql.DB, rdb *redis.Client, cfg *config.Config, provClient *provisioner.Client, planRegistry PlanRegistry, backupPlans BackupPlanRegistry, deployStatusK8s deployStatusK8sProvider, deployAutopsyK8s deployAutopsyK8sProvider, nrApp *newrelic.Application) *Workers {
	// rdb is used by LoopsEventForwarderWorker (cursor storage). Other
	// workers access redis indirectly via the platform DB.

	// River requires pgx pool — open a separate connection for the worker pool.
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("jobs.workers.pgxpool_failed", "error", err)
		return &Workers{}
	}

	// Run River schema migrations before starting the client.
	migrator := rivermigrate.New(riverpgxv5.New(pool), nil)
	if _, err := migrator.Migrate(context.Background(), rivermigrate.DirectionUp, nil); err != nil {
		slog.Error("jobs.workers.river_migrate_failed", "error", err)
	}

	// FOLLOWUP-5 (2026-05-14): the legacy Resend EmailClient was deleted in
	// this migration — the four remaining lifecycle methods (SendTrial* dead
	// code, SendWeeklyDigest + SendExpiryReminder live) were migrated to
	// audit_log → BrevoForwarder. The Resend SDK is no longer imported.
	//
	// Event-email provider — provider-agnostic seam. The factory chooses the
	// implementation from EMAIL_PROVIDER (Brevo today; SES/SendGrid possible
	// later) and returns NoopProvider when nothing is configured (fail-open).
	// Construction failure here (unknown provider name, brevo missing key)
	// is fatal: the operator opted into a real provider, silently falling
	// back to noop would hide the misconfiguration.
	emailProvider, err := email.NewProvider(email.Config{
		Provider: cfg.EmailProvider,
		Brevo: email.BrevoConfig{
			APIKey:      cfg.BrevoAPIKey,
			TemplateIDs: cfg.BrevoTemplateIDs,
			SenderEmail: cfg.BrevoSenderEmail,
			SenderName:  cfg.BrevoSenderName,
		},
		SES: email.SESConfig{
			AWSRegion:     cfg.SESAWSRegion,
			AWSAccessKey:  cfg.SESAWSAccessKey,
			AWSSecretKey:  cfg.SESAWSSecretKey,
			FromEmail:     cfg.SESFromEmail,
			TemplateNames: cfg.SESTemplateNames,
		},
	})
	if err != nil {
		slog.Error("jobs.workers.email_provider_init_failed",
			"error", err,
			"email_provider", cfg.EmailProvider,
		)
		return &Workers{}
	}
	slog.Info("jobs.workers.email_provider_ready", "name", emailProvider.Name())

	// Build MinIO admin client for storage IAM cleanup — nil unless the legacy
	// MINIO_* env vars are set. Only used when ExpireAnonymousWorker needs to
	// release per-IAM-user resources (i.e. self-hosted MinIO backend). With
	// the OBJECT_STORE_* shared-key backend (DO Spaces / AWS / GCS / R2) this
	// stays nil because no per-customer IAM was created in the first place.
	var minioClient *madmin.AdminClient
	if cfg.MinioEndpoint != "" {
		if mc, err := madmin.New(cfg.MinioEndpoint, cfg.MinioRootUser, cfg.MinioRootPassword, false); err != nil {
			slog.Warn("jobs.workers.minio_client_init_failed", "error", err)
		} else {
			minioClient = mc
		}
	}

	// Build the storage_bytes scanner — provider-agnostic, uses plain S3 API
	// against any backend. Reads OBJECT_STORE_* env vars (which fall back to
	// the legacy MINIO_* names in config.Load). Nil = fail open: the scanner
	// is skipped each run and storage_bytes for /storage/new resources isn't
	// updated. Other resource types (postgres/redis/mongo) continue via the
	// gRPC provisioner path.
	var minioScanner MinIOStorageScanner
	if cfg.ObjectStoreEndpoint != "" {
		if scanner, err := NewMinIOStorageScanner(cfg.ObjectStoreEndpoint, cfg.ObjectStoreAccessKey, cfg.ObjectStoreSecretKey, cfg.ObjectStoreBucket); err != nil {
			slog.Warn("jobs.workers.storage_scanner_init_failed", "error", err)
		} else {
			minioScanner = scanner
		}
	}

	// Object-store deleter for storage-resource expiry. Built from the same
	// minioScanner so it reaches the same S3-compatible OBJECT_STORE_* backend
	// (DO Spaces in prod). Without this, ExpireAnonymousWorker's storage case
	// flips the row to 'deleted' but never removes the tenant's objects —
	// cleanup silently relied on the 24h bucket-lifecycle rule. nil when no
	// OBJECT_STORE_* is wired (CI / docker-compose) or under the test seam;
	// the storage case then logs a WARN instead of a silent no-op.
	var storageObjectDeleter S3BackupDeleter
	if minioScanner != nil {
		if concrete, ok := minioScanner.(*minioStorageScanner); ok {
			storageObjectDeleter = newMinIOBackupDeleter(concrete)
		}
	}

	// Customer-backup object store — same endpoint/credentials as the
	// storage_bytes scanner, but a different BUCKET so pg_dump tarballs
	// don't mix with /storage/new customer object data. Nil = fail open:
	// the backup runner + restore runner log WARN and skip every tick.
	var backupStore BackupObjectStore
	if cfg.ObjectStoreEndpoint != "" {
		if store, err := NewMinIOBackupStore(cfg.ObjectStoreEndpoint, cfg.ObjectStoreAccessKey, cfg.ObjectStoreSecretKey); err != nil {
			slog.Warn("jobs.workers.backup_store_init_failed", "error", err)
		} else {
			backupStore = store
		}
	}

	workers := river.NewWorkers()
	// Each worker is wrapped in WithObservability so every job execution
	// stamps tid + trace_id on ctx and (optionally) opens a New Relic
	// transaction. nrApp may be nil — the wrapper still does the ctx work.
	// See middleware.go for the full contract.
	river.AddWorker(workers, WithObservability(
		NewExpireAnonymousWorker(db, provClient, minioClient).
			WithObjectDeleter(storageObjectDeleter, cfg.ObjectStoreBucket),
		nrApp,
	))
	river.AddWorker(workers, WithObservability(NewExpireStacksWorker(db, cfg.KubeNamespaceApps+"-"), nrApp))
	river.AddWorker(workers, WithObservability(NewRefreshGeoDBWorker(), nrApp))
	// TrialExpiryWorker was deleted in FOLLOWUP-5 (2026-05-14) — per project
	// memory rule `project_no_trial_pay_day_one.md`, the platform has NO
	// trial period (anonymous is the only free tier; hobby/pro/team are paid
	// from day one), so the worker scanned a column that's never populated.
	// WeeklyDigest is registered via composite literal, so the generic type
	// parameter can't be inferred from the constructor return — it must be
	// supplied explicitly. The worker writes an audit_log row instead of
	// calling Resend directly (FOLLOWUP-5 migration); the BrevoForwarder
	// picks the row up and dispatches via Brevo.
	river.AddWorker(workers, WithObservability(NewWeeklyDigestWorker(db), nrApp))
	river.AddWorker(workers, WithObservability(NewExpiryReminderWorker(db), nrApp))
	// Resource-expiry-imminent producer: every 10 minutes, scan for
	// authenticated resources whose expires_at falls inside the next hour
	// and write one resource.expiry_imminent audit_log row per resource per
	// 12h dedupe window. The Loops event forwarder drains those rows into
	// Brevo lifecycle emails (event = resource_expiring_soon). Separate from
	// ExpiryReminderWorker because the delivery channel (Loops/Brevo vs
	// Resend) and dedupe surface (audit_log subquery vs resources column)
	// are independent. See expire_imminent.go for the full SCOPE NOTE.
	river.AddWorker(workers, WithObservability(NewExpireImminentWorker(db), nrApp))
	// Build the infra revoker for storage-quota suspend/unsuspend.
	// All three credentials are optional — when absent the revoker is nil
	// and only the status row is flipped (fail-open per CLAUDE.md #1).
	var storageRevoker ResourceInfraRevoker
	if cfg.CustomerDatabaseURL != "" || cfg.MongoAdminURI != "" || cfg.CustomerRedisURL != "" {
		storageRevoker = NewDirectResourceRevoker(cfg.CustomerDatabaseURL, cfg.MongoAdminURI, cfg.CustomerRedisURL)
	}
	// Build the per-tenant Redis key evictor (A4). It uses the same shared-Redis
	// admin URL as the revoker; when CUSTOMER_REDIS_URL is unset the evictor is
	// a logged no-op and the quota worker still runs its suspend/unsuspend
	// loops. This is the only per-tenant memory enforcement on the shared
	// `redis-provision` pod — Redis ACL has no per-user maxmemory.
	var redisEvictor RedisKeyEvictor
	if cfg.CustomerRedisURL != "" {
		redisEvictor = NewDirectRedisEvictor(cfg.CustomerRedisURL)
	}
	river.AddWorker(workers, WithObservability(NewEnforceStorageQuotaWorkerWithEvictor(db, planRegistry, storageRevoker, redisEvictor), nrApp))
	river.AddWorker(workers, WithObservability(NewUpdateStorageBytesWorker(db, provClient, minioScanner), nrApp))
	// Quota-wall nudge — Track U1. Periodic scan that writes a single
	// near_quota_wall audit row per team per 24h when any axis (storage,
	// connections, provisions) crosses 80% of the tier limit. The
	// dashboard reads the latest row via GET /api/v1/usage/wall and
	// renders an upgrade banner. See quota_wall_nudge.go.
	river.AddWorker(workers, WithObservability(NewQuotaWallNudgeWorker(db, planRegistry), nrApp))
	// Custom-domain reconciler — TXT lookup, HTTP probe, stale-failed sweep.
	// k8s provider is nil today: the worker module does not import the api's
	// k8s client. Steps 2/3 (Ingress + cert poll) stay in the api handler.
	// See custom_domain_reconcile.go for the full SCOPE NOTE.
	river.AddWorker(workers, WithObservability(NewCustomDomainReconciler(db, nil, nil), nrApp))
	// Deploy-status reconciler — sweeps non-terminal deployments and rolls
	// status forward from live k8s Deployment state every 30s. deployStatusK8s
	// may be nil (kubeconfig unreachable in CI / docker-compose); the worker
	// then short-circuits with a WARN each tick. See deploy_status_reconcile.go.
	// deployAutopsyK8s (Phase 0) is wired via WithAutopsyK8s so failure
	// transitions trigger an autopsy capture on the same reconcile tick.
	// Pass nil for deployAutopsyK8s → Unknown-reason autopsy rows (fail-open).
	statusReconciler := NewDeployStatusReconciler(db, deployStatusK8s).
		WithAutopsyK8s(deployAutopsyK8s)
	river.AddWorker(workers, WithObservability(statusReconciler, nrApp))
	// Event-email forwarder — drains audit_log rows into the configured
	// provider every 60s for lifecycle email triggering. The provider is
	// always non-nil (NoopProvider when EMAIL_PROVIDER is unset). See
	// event_email_forwarder.go for the full contract.
	river.AddWorker(workers, WithObservability(NewEventEmailForwarderWorker(db, rdb, emailProvider), nrApp))
	// Churn predictor — daily 03:00 UTC scan that writes a
	// churn.risk_flagged audit_log row for every non-Team team that
	// has been inactive for 7+ days, still has active resources, and
	// hasn't been flagged in the last 30 days. The event-email
	// forwarder above drains those rows into the "we_miss_you"
	// reactivation email via the configured provider. See
	// churn_predictor.go for the activity-kind / dedupe rationale.
	river.AddWorker(workers, WithObservability(NewChurnPredictorWorker(db), nrApp))
	// Deploy-notify webhook dispatcher (A2). Drains deploy.* audit_log rows
	// into per-team customer-configured webhook URLs. No-op until customers
	// register a DEPLOY_NOTIFY_WEBHOOK_URL vault entry.
	river.AddWorker(workers, WithObservability(NewDeployNotifyWebhookWorker(db, rdb, nil), nrApp))
	// Payment grace reminder (A2). Every 6h, emits payment.grace_reminder
	// for dunning teams whose last_reminder_at is null or >6h ago.
	river.AddWorker(workers, WithObservability(NewPaymentGraceReminderWorker(db), nrApp))
	// Payment grace terminator (A2). Every 1h, POSTs /internal/teams/:id/
	// terminate for grace-expired teams. WARN-noops when the api URL or
	// WORKER_INTERNAL_JWT_SECRET is unset.
	river.AddWorker(workers, WithObservability(NewPaymentGraceTerminatorWorker(db, cfg.InstantAPIInternalURL, cfg.WorkerInternalJWTSecret, nil), nrApp))
	// GitHub auto-deploy dispatcher (W11 — migration 035 in the api repo).
	// Drains pending_github_deploys rows inserted by the api's /webhooks/github/:webhook_id
	// receive endpoint. Fetches the github archive tarball and POSTs to
	// /deploy/:appID/redeploy with the worker's internal JWT. No-op when
	// INSTANT_API_INTERNAL_URL or WORKER_INTERNAL_JWT_SECRET is unset — same
	// fail-open posture as PaymentGraceTerminator. See
	// github_deploy_dispatcher.go for the per-step contract.
	river.AddWorker(workers, WithObservability(NewGitHubDeployDispatcher(db, cfg.InstantAPIInternalURL, cfg.WorkerInternalJWTSecret), nrApp))
	// Magic-link reconciler (post 2026-05-14 RESEND_API_KEY=CHANGE_ME
	// outage). Every 60s, drains magic_links rows stuck at
	// email_send_status IN ('pending', 'send_failed') inside the 15-min
	// TTL window and POSTs each row id to the api's
	// /internal/email/resend-magic-link. Fail-open: WARNs and
	// short-circuits when INSTANT_API_INTERNAL_URL or
	// WORKER_INTERNAL_JWT_SECRET isn't set. See magic_link_reconciler.go
	// for the per-row outcome contract and the 3-attempt cap.
	river.AddWorker(workers, WithObservability(NewMagicLinkReconcilerWorker(db, cfg.InstantAPIInternalURL, cfg.WorkerInternalJWTSecret, nil), nrApp))
	// Pending-deletion expirer (Wave FIX-I, api migration 044). Every
	// 60s, flips pending_deletions rows past their TTL to status=
	// 'expired' so the per-resource dedup index clears and the next
	// DELETE on the same resource can mint a fresh email. Local SQL
	// only — no api round-trip, so no JWT-signing needed. See
	// pending_deletion_expirer.go.
	river.AddWorker(workers, WithObservability(NewPendingDeletionExpirerWorker(db), nrApp))
	// Deploy TTL reminder (Wave FIX-J) — every 60s, scan deployments whose
	// expires_at falls within the next 12h and CAS-advance the
	// reminders_sent counter to fire the next 2h-spaced reminder. Six
	// reminders total. Idempotent across ticks via the CAS guard. Email
	// dispatch best-effort — see deployment_reminder.go.
	river.AddWorker(workers, WithObservability(NewDeploymentReminderWorker(db, nil), nrApp))
	// Deploy TTL expirer (Wave FIX-J) — every 60s, scan deployments whose
	// expires_at has passed and soft-delete (status='expired'). Sends a
	// one-shot "your deploy expired" email. See deployment_expirer.go.
	river.AddWorker(workers, WithObservability(NewDeploymentExpirerWorker(db, nil), nrApp))
	// Customer-backup pipeline — three workers, two cron schedules.
	//
	//   scheduler (every hour)  — inserts pending resource_backups rows for
	//                              tier-eligible postgres/vector resources.
	//   runner    (every 30s)   — claims pending rows, pg_dump → S3, runs
	//                              retention sweep at end of batch.
	//   restore   (every 30s)   — claims pending resource_restores rows,
	//                              S3 → pg_restore into the same resource.
	//
	// All three are no-ops when cfg.AESKey or cfg.ObjectStoreEndpoint is
	// unset — fail-open so a dev environment that doesn't ship AES keys
	// doesn't block worker boot. See each worker's Work() top for the
	// exact WARN line emitted.
	river.AddWorker(workers, WithObservability(NewCustomerBackupSchedulerWorker(db), nrApp))
	// FIX-H #65/#Q47 — wire the refund client so terminal MANUAL backup
	// failures credit the team's daily counter via the api's internal
	// /internal/teams/:id/backup-quota/refund endpoint. Empty apiBase /
	// jwtSecret disables the refund (logs WARN, no-op) — matches the
	// rest of the worker's fail-open posture.
	customerRunner := NewCustomerBackupRunner(db, backupStore, cfg.BackupS3Bucket, cfg.BackupS3PathPrefix, cfg.AESKey, backupPlans).
		WithRefundClient(cfg.InstantAPIInternalURL, cfg.WorkerInternalJWTSecret, nil)
	river.AddWorker(workers, WithObservability(customerRunner, nrApp))
	river.AddWorker(workers, WithObservability(NewCustomerRestoreRunner(db, backupStore, cfg.BackupS3Bucket, cfg.AESKey), nrApp))

	// Platform-DB backup — nightly 02:00 UTC pg_dump of the platform DB to
	// S3 (DO Spaces today). Closes the "if instant_platform is lost, the
	// platform is unrecoverable" gap. See platform_db_backup.go for the
	// retention / locking / audit contract. If OBJECT_STORE_* is unset the
	// worker is constructed in disabled mode and logs at WARN each tick;
	// see Work's S3==nil branch.
	var backupS3 s3Client
	if cfg.ObjectStoreEndpoint != "" {
		c, err := NewBackupS3Client(cfg.ObjectStoreEndpoint, cfg.ObjectStoreAccessKey, cfg.ObjectStoreSecretKey)
		if err != nil {
			slog.Warn("jobs.workers.backup_s3_init_failed",
				"error", err,
				"note", "platform_db_backup will run in disabled mode until OBJECT_STORE_* env vars resolve",
			)
		} else {
			backupS3 = c
		}
	}
	river.AddWorker(workers, WithObservability(NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DB:          db,
		DatabaseURL: cfg.DatabaseURL,
		S3:          backupS3,
		Bucket:      cfg.BackupS3Bucket,
		OuterPrefix: cfg.BackupS3PathPrefix,
		InnerPrefix: cfg.PlatformBackupS3Prefix,
	}), nrApp))

	// Team deletion executor — daily 03:00 UTC. Completes the GDPR
	// right-to-be-forgotten lifecycle for any team that called
	// DELETE /api/v1/team more than 30 days ago. Runs AFTER the
	// platform-db-backup job at 02:00 UTC so today's tombstoned data is
	// still in tonight's backup before destruction. minioScanner is reused
	// as the S3 backup deleter — both the bytes-scanner and the deleter
	// need the same minioObjectLister surface against the shared bucket,
	// so we hand a deletion-flavoured wrapper into the executor. When the
	// scanner is nil (CI / docker-compose, no OBJECT_STORE_* env vars) the
	// executor skips S3 destruction and continues with the rest of the
	// pipeline. See team_deletion_executor.go for the per-step contract.
	var s3Deleter S3BackupDeleter
	if minioScanner != nil {
		// minioStorageScanner.client implements the minio-go RemoveObjects
		// + ListObjects surface defined by S3BackupDeleter. We expose it
		// via a tiny adapter rather than asserting on a private field so
		// the executor never depends on the scanner's internals. The
		// scanner is held as the MinIOStorageScanner interface for the
		// bytes-scanning worker; the deleter adapter needs the concrete
		// *minioStorageScanner so we narrow with a type assertion that
		// is always true in production (the only constructor returns
		// the concrete type).
		if concrete, ok := minioScanner.(*minioStorageScanner); ok {
			s3Deleter = newMinIOBackupDeleter(concrete)
		}
	}
	// k8s namespace deleter — shared by the team-deletion executor (deletes
	// a deleted team's instant-deploy-* namespaces) and the orphan-sweep
	// reconciler. nil when no cluster is reachable (CI / docker-compose) —
	// both consumers then skip the k8s teardown step with a WARN log.
	var nsClient K8sNamespaceDeleter
	if nc, nsErr := NewK8sNamespaceClient(); nsErr != nil {
		slog.Warn("jobs.k8s_namespace_client.unavailable",
			"error", nsErr, "effect", "team-deletion k8s teardown + orphan-sweep PASS 3 disabled")
	} else {
		nsClient = nc
	}

	teamDeletionExecutor := NewTeamDeletionExecutorWorker(db, provClient, s3Deleter, nsClient, cfg.ObjectStoreBucket)
	river.AddWorker(workers, WithObservability(teamDeletionExecutor, nrApp))

	// Orphan-sweep reconciler — the eventually-consistent safety net that
	// finishes any partial team deletion. Runs every 15 minutes:
	//   PASS 1 re-runs the executor's teardown for stuck deletion_pending
	//          teams (idempotent),
	//   PASS 2 cancels Razorpay subscriptions still live on tombstoned /
	//          deletion_pending teams (the "stop the money" backstop),
	//   PASS 3 deletes instant-deploy-* namespaces with no live owner.
	// The Razorpay canceler is nil when RAZORPAY_KEY_ID is unset — PASS 2
	// then skips. nsClient (a K8sNamespaceDeleter) is asserted up to the
	// K8sNamespaceLister the reconciler needs; the concrete
	// k8sNamespaceClient satisfies both.
	var orphanCanceler OrphanSubscriptionCanceler
	if oc, ocErr := NewRazorpayOrphanCanceler(); ocErr != nil {
		slog.Warn("jobs.orphan_sweep.canceler_init_failed", "error", ocErr)
	} else {
		orphanCanceler = oc // may be nil when Razorpay is unconfigured
	}
	var nsLister K8sNamespaceLister
	if l, ok := nsClient.(K8sNamespaceLister); ok {
		nsLister = l
	}
	river.AddWorker(workers, WithObservability(
		NewOrphanSweepReconciler(db, teamDeletionExecutor, orphanCanceler, nsLister),
		nrApp,
	))
	// Provisioner-reconciler (W5-A). Every 2min, recovers or abandons
	// stuck pending resources.
	//
	// MR-P0-2 (BugBash 2026-05-20): this previously passed nil → NoopProber,
	// whose every probe returns ProbeReachable — so the reconciler would
	// blindly flip every stuck 'pending' row to 'active' WITHOUT checking the
	// backend, handing the customer a credentials-less resource the platform
	// claims is healthy. Wire the SAME real prober resource_heartbeat uses two
	// lines below (NewRealProber): a stuck pending row is now only promoted to
	// 'active' if its backend is genuinely reachable, else abandoned/retried.
	river.AddWorker(workers, WithObservability(NewProvisionerReconcilerWorker(db, rdb, NewRealProber(cfg)), nrApp))
	// Resource-heartbeat (W5-A). Hourly (dev: 1min) probe of every active
	// resource. Sets degraded=true on probe failure, emits state-change
	// audit rows.
	//
	// P1-W5-09: this previously passed nil → NoopProber, so degraded
	// detection was a permanent no-op in production — every probe returned
	// ProbeSkip and no resource was ever flagged degraded. Wire the real
	// prober (real_prober.go): it dispatches per resource_type (postgres
	// SELECT 1, redis PING, mongo ping, storage/queue HTTP probe) and
	// decrypts connection_url with cfg.AESKey when set, falling back to
	// plaintext when it isn't. NewRealProber never returns nil.
	river.AddWorker(workers, WithObservability(NewResourceHeartbeatWorker(db, NewRealProber(cfg)), nrApp))
	// Uptime prober (W11). Per-minute liveness probe of every public
	// component (api, provisioner, worker, deploys, marketing). Writes
	// one uptime_samples row per component per tick. Consumed by the
	// api's GET /api/v1/status. See uptime_prober.go for per-probe
	// fail-mode rationale.
	river.AddWorker(workers, WithObservability(NewUptimeProberWorker(db), nrApp))
	// Uptime retention sweep — daily prune of uptime_samples > 90d.
	river.AddWorker(workers, WithObservability(NewUptimeRetentionWorker(db), nrApp))
	// Razorpay webhook-events prune — daily DELETE of razorpay_webhook_events
	// rows > 30d. The api appends one dedup row per Razorpay webhook delivery;
	// migration 033 envisioned a periodic prune but never shipped one, so the
	// table grew unbounded. See razorpay_webhook_prune.go.
	river.AddWorker(workers, WithObservability(NewRazorpayWebhookPruneWorker(db), nrApp))
	// Entitlement reconciler — detects "upgrade drift" (a postgres resource
	// whose tier was bumped on plan upgrade but whose actual connection cap
	// was never re-applied) and fixes it via the provisioner RegradeResource
	// RPC. The regrader is nil when PROVISIONER_ADDR is unset — the worker
	// then WARN-noops each tick (fail-open). See entitlement_reconciler.go.
	var entitlementRegrader entitlementRegrader
	if provClient != nil {
		entitlementRegrader = entitlementRegraderAdapter{client: provClient}
	}
	river.AddWorker(workers, WithObservability(NewEntitlementReconcilerWorker(db, planRegistry, entitlementRegrader), nrApp))
	// Billing reconciler (P1 Wave-3 Cluster-B Slice 4). Every 15 minutes,
	// compares Razorpay's live subscription state against teams.plan_tier and
	// corrects divergence in both directions (upgrade catch-up AND grace/downgrade
	// catch-up). The safety net for missed webhooks during pod-restart windows.
	//
	// The fetcher is noopSubFetcher when RAZORPAY_KEY_ID is unset — the
	// reconciler logs a WARN per tick and is otherwise a no-op, matching the
	// fail-open posture of every other optional-dependency worker here.
	//
	// WrapFetcherWithBreaker adds the worker-local circuit breaker so a Razorpay
	// outage aborts the tick cleanly instead of burning 100 × 10s timeouts.
	//
	// Wire the real Razorpay SDK fetcher when RAZORPAY_KEY_ID + RAZORPAY_KEY_SECRET
	// are set. Falls back to noopSubFetcher when credentials are absent so the
	// worker starts cleanly in dev/test environments without Razorpay configured.
	// WrapFetcherWithBreaker adds the circuit breaker on top of either path so a
	// Razorpay outage aborts the tick cleanly instead of burning 100 × 10s timeouts.
	billingBreakerInst := NewBillingReconcilerCircuitBreaker()
	var billingFetcher subscriptionFetcher
	if realFetcher, err := NewRazorpaySubFetcher(); err != nil {
		slog.Error("billing.reconciler.fetcher_init_failed",
			"error", err,
			"fallback", "noopSubFetcher",
		)
		billingFetcher = noopSubFetcher{}
	} else if realFetcher != nil {
		slog.Info("billing.reconciler.fetcher_configured",
			"fetcher", "razorpaySubFetcher",
			"note", "real Razorpay SDK fetcher active",
		)
		billingFetcher = realFetcher
	} else {
		slog.Warn("billing.reconciler.fetcher_not_configured",
			"note", "RAZORPAY_KEY_ID/SECRET unset — reconciler is a no-op each tick",
		)
		billingFetcher = noopSubFetcher{}
	}
	billingFetcher = WrapFetcherWithBreaker(billingFetcher, billingBreakerInst)
	river.AddWorker(workers, WithObservability(NewBillingReconcilerWorker(db, billingFetcher, nil), nrApp))
	// Checkout reconciler — every 5min, scan pending_checkouts (api
	// migration 034) for Razorpay checkouts that never resolved within the
	// 15-minute grace window and email the customer "your upgrade didn't
	// complete". This is the ONLY mechanism that catches the no-webhook
	// case: a checkout that failed on Razorpay's hosted page WITHOUT a
	// payment object ever being created emits zero webhooks, so the api
	// never learns of it. The same billingFetcher constructed above is
	// reused for a best-effort Razorpay double-check — if Razorpay reports
	// the subscription active (webhook merely delayed) the row is stamped
	// resolved_at and no email is sent. When RAZORPAY_KEY_ID is unset the
	// fetcher is the noop and the double-check is skipped — the 15-minute
	// heuristic stands alone. See checkout_reconcile.go.
	river.AddWorker(workers, WithObservability(NewCheckoutReconcileWorker(db, billingFetcher), nrApp))

	periodicJobs := buildPeriodicJobs(cfg)

	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			// Bulk email + heavyweight periodics live on the default queue.
			// A fan-out (one row per team) can blow this to 100K+ rows; the
			// reconcile queue below guarantees small-but-critical periodic
			// jobs always have worker capacity.
			river.QueueDefault: {MaxWorkers: 5},
			// Reserved for fast, frequent reconcilers (deploy-status every 30s,
			// custom-domain every 5min). 2 workers is enough because each
			// invocation does one batched DB query + per-row k8s GETs.
			queueReconcile: {MaxWorkers: 2},
			// Dedicated to the billing reconciler (P1-L). The billing sweep
			// runs ~17 min; isolating it on its own queue means it can never
			// occupy a queueReconcile slot and starve the fast reconcilers.
			// 1 worker is enough — the sweep is a single periodic job and
			// must not run concurrently with itself.
			queueBilling: {MaxWorkers: 1},
		},
		Workers:      workers,
		PeriodicJobs: periodicJobs,
	})
	if err != nil {
		slog.Error("jobs.workers.client_init_failed", "error", err)
		pool.Close()
		return &Workers{}
	}

	workerCtx, cancel := context.WithCancel(ctx)

	if err := riverClient.Start(workerCtx); err != nil {
		slog.Error("jobs.workers.start_failed", "error", err)
		cancel()
		pool.Close()
		return &Workers{started: false}
	}

	slog.Info("jobs.workers.started",
		"queues", fmt.Sprintf("%v", []string{river.QueueDefault, queueReconcile, queueBilling}),
		"max_workers", 5,
	)

	return &Workers{
		client:  riverClient,
		cancel:  cancel,
		started: true,
	}
}

// buildPeriodicJobs constructs the full set of periodic jobs the worker
// schedules. Extracted from StartWorkers so a registry-iterating test
// (TestPeriodicJobs_AllCarryUniqueOpts) can assert every periodic job
// carries River UniqueOpts — without that guard a replicas:2 cluster runs
// every sweep twice (P1-W3-07 / P1-W4-06 / P2-W5-05).
//
// The job set depends only on cfg (MaxMind license key + GeoLite2 path for
// the geo refresh job, Environment for the resource-heartbeat cadence) so
// the test can pass a minimal config.
func buildPeriodicJobs(cfg *config.Config) []*river.PeriodicJob {
	return []*river.PeriodicJob{
		river.NewPeriodicJob(
			river.PeriodicInterval(1*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return ExpireAnonymousArgs{}, periodicInsertOpts(1 * time.Hour)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		river.NewPeriodicJob(
			river.PeriodicInterval(1*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return ExpireStacksArgs{}, periodicInsertOpts(1 * time.Hour)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// GeoLite2 refresh (P1-D). RunOnStart=true: a frequently-redeployed
		// worker would otherwise reset the long periodic timer on every
		// restart and the job would never fire — the geo DB stayed the
		// stale baked-in copy forever. The job itself is cheap on a fresh
		// worker: geodb.Work() skips the MaxMind download when the on-disk
		// MMDB is still within geoLite2MaxAge (7d). The interval is a
		// backstop for the rare long-lived worker that never restarts.
		river.NewPeriodicJob(
			river.PeriodicInterval(geoLite2RefreshInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return RefreshGeoDBArgs{
					LicenseKey: cfg.MaxMindLicenseKey,
					DBPath:     cfg.GeoLite2DBPath,
				}, periodicInsertOpts(geoLite2RefreshInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// TrialExpiry periodic job removed in FOLLOWUP-5 (2026-05-14) —
		// see deletion note above on TrialExpiryWorker. No-trial policy is
		// enforced by `project_no_trial_pay_day_one.md`.
		river.NewPeriodicJob(
			mondayAt8UTCSchedule{},
			func() (river.JobArgs, *river.InsertOpts) {
				// Cron schedule (Mon 08:00 UTC) — the nominal period is one
				// week, so a sibling pod's tick inside the same week is
				// deduped.
				return WeeklyDigestArgs{}, periodicInsertOpts(weeklyPeriod)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		river.NewPeriodicJob(
			river.PeriodicInterval(6*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return UpdateStorageBytesArgs{}, periodicInsertOpts(6 * time.Hour)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		river.NewPeriodicJob(
			river.PeriodicInterval(6*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return EnforceStorageQuotaArgs{}, periodicInsertOpts(6 * time.Hour)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Quota-wall nudge — Track U1. Runs every 30 minutes; the 24h
		// dedupe in the job guarantees at most one audit row per team
		// per day no matter how many ticks see the same condition.
		// RunOnStart=false: a worker restart doesn't need to immediately
		// re-scan — the previous run's nudges are still inside the 24h
		// dedupe window and the dashboard banner will still render.
		river.NewPeriodicJob(
			river.PeriodicInterval(quotaWallNudgeInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return QuotaWallNudgeArgs{}, periodicInsertOpts(quotaWallNudgeInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Expiry reminder — 30-minute sweep that emails owners of
		// claimed-but-unpaid (tier='free') resources whose expires_at is
		// inside the 12h / 6h / 1h stage windows. Dedupe lives in the DB
		// (resources.reminders_sent CAS) so one (resource, stage) gets at
		// most one email no matter how many ticks see it.
		//
		// P2-11 (BugBash 2026-05-18): the cadence MUST be 30 min, not 1h.
		// The tightest stage window is (1h, 0h] — exactly one hour wide.
		// At an hourly tick a single missed/late tick (worker restart,
		// River backlog) drops the final 1h urgent reminder entirely. A
		// 30-minute cadence guarantees at least two ticks observe every
		// stage window, so a single missed tick can't drop a stage. The
		// 30-min reminderCooldown floor in expiry_reminder.go still
		// prevents two ticks from double-firing the same resource.
		river.NewPeriodicJob(
			river.PeriodicInterval(expiryReminderInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return ExpiryReminderArgs{}, periodicInsertOpts(expiryReminderInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Resource-expiry-imminent — every 10 minutes, write a
		// resource.expiry_imminent audit row for any authenticated
		// resource whose expires_at falls inside the next hour. Dedupe
		// is enforced inside the candidate query (12h window on the
		// audit_log table) so the dispatch cadence is independent of
		// the per-resource email frequency. See expire_imminent.go for
		// the freshness-window rationale.
		river.NewPeriodicJob(
			river.PeriodicInterval(expireImminentInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return ExpireImminentArgs{}, periodicInsertOpts(expireImminentInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Custom-domain reconciler runs every 5 minutes — see
		// customDomainReconcileInterval in custom_domain_reconcile.go.
		// RunOnStart=true so a worker restart immediately picks up domains
		// that became verifiable while we were down.
		// Routed to the "reconcile" queue so a backlog on the default queue
		// (e.g. a weekly_digest fan-out) cannot starve it.
		river.NewPeriodicJob(
			river.PeriodicInterval(customDomainReconcileInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return CustomDomainReconcileArgs{}, reconcileInsertOpts(customDomainReconcileInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Deploy-status reconciler runs every 30s — see
		// deployStatusReconcileInterval in deploy_status_reconcile.go.
		// RunOnStart=true so a worker restart immediately reconciles any
		// deployments stuck in "building" or "deploying" from the last cycle.
		// On the "reconcile" queue for the same starvation-protection reason.
		river.NewPeriodicJob(
			river.PeriodicInterval(deployStatusReconcileInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return DeployStatusReconcileArgs{}, reconcileInsertOpts(deployStatusReconcileInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// GitHub auto-deploy dispatcher — every 30s. RunOnStart=true so a
		// worker restart drains rows that piled up while the worker was
		// down. Same starvation-protection queue as the other reconcilers.
		river.NewPeriodicJob(
			river.PeriodicInterval(githubDispatcherInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return GitHubDeployDispatcherArgs{}, reconcileInsertOpts(githubDispatcherInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Magic-link reconciler — every 60s. RunOnStart=true so a worker
		// restart immediately drains rows whose first send failed while
		// the worker was down (we have a 15-min TTL window to retry, so
		// the cost of a full sweep at boot is small). Routed to the
		// reconcile queue so a weekly_digest fan-out on the default
		// queue can't starve magic-link reliability — auth being slow is
		// the most visible reliability surface this platform has.
		river.NewPeriodicJob(
			river.PeriodicInterval(magicLinkReconcilerInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return MagicLinkReconcilerArgs{}, reconcileInsertOpts(magicLinkReconcilerInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Pending-deletion expirer (Wave FIX-I, api migration 044) —
		// every 60s. RunOnStart=true so a worker restart immediately
		// vacates the dedup index for any rows that overshot their TTL
		// while we were down. Same starvation-protection queue as the
		// other reconcilers.
		river.NewPeriodicJob(
			river.PeriodicInterval(pendingDeletionExpirerInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return PendingDeletionExpirerArgs{}, reconcileInsertOpts(pendingDeletionExpirerInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Event-email forwarder runs every eventEmailForwarderInterval (60s).
		// RunOnStart=false: a worker restart should pick up the cursor on
		// the next tick, not race to fire a duplicate batch. See
		// event_email_forwarder.go for the cursor / idempotency contract.
		river.NewPeriodicJob(
			river.PeriodicInterval(eventEmailForwarderInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return EventEmailForwarderArgs{}, periodicInsertOpts(eventEmailForwarderInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Deploy TTL reminder (Wave FIX-J) — every 60s. RunOnStart=false:
		// the 2h cooldown CAS guarantees a worker restart can't fire
		// spurious reminders, so there's no urgency to scan immediately
		// after boot. The next scheduled tick within 60s catches us up.
		river.NewPeriodicJob(
			river.PeriodicInterval(60*time.Second),
			func() (river.JobArgs, *river.InsertOpts) {
				return DeploymentReminderArgs{}, reconcileInsertOpts(60 * time.Second)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Deploy TTL expirer (Wave FIX-J) — every 60s. RunOnStart=true so
		// a worker restart immediately picks up deploys that crossed their
		// expires_at while the worker was down. The guarded UPDATE
		// (status NOT IN ('deleted','expired')) prevents re-processing
		// already-expired rows.
		river.NewPeriodicJob(
			river.PeriodicInterval(60*time.Second),
			func() (river.JobArgs, *river.InsertOpts) {
				return DeploymentExpirerArgs{}, reconcileInsertOpts(60 * time.Second)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Churn predictor — runs daily at 03:00 UTC. The 30-day dedupe
		// guarantees at most one churn.risk_flagged row per team per
		// month regardless of restart cadence. RunOnStart=false: a
		// worker restart in the middle of the day shouldn't immediately
		// re-scan — wait for the next scheduled 03:00 slot so the
		// scan happens during quiet hours.
		river.NewPeriodicJob(
			dailyAt3UTCSchedule{},
			func() (river.JobArgs, *river.InsertOpts) {
				return ChurnPredictorArgs{}, periodicInsertOpts(dailyPeriod)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Deploy-notify webhook dispatcher (A2) — every 30s, drain
		// deploy.* audit_log rows to customer webhook URLs.
		river.NewPeriodicJob(
			river.PeriodicInterval(deployNotifyWebhookInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return DeployNotifyWebhookArgs{}, reconcileInsertOpts(deployNotifyWebhookInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Payment grace reminder (A2) — every 6h. RunOnStart=false.
		river.NewPeriodicJob(
			river.PeriodicInterval(paymentGraceReminderInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return PaymentGraceReminderArgs{}, periodicInsertOpts(paymentGraceReminderInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Payment grace terminator (A2) — every 1h. RunOnStart=true.
		river.NewPeriodicJob(
			river.PeriodicInterval(paymentGraceTerminatorInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return PaymentGraceTerminatorArgs{}, periodicInsertOpts(paymentGraceTerminatorInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Checkout reconciler — every 5min, sweeps pending_checkouts for
		// Razorpay checkouts unresolved past the 15-minute grace window and
		// emails the customer "your upgrade didn't complete". RunOnStart=
		// false: a worker restart should not fire an immediate sweep — the
		// 15-minute grace window means a freshly-restarted worker waiting
		// for the next 5min slot still catches every abandoned checkout
		// well within its window. See checkout_reconcile.go.
		river.NewPeriodicJob(
			river.PeriodicInterval(checkoutReconcileInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return CheckoutReconcileArgs{}, periodicInsertOpts(checkoutReconcileInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Customer backup scheduler — every hour, sweeps active
		// postgres/vector resources and INSERTs a 'pending' row for any
		// tier that's due this hour (pro/team/growth every hour; hobby
		// once per day at the team's daily slot). RunOnStart=true so a
		// worker restart immediately covers any hour-bucket that fell
		// inside the downtime; the in-job 50min dedupe lookback
		// prevents double-inserts.
		river.NewPeriodicJob(
			river.PeriodicInterval(1*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return CustomerBackupSchedulerArgs{}, periodicInsertOpts(1 * time.Hour)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Customer backup runner — every 30 seconds, picks up pending
		// rows (whether inserted by the scheduler above or by the api's
		// POST /resources/:id/backup) and streams pg_dump → gzip → S3.
		// RunOnStart=true so a restart drains any rows that were
		// 'pending' during the downtime. Routed to the reconcile queue
		// so a fan-out on the default queue (weekly_digest) can't
		// starve customer-facing backup latency.
		river.NewPeriodicJob(
			river.PeriodicInterval(30*time.Second),
			func() (river.JobArgs, *river.InsertOpts) {
				return CustomerBackupRunnerArgs{}, reconcileInsertOpts(30 * time.Second)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Customer restore runner — every 30 seconds, picks up pending
		// resource_restores rows and pg_restores from S3 into the same
		// resource. Smaller per-tick batch (5) than the backup runner
		// because pg_restore is heavier and holds DB locks.
		river.NewPeriodicJob(
			river.PeriodicInterval(30*time.Second),
			func() (river.JobArgs, *river.InsertOpts) {
				return CustomerRestoreRunnerArgs{}, reconcileInsertOpts(30 * time.Second)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Platform-DB backup — runs daily at 02:00 UTC. RunOnStart=false:
		// a worker restart should not trigger an immediate dump (the
		// previous successful run is still within the NR "< 26h" KPI
		// window) — wait for the next scheduled 02:00 slot. The advisory
		// lock inside the worker guards against a multi-pod cluster
		// running concurrent dumps if all pods happen to wake on the
		// same second.
		river.NewPeriodicJob(
			dailyAt2UTCSchedule{},
			func() (river.JobArgs, *river.InsertOpts) {
				return PlatformDBBackupArgs{}, periodicInsertOpts(dailyPeriod)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Team deletion executor — runs daily at 03:00 UTC, after the
		// platform-DB backup at 02:00 UTC (so today's tombstoned data
		// IS captured in tonight's backup before destruction).
		// RunOnStart=false: a worker restart should NOT immediately tear
		// down customer data — wait for the next 03:00 slot so the run
		// happens during quiet hours and the operator has a chance to
		// notice anything anomalous in the logs first.
		river.NewPeriodicJob(
			dailyAt3UTCSchedule{},
			func() (river.JobArgs, *river.InsertOpts) {
				return TeamDeletionExecutorArgs{}, periodicInsertOpts(dailyPeriod)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Orphan-sweep reconciler — every 15min. The eventually-consistent
		// safety net for team/account deletion: completes any partial
		// teardown (stuck deletion_pending teams), cancels orphaned
		// Razorpay subscriptions still charging a tombstoned customer's
		// card, and deletes instant-deploy-* namespaces with no live
		// owner. RunOnStart=true so a worker restart immediately checks
		// for orphans that piled up while it was down — a deleted
		// customer being billed is time-sensitive enough to warrant the
		// boot-time scan. Routed to the reconcile queue so a default-
		// queue fan-out cannot starve the billing-safety sweep.
		river.NewPeriodicJob(
			river.PeriodicInterval(orphanSweepInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return OrphanSweepReconcilerArgs{}, reconcileInsertOpts(orphanSweepInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Provisioner-reconciler (W5-A) — every 2min, reconcile queue.
		// RunOnStart=true.
		river.NewPeriodicJob(
			river.PeriodicInterval(provisionerReconcilerInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return ProvisionerReconcilerArgs{}, reconcileInsertOpts(provisionerReconcilerInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Resource-heartbeat (W5-A) — 1h prod / 1min dev. RunOnStart=false.
		river.NewPeriodicJob(
			river.PeriodicInterval(resourceHeartbeatPeriodicInterval(cfg.Environment)),
			func() (river.JobArgs, *river.InsertOpts) {
				return ResourceHeartbeatArgs{}, reconcileInsertOpts(resourceHeartbeatPeriodicInterval(cfg.Environment))
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Uptime prober (W11) — every minute, writes one uptime_samples
		// row per component. Routed to the reconcile queue so a default-
		// queue backlog (weekly_digest fan-out) can't starve the status
		// page during exactly the moment we want it to be honest.
		// RunOnStart=true so a worker restart immediately writes a row
		// for "we are up RIGHT NOW".
		river.NewPeriodicJob(
			river.PeriodicInterval(uptimeProberInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return UptimeProberArgs{}, reconcileInsertOpts(uptimeProberInterval)
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Uptime retention sweep — daily prune of uptime_samples > 90d.
		// RunOnStart=false: a restart shouldn't immediately scan; wait
		// for the next 24h slot.
		river.NewPeriodicJob(
			river.PeriodicInterval(24*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return UptimeRetentionArgs{}, periodicInsertOpts(24 * time.Hour)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Razorpay webhook-events prune — daily DELETE of dedup rows > 30d.
		// RunOnStart=false: a restart shouldn't immediately scan; the table
		// grows slowly (one row per webhook delivery) so a day's delay before
		// the first prune after a restart is harmless. Wait for the next 24h
		// slot.
		river.NewPeriodicJob(
			river.PeriodicInterval(24*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return RazorpayWebhookPruneArgs{}, periodicInsertOpts(24 * time.Hour)
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Entitlement reconciler — cadence from ENTITLEMENT_RECONCILE_INTERVAL
		// (Go duration string; default 5m). Detects + fixes upgrade drift on
		// postgres connection caps. Routed to the reconcile queue so a
		// default-queue fan-out can't starve it. RunOnStart=true so a worker
		// restart immediately re-checks any resources that were upgraded
		// while the worker was down.
		river.NewPeriodicJob(
			river.PeriodicInterval(EntitlementReconcileInterval()),
			func() (river.JobArgs, *river.InsertOpts) {
				return EntitlementReconcilerArgs{}, reconcileInsertOpts(EntitlementReconcileInterval())
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Billing reconciler (P1 Wave-3 Cluster-B Slice 4) — every 15 min
		// (override via BILLING_RECONCILE_INTERVAL). RunOnStart=true so a
		// worker restart immediately sweeps for any gaps that opened during
		// the downtime. Routed to the DEDICATED billing queue (P1-L): the
		// sweep takes ~17 min and previously occupied both queueReconcile
		// slots, starving the fast reconcilers (deploy-status, custom-domain).
		//
		// NR alert: billing.reconciler.gap_detected > 3 in 15m → PagerDuty P2.
		river.NewPeriodicJob(
			river.PeriodicInterval(BillingReconcileInterval()),
			func() (river.JobArgs, *river.InsertOpts) {
				return BillingReconcilerArgs{}, billingInsertOpts(BillingReconcileInterval())
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
	}
}
