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

// reconcileInsertOpts is the InsertOpts every reconciler periodic-job builder
// must return. Extracted so a test can exercise the exact production value
// (asserting the closures embed the right Queue) without scraping source code.
func reconcileInsertOpts() *river.InsertOpts {
	return &river.InsertOpts{Queue: queueReconcile}
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
func StartWorkers(ctx context.Context, db *sql.DB, rdb *redis.Client, cfg *config.Config, provClient *provisioner.Client, planRegistry PlanRegistry, deployStatusK8s deployStatusK8sProvider, nrApp *newrelic.Application) *Workers {
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

	emailClient := NewEmailClient(cfg.ResendAPIKey)

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
	river.AddWorker(workers, WithObservability(NewExpireAnonymousWorker(db, provClient, minioClient), nrApp))
	river.AddWorker(workers, WithObservability(NewExpireStacksWorker(db, cfg.KubeNamespaceApps+"-"), nrApp))
	river.AddWorker(workers, WithObservability(NewRefreshGeoDBWorker(), nrApp))
	// TrialExpiry / WeeklyDigest are registered via composite literal, so the
	// generic type parameter can't be inferred from the constructor return —
	// it must be supplied explicitly.
	river.AddWorker(workers, WithObservability[TrialExpiryArgs](&TrialExpiryWorker{db: db, email: emailClient}, nrApp))
	river.AddWorker(workers, WithObservability[WeeklyDigestArgs](&WeeklyDigestWorker{db: db, email: emailClient}, nrApp))
	river.AddWorker(workers, WithObservability(NewExpiryReminderWorker(db, emailClient), nrApp))
	// Resource-expiry-imminent producer: every 10 minutes, scan for
	// authenticated resources whose expires_at falls inside the next hour
	// and write one resource.expiry_imminent audit_log row per resource per
	// 12h dedupe window. The Loops event forwarder drains those rows into
	// Brevo lifecycle emails (event = resource_expiring_soon). Separate from
	// ExpiryReminderWorker because the delivery channel (Loops/Brevo vs
	// Resend) and dedupe surface (audit_log subquery vs resources column)
	// are independent. See expire_imminent.go for the full SCOPE NOTE.
	river.AddWorker(workers, WithObservability(NewExpireImminentWorker(db), nrApp))
	river.AddWorker(workers, WithObservability(NewEnforceStorageQuotaWorker(db, planRegistry), nrApp))
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
	river.AddWorker(workers, WithObservability(NewDeployStatusReconciler(db, deployStatusK8s), nrApp))
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
	// Deploy-notify webhook dispatcher — drains deploy.* audit_log rows
	// into per-team customer-configured webhook URLs. Forward-compat:
	// no-op when no team has a DEPLOY_NOTIFY_WEBHOOK_URL vault entry.
	// See deploy_notify_webhook.go for the SSRF / retry / cursor design.
	river.AddWorker(workers, WithObservability(NewDeployNotifyWebhookWorker(db, rdb, nil), nrApp))
	// Payment grace reminder — every 6h, emit payment.grace_reminder
	// audit rows for teams in active dunning whose last_reminder_at is
	// null or >6h ago. The event-email forwarder above drains those
	// rows into the configured email provider's reminder template.
	// See payment_grace_reminder.go.
	river.AddWorker(workers, WithObservability(NewPaymentGraceReminderWorker(db), nrApp))
	// Payment grace terminator — every 1h, POST to the api's
	// /internal/teams/:id/terminate for teams whose grace expired.
	// Misconfig-tolerant: short-circuits with a WARN when
	// INSTANT_API_INTERNAL_URL or WORKER_INTERNAL_JWT_SECRET is unset.
	// See payment_grace_terminator.go.
	river.AddWorker(workers, WithObservability(NewPaymentGraceTerminatorWorker(db, cfg.InstantAPIInternalURL, cfg.WorkerInternalJWTSecret, nil), nrApp))
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
	river.AddWorker(workers, WithObservability(NewCustomerBackupRunner(db, backupStore, cfg.BackupS3Bucket, cfg.BackupS3PathPrefix, cfg.AESKey), nrApp))
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

	periodicJobs := []*river.PeriodicJob{
		river.NewPeriodicJob(
			river.PeriodicInterval(1*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return ExpireAnonymousArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		river.NewPeriodicJob(
			river.PeriodicInterval(1*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return ExpireStacksArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		river.NewPeriodicJob(
			river.PeriodicInterval(30*24*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return RefreshGeoDBArgs{
					LicenseKey: cfg.MaxMindLicenseKey,
					DBPath:     cfg.GeoLite2DBPath,
				}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		river.NewPeriodicJob(
			scheduleFunc(func(t time.Time) time.Time {
				return t.Add(6 * time.Hour)
			}),
			func() (river.JobArgs, *river.InsertOpts) {
				return TrialExpiryArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		river.NewPeriodicJob(
			mondayAt8UTCSchedule{},
			func() (river.JobArgs, *river.InsertOpts) {
				return WeeklyDigestArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		river.NewPeriodicJob(
			river.PeriodicInterval(6*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return UpdateStorageBytesArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		river.NewPeriodicJob(
			river.PeriodicInterval(6*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return EnforceStorageQuotaArgs{}, nil
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
				return QuotaWallNudgeArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Expiry reminder — hourly sweep that emails owners of claimed-but-unpaid
		// (tier='free') resources whose expires_at is inside the next 4 hours.
		// Dedupe lives in the DB (resources.expiry_reminded_at) so one row gets
		// at most one email no matter how many ticks see it. See expiry_reminder.go.
		river.NewPeriodicJob(
			river.PeriodicInterval(1*time.Hour),
			func() (river.JobArgs, *river.InsertOpts) {
				return ExpiryReminderArgs{}, nil
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
				return ExpireImminentArgs{}, nil
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
				return CustomDomainReconcileArgs{}, reconcileInsertOpts()
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
				return DeployStatusReconcileArgs{}, reconcileInsertOpts()
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
				return EventEmailForwarderArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
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
				return ChurnPredictorArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Deploy-notify webhook dispatcher — every 30s, drain
		// deploy.* audit_log rows to customer webhook URLs. On
		// the reconcile queue so a backlog on the default queue
		// (weekly_digest, etc.) cannot starve it.
		river.NewPeriodicJob(
			river.PeriodicInterval(deployNotifyWebhookInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return DeployNotifyWebhookArgs{}, reconcileInsertOpts()
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		),
		// Payment grace reminder — every 6h. RunOnStart=false: a
		// restart shouldn't immediately re-fire reminders that the
		// previous instance just sent.
		river.NewPeriodicJob(
			river.PeriodicInterval(paymentGraceReminderInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return PaymentGraceReminderArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
		// Payment grace terminator — every 1h. RunOnStart=true so
		// a worker restart immediately catches any rows whose clock
		// expired while we were down.
		river.NewPeriodicJob(
			river.PeriodicInterval(paymentGraceTerminatorInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return PaymentGraceTerminatorArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: true},
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
				return CustomerBackupSchedulerArgs{}, nil
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
				return CustomerBackupRunnerArgs{}, reconcileInsertOpts()
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
				return CustomerRestoreRunnerArgs{}, reconcileInsertOpts()
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
				return PlatformDBBackupArgs{}, nil
			},
			&river.PeriodicJobOpts{RunOnStart: false},
		),
	}

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
		"queues", fmt.Sprintf("%v", []string{river.QueueDefault, queueReconcile}),
		"max_workers", 5,
	)

	return &Workers{
		client:  riverClient,
		cancel:  cancel,
		started: true,
	}
}
