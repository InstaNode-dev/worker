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
