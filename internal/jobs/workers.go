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
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"instant.dev/worker/internal/config"
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
func StartWorkers(ctx context.Context, db *sql.DB, rdb *redis.Client, cfg *config.Config, provClient *provisioner.Client, planRegistry PlanRegistry, deployStatusK8s deployStatusK8sProvider) *Workers {
	_ = rdb // available for future workers; currently only used by quota checks done via db

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

	// Build MinIO admin client for storage IAM cleanup — nil if not configured (fail open).
	var minioClient *madmin.AdminClient
	if cfg.MinioEndpoint != "" {
		if mc, err := madmin.New(cfg.MinioEndpoint, cfg.MinioRootUser, cfg.MinioRootPassword, false); err != nil {
			slog.Warn("jobs.workers.minio_client_init_failed", "error", err)
		} else {
			minioClient = mc
		}
	}

	// Build MinIO storage scanner for the UpdateStorageBytesWorker — nil if
	// MINIO_ENDPOINT is not set (fail open: storage_bytes updates for MinIO
	// resources are skipped each run, postgres/redis/mongo continue via the
	// gRPC provisioner path).
	var minioScanner MinIOStorageScanner
	if cfg.MinioEndpoint != "" {
		if scanner, err := NewMinIOStorageScanner(cfg.MinioEndpoint, cfg.MinioRootUser, cfg.MinioRootPassword, cfg.MinioBucketName); err != nil {
			slog.Warn("jobs.workers.minio_storage_scanner_init_failed", "error", err)
		} else {
			minioScanner = scanner
		}
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, NewExpireAnonymousWorker(db, provClient, minioClient))
	river.AddWorker(workers, NewExpireStacksWorker(db, cfg.KubeNamespaceApps+"-"))
	river.AddWorker(workers, NewRefreshGeoDBWorker())
	river.AddWorker(workers, &TrialExpiryWorker{db: db, email: emailClient})
	river.AddWorker(workers, &WeeklyDigestWorker{db: db, email: emailClient})
	river.AddWorker(workers, NewEnforceStorageQuotaWorker(db, planRegistry))
	river.AddWorker(workers, NewUpdateStorageBytesWorker(db, provClient, minioScanner))
	// Custom-domain reconciler — TXT lookup, HTTP probe, stale-failed sweep.
	// k8s provider is nil today: the worker module does not import the api's
	// k8s client. Steps 2/3 (Ingress + cert poll) stay in the api handler.
	// See custom_domain_reconcile.go for the full SCOPE NOTE.
	river.AddWorker(workers, NewCustomDomainReconciler(db, nil, nil))
	// Deploy-status reconciler — sweeps non-terminal deployments and rolls
	// status forward from live k8s Deployment state every 30s. deployStatusK8s
	// may be nil (kubeconfig unreachable in CI / docker-compose); the worker
	// then short-circuits with a WARN each tick. See deploy_status_reconcile.go.
	river.AddWorker(workers, NewDeployStatusReconciler(db, deployStatusK8s))

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
