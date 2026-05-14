package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"instant.dev/common/buildinfo"
	"instant.dev/common/logctx"
	commonplans "instant.dev/common/plans"
	"instant.dev/worker/internal/config"
	"instant.dev/worker/internal/db"
	"instant.dev/worker/internal/jobs"
	"instant.dev/worker/internal/obs"
	"instant.dev/worker/internal/provisioner"
	"instant.dev/worker/internal/telemetry"
)

func main() {
	// Structured JSON logging — wrapped in logctx so every line carries
	// service + commit_id + (when present) tid / trace_id / team_id.
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})
	slog.SetDefault(slog.New(logctx.NewHandler("worker", base)))

	// New Relic Go agent. Fail-open on empty / missing license so local dev
	// and CI runs (which never get a real key) still boot. Matches the
	// contract of telemetry.InitTracer below.
	nrApp, _ := obs.InitNewRelic()
	defer func() {
		if nrApp != nil {
			nrApp.Shutdown(5 * time.Second)
		}
	}()

	shutdownTracer := telemetry.InitTracer("instant-worker", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	defer func() {
		if err := shutdownTracer(context.Background()); err != nil {
			slog.Error("telemetry.shutdown_failed", "error", err)
		}
	}()

	cfg := config.Load() // panics on missing required env vars

	database := db.ConnectPostgres(cfg.DatabaseURL)
	defer database.Close()

	rdb := db.ConnectRedis(cfg.RedisURL)
	defer rdb.Close()

	var provClient *provisioner.Client
	if cfg.ProvisionerAddr != "" {
		var conn *grpc.ClientConn
		var err error
		provClient, conn, err = provisioner.NewClient(cfg.ProvisionerAddr, cfg.ProvisionerSecret)
		if err != nil {
			slog.Error("worker.provisioner_connect_failed", "error", err)
			os.Exit(1)
		}
		defer conn.Close()
		slog.Info("worker.provisioner_connected", "addr", cfg.ProvisionerAddr)
	} else {
		slog.Info("worker.provisioner_not_configured", "note", "PROVISIONER_ADDR not set — UpdateStorageBytesWorker will be a no-op")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	plansPath := cfg.PlansPath
	if plansPath == "" {
		plansPath = "plans.yaml"
	}
	planRegistry, err := commonplans.Load(plansPath)
	if err != nil {
		slog.Warn("worker.plans_load_failed_using_defaults", "error", err, "path", plansPath)
		planRegistry = commonplans.Default()
	}

	// Build the k8s client used by DeployStatusReconciler. Fails open: if
	// neither in-cluster config nor kubeconfig is reachable (CI, docker-compose,
	// bare-metal dev box), we pass nil and the reconciler warn-logs each tick
	// while every other periodic job keeps running. See
	// worker/internal/jobs/deploy_status_reconcile.go for the SCOPE NOTE.
	deployStatusK8s, k8sErr := jobs.NewK8sDeployStatusClient()
	if k8sErr != nil {
		slog.Warn("worker.deploy_status_k8s_client_init_failed",
			"error", k8sErr,
			"note", "DeployStatusReconciler will log warnings each tick; other periodic jobs unaffected")
		deployStatusK8s = nil
	} else {
		slog.Info("worker.deploy_status_k8s_client_ready")
	}

	// Build the BackupPlanRegistry adapter from the same *commonplans.Registry.
	// CustomerBackupRunner reads tier→retention_days from plans.yaml via this
	// adapter; passing nil falls back to a legacy 7-day default with a WARN.
	backupPlans := jobs.NewBackupPlanRegistry(planRegistry)

	workers := jobs.StartWorkers(ctx, database, rdb, cfg, provClient, planRegistry, backupPlans, deployStatusK8s, nrApp)
	defer workers.Stop()

	// Exit immediately if River failed to start so Kubernetes restarts the pod.
	// A process that is alive but has no active River client is worse than a crash:
	// k8s thinks the pod is healthy while no jobs are being processed.
	if !workers.Started() {
		slog.Error("worker.river_start_failed — exiting so k8s restarts the pod")
		os.Exit(1)
	}

	// Liveness HTTP server on port 8091.
	// k8s livenessProbe GETs /healthz — a 200 means the process and River are up.
	// If this process is alive, River is running (startup failure exits above).
	// If River's goroutines panic after start, Go crashes the process and k8s restarts.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"ok":true,"service":"instant-worker","commit_id":%q,"build_time":%q,"version":%q}`,
			buildinfo.GitSHA, buildinfo.BuildTime, buildinfo.Version)
	})
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":8091", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("worker.liveness_server_failed", "error", err)
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	slog.Info("worker.started",
		"environment", cfg.Environment,
		"liveness_port", 8091,
		"commit_id", buildinfo.GitSHA,
		"build_time", buildinfo.BuildTime,
		"version", buildinfo.Version,
	)
	<-ctx.Done()
	slog.Info("worker.shutdown")
}
