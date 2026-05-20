package jobs

// chaos_lease_recovery.go — STUB JOB for the chaos drill of 2026-05-20.
//
// See CHAOS-DRILL-2026-05-20.md (repo root) for the full procedure.
//
// ─── WHAT THIS JOB EXISTS FOR ─────────────────────────────────────────────────
//
// CLAUDE.md rule 12 ("Shipped ≠ Verified"). When task #172 added
// `JobTimeout: globalJobTimeout` to the River client config (workers.go), the
// CHANGE was shipped but the FAILURE CASE — what happens when a pod dies
// mid-job-execution — was never exercised. River's lease-takeover (rescuer)
// path is what guarantees an orphaned job re-leases to a sibling worker
// instead of being lost. The rescuer was deliberately set to its default in
// our config:
//
//	JobRescuerRescueAfterDefault = time.Hour      (from river internals)
//	JobTimeout                   = 20 * time.Minute (workers.go const)
//
// The effective rescue window is therefore
//	max_takeover_RTO = JobTimeout + JobRescuerRescueAfterDefault ≈ 1h20m
//
// — a job orphaned by an OOMKill mid-execution can stay un-leased for up to
// 80 minutes before another worker picks it up. That is a FINDING from the
// drill, not something we knew before the drill ran.
//
// ─── HOW THIS JOB IS USED IN THE DRILL ────────────────────────────────────────
//
// The drill in api/e2e/propagation_chaos_test.go enqueues ONE
// ChaosLeaseRecoveryArgs row, then:
//
//	1. Waits for the START audit_log marker (chaos.lease_recovery.start)
//	   to be persisted by some worker pod. The marker carries pod_id =
//	   $HOSTNAME so the test knows which pod owns the in-flight job.
//	2. `kubectl delete pod -n instant-infra <that-pod-id> --grace-period=0
//	   --force` — simulates OOMKill (no graceful drain, no defer
//	   completion).
//	3. The job's sleep wakes up in the (now-doomed) original pod or is
//	   already terminated mid-sleep. River's rescuer (default 1h interval,
//	   1h RescueAfter) eventually re-leases the row to a different pod.
//	4. The other pod runs the job from scratch (River retries from
//	   `attempted_at` reset — semantics matches "at-least-once
//	   delivery"). It writes its OWN start marker, sleeps, writes an END
//	   marker chaos.lease_recovery.end.
//	5. The test asserts:
//	     - chaos.lease_recovery.start rows exist with TWO distinct
//	       pod_id values (the killed pod + the rescuer pod), or one
//	       start+one end if the original pod is killed AFTER the end
//	       marker (the test handles both orderings).
//	     - chaos.lease_recovery.end exists exactly once (the job
//	       eventually completed successfully).
//	     - Wall-clock from FIRST start marker to END marker is the
//	       observed lease-recovery RTO. This is the real number the
//	       drill produces.
//
// ─── PARAMETERS ───────────────────────────────────────────────────────────────
//
// The job's payload carries:
//
//   - SleepSeconds — how long the worker holds the slot. 30s is enough for
//     the operator (or the drill test) to `kubectl get pods -l app=
//     instant-worker -w` and pick the running pod to kill, while short
//     enough not to occupy a worker slot for 5 minutes if the kill never
//     happens. 0 = a normal completion (no kill) — useful for testing the
//     happy path of the stub.
//
//   - RunID — a stable string the test generates per-drill so it can find
//     ITS audit rows among multiple concurrent drill runs.
//
// ─── IDEMPOTENCY ──────────────────────────────────────────────────────────────
//
// The audit_log writes are appended unconditionally. A re-run from the
// rescuer's takeover yields a SECOND start marker (with a different
// pod_id) — that is the SIGNAL the drill keys on. We do NOT dedupe in
// this stub; the whole point is to expose the at-least-once execution
// semantics River guarantees.
//
// ─── SAFETY ──────────────────────────────────────────────────────────────────
//
// The job does NO real work. It sleeps + writes audit_log markers under
// the synthetic team_id supplied in the payload (the drill seeds a
// synthetic team for this purpose). No customer resource is touched.
// The job kind is documented as "chaos-drill-only" and is never enqueued
// outside of the drill — there is no periodic schedule, no api enqueue
// path. The worker only RUNS it if a row already exists in river_job
// with kind=chaos_lease_recovery, which only happens when the drill
// inserts one.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

// ─── named constants ─────────────────────────────────────────────────────────

const (
	// chaosLeaseRecoveryKind is the River worker kind. Mirrored as a
	// constant in api/e2e/lease_recovery_chaos_test.go so the test can
	// query river_job for status.
	chaosLeaseRecoveryKind = "chaos_lease_recovery"

	// AuditKindChaosLeaseRecoveryStart / End are the markers the drill
	// test polls audit_log for. The summaries are deliberately distinctive
	// so a support query can find them long after a drill.
	AuditKindChaosLeaseRecoveryStart = "chaos.lease_recovery.start"
	AuditKindChaosLeaseRecoveryEnd   = "chaos.lease_recovery.end"

	// chaosLeaseRecoveryActor is the audit_log.actor value. Mirrors the
	// propagation_runner / billing_reconciler conventions (one actor per
	// subsystem).
	chaosLeaseRecoveryActor = "chaos_lease_recovery"

	// chaosLeaseRecoveryMaxSleep clamps the SleepSeconds payload value to
	// guard against an oversized sleep accidentally seeded by a typo —
	// the job is otherwise indistinguishable from a real long-running
	// task to River's slot scheduler.
	chaosLeaseRecoveryMaxSleep = 5 * time.Minute
)

// ─── job definition ───────────────────────────────────────────────────────────

// ChaosLeaseRecoveryArgs is the River job payload for the lease-recovery
// drill. Field names are JSON-tagged because River serialises args through
// encoding/json.
type ChaosLeaseRecoveryArgs struct {
	// SleepSeconds is how long the worker holds the slot before
	// completing. Clamped at chaosLeaseRecoveryMaxSleep.
	SleepSeconds int `json:"sleep_seconds"`
	// TeamID is the synthetic team the drill created; the worker writes
	// audit_log rows against this team_id.
	TeamID string `json:"team_id"`
	// RunID is a stable identifier the drill uses to find ITS audit rows
	// among multiple concurrent drill runs.
	RunID string `json:"run_id"`
}

// Kind is the River worker key.
func (ChaosLeaseRecoveryArgs) Kind() string { return chaosLeaseRecoveryKind }

// InsertOpts overrides the default queue + uniqueness rules for this kind.
// We deliberately do NOT set river.UniqueOpts so a drill that takes over
// after a kill genuinely sees a separate enqueue → River picks the row up
// on a fresh attempt rather than treating it as a duplicate.
func (ChaosLeaseRecoveryArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		// Drill jobs run on the default queue alongside the bulk-email +
		// heavyweight periodics. They occupy ONE slot (MaxWorkers=5) for
		// SleepSeconds — a 30s sleep is cheap.
		Queue:    river.QueueDefault,
		Priority: 4, // lowest priority — never starves real work
	}
}

// ChaosLeaseRecoveryWorker is the in-process executor.
type ChaosLeaseRecoveryWorker struct {
	river.WorkerDefaults[ChaosLeaseRecoveryArgs]
	db *sql.DB
}

// NewChaosLeaseRecoveryWorker constructs the worker.
func NewChaosLeaseRecoveryWorker(db *sql.DB) *ChaosLeaseRecoveryWorker {
	return &ChaosLeaseRecoveryWorker{db: db}
}

// Work executes the drill job. Writes a START marker, sleeps, writes an END
// marker. Both markers carry the pod's $HOSTNAME so the drill test can tell
// which pod ran each side of the kill.
func (w *ChaosLeaseRecoveryWorker) Work(ctx context.Context, job *river.Job[ChaosLeaseRecoveryArgs]) error {
	args := job.Args
	sleep := time.Duration(args.SleepSeconds) * time.Second
	if sleep < 0 {
		sleep = 0
	}
	if sleep > chaosLeaseRecoveryMaxSleep {
		sleep = chaosLeaseRecoveryMaxSleep
	}

	pod := podHostname()

	// Parse the team_id — the audit_log FK requires a real uuid.
	teamID, err := uuid.Parse(args.TeamID)
	if err != nil {
		return fmt.Errorf("ChaosLeaseRecoveryWorker.Work: parse team_id %q: %w", args.TeamID, err)
	}

	startedAt := time.Now()
	if mErr := w.markChaos(ctx, teamID, args, pod, AuditKindChaosLeaseRecoveryStart, "drill start", startedAt, 0); mErr != nil {
		return fmt.Errorf("write start marker: %w", mErr)
	}
	slog.Info("jobs.chaos_lease_recovery.start",
		"run_id", args.RunID,
		"team_id", args.TeamID,
		"pod", pod,
		"sleep", sleep.String(),
		"river_attempt", job.Attempt,
	)

	// The sleep is the part the kill is intended to interrupt. If ctx
	// expires (River cancels via JobTimeout or the pod terminates), exit
	// with an error so River retries via the rescuer.
	select {
	case <-time.After(sleep):
		// Normal completion — the kill never happened, or the job was
		// re-leased AFTER the original sleep would have completed.
	case <-ctx.Done():
		// Pod is being torn down or River cancelled the job. Return ctx.Err
		// so River reschedules.
		slog.Warn("jobs.chaos_lease_recovery.interrupted",
			"run_id", args.RunID,
			"team_id", args.TeamID,
			"pod", pod,
			"river_attempt", job.Attempt,
			"reason", ctx.Err(),
		)
		return fmt.Errorf("ctx done before completion: %w", ctx.Err())
	}

	endedAt := time.Now()
	duration := endedAt.Sub(startedAt)
	if mErr := w.markChaos(ctx, teamID, args, pod, AuditKindChaosLeaseRecoveryEnd, "drill end", endedAt, duration); mErr != nil {
		return fmt.Errorf("write end marker: %w", mErr)
	}
	slog.Info("jobs.chaos_lease_recovery.end",
		"run_id", args.RunID,
		"team_id", args.TeamID,
		"pod", pod,
		"duration_ms", duration.Milliseconds(),
		"river_attempt", job.Attempt,
	)
	return nil
}

// markChaos writes one audit_log row carrying the drill markers + pod id.
// The metadata JSONB carries everything the drill test needs to assert the
// recovery shape.
func (w *ChaosLeaseRecoveryWorker) markChaos(ctx context.Context, teamID uuid.UUID, args ChaosLeaseRecoveryArgs, pod, kind, summary string, ts time.Time, duration time.Duration) error {
	meta, _ := json.Marshal(map[string]any{
		"run_id":        args.RunID,
		"pod":           pod,
		"sleep_seconds": args.SleepSeconds,
		"duration_ms":   duration.Milliseconds(),
		"ts":            ts.UTC().Format(time.RFC3339Nano),
	})
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1::uuid, $2, $3, $4, $5::jsonb)
	`, teamID, chaosLeaseRecoveryActor, kind, summary, meta)
	return err
}

// podHostname returns $HOSTNAME (k8s injects the pod name into HOSTNAME for
// every container) or "unknown" when unset. Used as the pod_id marker in
// audit rows so the drill test can distinguish the killed pod from the
// rescuer pod.
func podHostname() string {
	if v := os.Getenv("HOSTNAME"); v != "" {
		return v
	}
	return "unknown"
}
