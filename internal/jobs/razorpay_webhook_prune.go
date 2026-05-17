package jobs

// razorpay_webhook_prune.go — daily prune of the razorpay_webhook_events
// dedup table.
//
// # Purpose
//
// The api inserts one razorpay_webhook_events row per Razorpay webhook
// delivery (keyed on the Razorpay event id) so a duplicate redelivery is a
// cheap no-op. The table is append-only and unbounded — migration 033's
// comments envisioned a periodic prune but no job ever shipped it. Without
// this worker the table grows forever; old rows have no consumer because
// Razorpay never redelivers an event more than a few days old.
//
// # Retention
//
// 30 days. Razorpay's published webhook retry window is far shorter than a
// month, so a 30-day-old dedup row can never block a legitimate redelivery.
// The wide margin keeps the prune safe even if Razorpay changes its retry
// policy.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// RazorpayWebhookPruneArgs is the River job payload for the daily prune.
// No fields — it's a periodic maintenance job.
type RazorpayWebhookPruneArgs struct{}

// Kind is the River worker key.
func (RazorpayWebhookPruneArgs) Kind() string { return "razorpay_webhook_prune" }

// razorpayWebhookEventsRetentionDays is how long razorpay_webhook_events rows
// are kept. 30 days is well beyond Razorpay's webhook retry window, so a
// pruned row can never cause a legitimate redelivery to be processed twice.
const razorpayWebhookEventsRetentionDays = 30

// RazorpayWebhookPruneWorker deletes razorpay_webhook_events rows older than
// razorpayWebhookEventsRetentionDays. Runs daily — the prune is a cheap
// indexed range delete.
type RazorpayWebhookPruneWorker struct {
	river.WorkerDefaults[RazorpayWebhookPruneArgs]
	db *sql.DB
}

// NewRazorpayWebhookPruneWorker constructs the prune worker.
func NewRazorpayWebhookPruneWorker(db *sql.DB) *RazorpayWebhookPruneWorker {
	return &RazorpayWebhookPruneWorker{db: db}
}

// Work executes one prune: DELETE FROM razorpay_webhook_events WHERE
// received_at < now() - interval '30 days'.
func (w *RazorpayWebhookPruneWorker) Work(ctx context.Context, _ *river.Job[RazorpayWebhookPruneArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.razorpay_webhook_prune")
	defer span.End()

	res, err := w.db.ExecContext(ctx, `
		DELETE FROM razorpay_webhook_events
		WHERE received_at < now() - INTERVAL '`+strconv.Itoa(razorpayWebhookEventsRetentionDays)+` days'
	`)
	if err != nil {
		return fmt.Errorf("RazorpayWebhookPruneWorker: delete failed: %w", err)
	}
	n, _ := res.RowsAffected()
	slog.Info("jobs.razorpay_webhook_prune.swept",
		"deleted_rows", n,
		"retention_days", razorpayWebhookEventsRetentionDays,
	)
	return nil
}
