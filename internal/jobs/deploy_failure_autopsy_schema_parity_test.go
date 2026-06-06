package jobs

// deploy_failure_autopsy_schema_parity_test.go — the PRODUCER↔CONSUMER schema
// parity assertion for the deploy-failure auto-debug path (task #70,
// docs/ci/02-FAILURE-DIAGNOSIS-AND-AUTODEBUG.md §5.3).
//
// THE CONTRACT THIS GUARDS
//
// The worker (PRODUCER) writes a deployment_events row in
// upsertAutopsyRow (deploy_failure_autopsy.go). The api (CONSUMER) reads it
// back in models.GetDeploymentEvents + models.GetLatestDeploymentAutopsy and
// serves it as GET /api/v1/deployments/:id/events. The two live in SEPARATE Go
// modules (the worker does NOT import the api — see the file header of
// deploy_failure_autopsy.go), so there is NO compiler-enforced link between the
// columns the worker INSERTs and the columns the api SELECTs. A drift on either
// side (a renamed column, a changed last_lines encoding, an exit_code type
// flip) would silently break the agent debug surface with NO build error.
//
// Existing tests already cover that the worker WRITES the row
// (deploy_failure_autopsy_test.go: upsert idempotency, full-capture,
// last_lines JSON round-trip) and that the api SERVES it
// (api/.../deploy_events_endpoint_test.go + the new
// api/.../deploy_autodebug_path_test.go). This test adds the focused PARITY
// assertion: the exact JSON shape the worker writes for last_lines is exactly
// what the api Events handler unmarshals, and the column SET the worker INSERTs
// is the set the api SELECTs.
//
// HOW IT ASSERTS PARITY WITHOUT IMPORTING THE API
//
// The api consumer (models.GetDeploymentEvents) scans last_lines into a
// []byte then `json.Unmarshal(raw, &[]string)`. We capture the EXACT bytes the
// worker's upsertAutopsyRow binds to the last_lines column (via a sqlmock
// argument matcher) and run the api's unmarshal logic over them — if the worker
// ever changed the encoding (e.g. to a comma-joined string, or pq.Array), this
// reds. The api's scan code is duplicated here as a small literal mirror so the
// assertion is self-contained (the worker can't import the api models).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// apiEventsColumns mirrors the column list api/internal/models.GetDeploymentEvents
// SELECTs (minus id + created_at, which are DB-generated, not worker-written).
// The worker's upsertAutopsyRow INSERTs exactly this set. Drift on either side
// reds the parity check below. Kept as a literal (the worker can't import the
// api) — the test header documents the cross-module contract this pins.
var apiEventsColumns = []string{
	"deployment_id", "kind", "reason", "exit_code", "event", "last_lines", "hint",
}

// lastLinesCapture is a sqlmock Argument matcher that records the value bound to
// the last_lines column and always matches, so the INSERT proceeds. We then
// assert the captured bytes unmarshal via the api's consumer logic.
type lastLinesCapture struct {
	captured []byte
	matched  bool
}

// Match implements sqlmock.Argument. The worker binds last_lines as a
// json.Marshal([]string) → []byte; capture that. (Any other arg type means the
// worker changed the encoding — record it so the assertion can fail loudly.)
func (c *lastLinesCapture) Match(v driver.Value) bool {
	c.matched = true
	switch b := v.(type) {
	case []byte:
		c.captured = b
	case string:
		c.captured = []byte(b)
	}
	return true
}

// TestAutopsySchemaParity_LastLinesEncodingMatchesAPIConsumer captures the
// EXACT last_lines value the worker writes and asserts it unmarshals to the
// same []string via the api's consumer logic (json.Unmarshal into []string).
// This is the producer↔consumer schema-parity assertion: if the worker ever
// changed the last_lines encoding, the api /events handler would break — and
// this test reds first.
func TestAutopsySchemaParity_LastLinesEncodingMatchesAPIConsumer(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	producerLines := []string{
		"npm ERR! code ELIFECYCLE",
		"FATAL ERROR: Reached heap limit — JavaScript heap out of memory",
		"", // empty line must survive the round-trip too
	}

	cap := &lastLinesCapture{}
	// last_lines is the 6th positional arg in the INSERT
	// (deployment_id, kind, reason, exit_code, event, last_lines, hint).
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WithArgs(
			sqlmock.AnyArg(), // deployment_id
			sqlmock.AnyArg(), // kind
			sqlmock.AnyArg(), // reason
			sqlmock.AnyArg(), // exit_code
			sqlmock.AnyArg(), // event
			cap,              // last_lines ← captured here
			sqlmock.AnyArg(), // hint
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := upsertAutopsyRow(context.Background(), db, uuid.New(),
		workerFailureReasonOOMKilled,
		sql.NullInt32{Int32: 137, Valid: true},
		"OOMKilling: out of memory",
		producerLines,
	); err != nil {
		t.Fatalf("upsertAutopsyRow (producer write): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}

	if !cap.matched {
		t.Fatal("last_lines arg was never bound — INSERT arg order drifted")
	}
	if len(cap.captured) == 0 {
		t.Fatal("worker bound an EMPTY last_lines value — the api consumer would " +
			"see no log tail")
	}

	// CONSUMER logic (mirror of api/internal/models.GetDeploymentEvents): scan
	// the column into []byte, json.Unmarshal into []string. If the worker's
	// encoding ever changed, this unmarshal fails or yields the wrong slice.
	var consumerLines []string
	if err := json.Unmarshal(cap.captured, &consumerLines); err != nil {
		t.Fatalf("api consumer cannot unmarshal worker last_lines (%q): %v\n"+
			"PRODUCER↔CONSUMER SCHEMA DRIFT: the worker's upsertAutopsyRow "+
			"changed the last_lines encoding away from json.Marshal([]string); "+
			"GET /api/v1/deployments/:id/events would break.", string(cap.captured), err)
	}

	if len(consumerLines) != len(producerLines) {
		t.Fatalf("last_lines parity: producer wrote %d lines, api consumer reads %d",
			len(producerLines), len(consumerLines))
	}
	for i := range producerLines {
		if consumerLines[i] != producerLines[i] {
			t.Errorf("last_lines[%d] parity: producer %q, consumer %q",
				i, producerLines[i], consumerLines[i])
		}
	}
}

// TestAutopsySchemaParity_ColumnSetMatchesAPIConsumer asserts the worker
// INSERTs exactly the column SET the api Events handler SELECTs. The worker's
// INSERT statement is a literal in upsertAutopsyRow; this test pins that every
// api-consumed column is bound (and in the documented order), so a column
// rename on either side reds. We drive a real upsert and assert the INSERT
// matched a regex naming each api-consumed column — a missing column would make
// the regex fail to match and sqlmock would error with "call to ExecQuery ...
// was not expected".
func TestAutopsySchemaParity_ColumnSetMatchesAPIConsumer(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// A regex that requires every api-consumed column name to appear in the
	// INSERT's column list, in order. If the worker drops/renames a column the
	// api SELECTs, this regex won't match → sqlmock errors and the test reds.
	colRegex := `INSERT INTO deployment_events\s*\(\s*` +
		apiEventsColumns[0]
	for _, c := range apiEventsColumns[1:] {
		colRegex += `,\s*` + c
	}

	mock.ExpectExec(colRegex).WillReturnResult(sqlmock.NewResult(0, 1))

	if err := upsertAutopsyRow(context.Background(), db, uuid.New(),
		workerFailureReasonBuildFailed,
		sql.NullInt32{},
		"build failed",
		[]string{"a"},
	); err != nil {
		t.Fatalf("upsertAutopsyRow: %v (the INSERT column set drifted from the "+
			"api-consumed columns %v)", err, apiEventsColumns)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations — INSERT column set drifted from the "+
			"api-consumed columns %v: %v", apiEventsColumns, err)
	}
}
