package jobs

// idle_tick_log_level_test.go — T21 P1-1 regression guard (BugBash 2026-05-20).
//
// The fix demotes idle-tick `.completed` INFO lines to DEBUG across eight
// noisy jobs. This test is a SOURCE-LEVEL guard: a future edit that
// reintroduces an idle-tick INFO emit on a high-frequency job is caught
// by string-search instead of waiting for prod logs to flood NR again.
//
// The list of jobs is enumerated explicitly (NOT a glob) because new
// noisy jobs must be reviewed for their idle behaviour — a glob would
// silently miss them.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestIdleTickLogLevel_DemotedToDebug pins the demotion in source. For
// each job file listed below, the test asserts there is no
// `slog.Info(.*\.completed",` line inside an `if len(...) == 0 {` block.
// We approximate by requiring each listed job's `.completed` Info line
// to either:
//   - not exist at all (single-completed-line jobs whose work-tick guard
//     means the line only fires on non-zero work), OR
//   - co-occur with a `slog.Debug(.*\.completed"` line earlier (the idle
//     branch demoted to DEBUG; the INFO line is the work-done branch).
//
// This is intentionally a heuristic — the precise property is "no
// idle-tick INFO" — but it catches the modal regression (someone reverts
// the Info→Debug change on a single job).
func TestIdleTickLogLevel_DemotedToDebug(t *testing.T) {
	// The 8 jobs the T21 P1-1 fix touched. A new addition to this list
	// requires (a) demoting the file's idle-tick completed log to DEBUG,
	// and (b) adding the file here.
	jobs := []string{
		"deploy_status_reconcile.go",
		"deploy_notify_webhook.go",
		"magic_link_reconciler.go",
		"pending_deletion_expirer.go",
		"deployment_expirer.go",
		"provisioner_reconciler.go",
		"customer_backup_runner.go",
		"customer_restore_runner.go",
	}

	// Pattern for an idle-tick INFO line: `slog.Info("jobs.<job>.completed",`
	// followed within ~150 chars by a zero-marker (`"...", 0,` or `"candidates", 0,`).
	// We don't try to parse Go — just sanity-grep the source for an INFO
	// line that lives inside an `if len(...) == 0 {` block, identified by
	// proximity (the `0,` marker on the next-line slog field).
	infoIdleRE := regexp.MustCompile(`slog\.Info\(\s*"jobs\.[a-z_]+\.completed"\s*,[^)]*\b0\s*,\s*\n`)

	pkgDir := "."
	for _, f := range jobs {
		path := filepath.Join(pkgDir, f)
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		body := string(src)

		// Each file should have a DEBUG branch for the idle path. Pin
		// that the demotion landed.
		if !strings.Contains(body, "slog.Debug(") {
			t.Errorf("%s: T21 P1-1 regression — file no longer carries a slog.Debug(...) call for the idle-tick path. The idle-tick `.completed` INFO line must be demoted to DEBUG (or guarded by a work-done conditional that demotes to DEBUG on processed=0).",
				f)
		}

		// And the file must NOT carry an INFO `.completed` line followed
		// immediately by a `0,` zero-marker — that is the modal shape
		// of the pre-fix idle-tick spam.
		if loc := infoIdleRE.FindStringIndex(body); loc != nil {
			snippet := body[loc[0]:min(loc[0]+200, len(body))]
			t.Errorf("%s: T21 P1-1 regression — found an idle-tick INFO emit shape (`slog.Info(...completed..., 0,`) at byte offset %d. The idle-path must be DEBUG, not INFO.\nSnippet:\n%s",
				f, loc[0], snippet)
		}
	}
}
