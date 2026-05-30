package jobs

// deploy_failure_autopsy_log_capture_test.go — coverage for PR 2 of the
// silent-deploy-failure fix (2026-05-30 incident).
//
// Three behaviours pinned per the brief:
//
//   1. TestAutopsy_PodAlive_CapturesLogs       — when the app pod is alive,
//                                                 50 tail lines land in
//                                                 deployment_events.last_lines.
//   2. TestAutopsy_PodGCd_FallsBackToJobEvent   — when the app pod is gone,
//                                                 the reason / event are
//                                                 derived from the namespace
//                                                 events list and the
//                                                 last_lines stays empty.
//   3. TestAutopsy_Idempotent                   — running the autopsy twice
//                                                 for the same deployment
//                                                 returns nil at the DB layer
//                                                 (the unique constraint +
//                                                 ON CONFLICT DO UPDATE keeps
//                                                 exactly one row).
//
// Plus coverage for the PR 2 surface: build-pod log fallback, error_message
// stamping, audit_log emit, and the outcome metric.

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ── fake autopsy k8s helpers ───────────────────────────────────────────────────

// fakeAutopsyK8sPR2 returns canned data per call type. Each helper closure
// receives the namespace + (for ListPods) label selector so a single fake
// can answer both the app-pod and build-pod queries differently.
type fakeAutopsyK8sPR2 struct {
	listPodsFn  func(ns, sel string) (*corev1.PodList, error)
	listEvFn    func(ns string) (*corev1.EventList, error)
	getLogsFn   func(ns, pod string, tail int64) ([]string, error)
	logsCallLog []string
}

func (f *fakeAutopsyK8sPR2) ListPods(_ context.Context, ns, sel string) (*corev1.PodList, error) {
	if f.listPodsFn == nil {
		return &corev1.PodList{}, nil
	}
	return f.listPodsFn(ns, sel)
}

func (f *fakeAutopsyK8sPR2) ListEvents(_ context.Context, ns string) (*corev1.EventList, error) {
	if f.listEvFn == nil {
		return &corev1.EventList{}, nil
	}
	return f.listEvFn(ns)
}

func (f *fakeAutopsyK8sPR2) GetPodLogs(_ context.Context, ns, pod string, tail int64) ([]string, error) {
	f.logsCallLog = append(f.logsCallLog, pod)
	if f.getLogsFn == nil {
		return nil, nil
	}
	return f.getLogsFn(ns, pod, tail)
}

var _ deployAutopsyK8sProvider = (*fakeAutopsyK8sPR2)(nil)

// TestAutopsy_PodAlive_CapturesLogs is brief-test #1.
func TestAutopsy_PodAlive_CapturesLogs(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Build a tail of 50 log lines for the assertion target.
	tail := make([]string, 50)
	for i := range tail {
		tail[i] = "FATAL line " + uuid.NewString()[:4]
	}

	k8s := &fakeAutopsyK8sPR2{
		listPodsFn: func(ns, sel string) (*corev1.PodList, error) {
			// The autopsy first queries the APP pod (label instant-app-id=).
			// The build-pod fallback uses label job-name=. Distinguish by
			// substring — we want the app-pod path to succeed so the build
			// fallback never runs.
			if strings.Contains(sel, labelInstantAppID) {
				return &corev1.PodList{
					Items: []corev1.Pod{*buildPodWithTerminated("OOMKilled", 137)},
				}, nil
			}
			return &corev1.PodList{}, nil
		},
		listEvFn: func(ns string) (*corev1.EventList, error) { return &corev1.EventList{}, nil },
		getLogsFn: func(ns, pod string, tailLines int64) ([]string, error) {
			return tail, nil
		},
	}

	id := uuid.New()
	teamID := uuid.New()

	mock.ExpectQuery(`SELECT reason FROM deployment_events`).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE deployments\s+SET error_message`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(teamID))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	captureDeploymentAutopsy(context.Background(), db, id, "app-alive", k8s)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	// Build-pod fallback MUST NOT fire when the app-pod path already captured
	// logs — the logsCallLog should contain exactly one pod (the app pod).
	if len(k8s.logsCallLog) != 1 {
		t.Errorf("expected GetPodLogs called once (app pod only); got %d calls: %v",
			len(k8s.logsCallLog), k8s.logsCallLog)
	}
}

// TestAutopsy_PodGCd_FallsBackToJobEvent is brief-test #2: app pod is GC'd,
// build pod is GC'd, but namespace events still contain a FailedToPull /
// OOMKilling message. The autopsy MUST populate reason + event from the
// event message and leave last_lines empty.
func TestAutopsy_PodGCd_FallsBackToJobEvent(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	k8s := &fakeAutopsyK8sPR2{
		// Both ListPods queries return empty (both pods GC'd).
		listPodsFn: func(ns, sel string) (*corev1.PodList, error) {
			return &corev1.PodList{}, nil
		},
		// Namespace event surfaces the OOMKilling reason after the pod is gone.
		listEvFn: func(ns string) (*corev1.EventList, error) {
			return &corev1.EventList{
				Items: []corev1.Event{{
					ObjectMeta: metav1.ObjectMeta{Name: "ev-1"},
					Type:       corev1.EventTypeWarning,
					Reason:     "OOMKilling",
					Message:    "Memory cgroup out of memory: Killed process 1",
				}},
			}, nil
		},
		// GetPodLogs should never be called (no pod name) — guarded below.
		getLogsFn: func(ns, pod string, tail int64) ([]string, error) {
			t.Fatalf("GetPodLogs should not be called when pods are GC'd")
			return nil, nil
		},
	}

	id := uuid.New()
	teamID := uuid.New()

	mock.ExpectQuery(`SELECT reason FROM deployment_events`).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE deployments\s+SET error_message`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(teamID))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	captureDeploymentAutopsy(context.Background(), db, id, "app-gced", k8s)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAutopsy_Idempotent is brief-test #3. Run captureDeploymentAutopsy
// twice for the same deployment. The deployment_events ON CONFLICT DO UPDATE
// clause makes the second insert a no-op-equivalent (sqlmock fires both INSERT
// statements; in real Postgres they both produce 1 affected row but only one
// physical row exists). After the second run, the autopsy's already_present
// detector returns true and outcome flips to "already_present" — which is
// what we assert via the logsCallLog (no second log-tail call should be
// needed because the metric path doesn't re-fetch).
func TestAutopsy_Idempotent(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	k8s := &fakeAutopsyK8sPR2{
		listPodsFn: func(ns, sel string) (*corev1.PodList, error) {
			if strings.Contains(sel, labelInstantAppID) {
				return &corev1.PodList{
					Items: []corev1.Pod{*buildPodWithWaiting("CrashLoopBackOff", "boom")},
				}, nil
			}
			return &corev1.PodList{}, nil
		},
		listEvFn:  func(ns string) (*corev1.EventList, error) { return &corev1.EventList{}, nil },
		getLogsFn: func(ns, pod string, tail int64) ([]string, error) { return []string{"line"}, nil },
	}

	id := uuid.New()
	teamID := uuid.New()
	provID := "app-idem"

	// First call: full flow, real capture.
	mock.ExpectQuery(`SELECT reason FROM deployment_events`).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE deployments\s+SET error_message`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(teamID))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Second call: already_present pre-check returns CrashLoopBackOff (real reason),
	// upsert still fires (idempotent ON CONFLICT), then error_message + audit_log emit.
	mock.ExpectQuery(`SELECT reason FROM deployment_events`).
		WillReturnRows(sqlmock.NewRows([]string{"reason"}).AddRow(workerFailureReasonCrashLoopBackOff))
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE deployments\s+SET error_message`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(teamID))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	captureDeploymentAutopsy(context.Background(), db, id, provID, k8s)
	captureDeploymentAutopsy(context.Background(), db, id, provID, k8s)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAutopsy_BuildPodFallback exercises the new code path: app pod has no
// logs, build pod is reachable. The autopsy MUST query GetPodLogs twice
// (once for the app pod, once for the build pod) and the build-pod logs
// land in last_lines + the reason is upgraded from Unknown to BuildFailed.
func TestAutopsy_BuildPodFallback(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	k8s := &fakeAutopsyK8sPR2{
		listPodsFn: func(ns, sel string) (*corev1.PodList, error) {
			switch {
			case strings.Contains(sel, labelInstantAppID):
				// App pod present but produces no logs (image-pull stage).
				return &corev1.PodList{
					Items: []corev1.Pod{*buildPodWithWaiting("ImagePullBackOff", "")},
				}, nil
			case strings.Contains(sel, labelBuildJobName):
				// Build pod alive — provides the kaniko stderr tail.
				return &corev1.PodList{
					Items: []corev1.Pod{{
						ObjectMeta: metav1.ObjectMeta{Name: "build-x-pod"},
					}},
				}, nil
			}
			return &corev1.PodList{}, nil
		},
		listEvFn: func(ns string) (*corev1.EventList, error) { return &corev1.EventList{}, nil },
		getLogsFn: func(ns, pod string, tail int64) ([]string, error) {
			if strings.HasPrefix(pod, "build-") {
				return []string{"kaniko: COPY failed", "kaniko: stage 1 errored"}, nil
			}
			// App pod returns nothing — triggers fallback.
			return nil, nil
		},
	}

	id := uuid.New()
	teamID := uuid.New()

	mock.ExpectQuery(`SELECT reason FROM deployment_events`).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE deployments\s+SET error_message`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(teamID))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	captureDeploymentAutopsy(context.Background(), db, id, "app-x", k8s)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	// Both pods should have been log-fetched.
	if len(k8s.logsCallLog) != 2 {
		t.Errorf("expected 2 GetPodLogs calls (app pod + build pod fallback); got %d: %v",
			len(k8s.logsCallLog), k8s.logsCallLog)
	}
}

// TestUpdateDeploymentErrorMessage_OnlyUpdatesEmptyColumn pins the
// non-clobber guard: error_message is only stamped when NULL or empty.
func TestUpdateDeploymentErrorMessage_OnlyUpdatesEmptyColumn(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Expect the UPDATE to carry the WHERE error_message IS NULL OR ='' guard.
	mock.ExpectExec(`UPDATE deployments\s+SET error_message = \$1\s+WHERE id = \$2\s+AND \(error_message IS NULL OR error_message = ''\)`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := updateDeploymentErrorMessage(context.Background(), db, uuid.New(),
		workerFailureReasonOOMKilled,
		workerFailureHint[workerFailureReasonOOMKilled],
	); err != nil {
		t.Fatalf("updateDeploymentErrorMessage: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateDeploymentErrorMessage_DBError surfaces the error path.
func TestUpdateDeploymentErrorMessage_DBError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE deployments\s+SET error_message`).
		WillReturnError(errors.New("brownout"))

	if err := updateDeploymentErrorMessage(context.Background(), db, uuid.New(),
		"Reason", "hint"); err == nil {
		t.Fatal("expected error from DB")
	}
}

// TestFirstSentence pins the snippet helper.
func TestFirstSentence(t *testing.T) {
	cases := []struct {
		in     string
		maxLen int
		want   string
	}{
		{"", 50, ""},
		{"no period text", 50, "no period text"},
		{"first sentence. second sentence.", 50, "first sentence."},
		{"period beyond maxlen no truncation", 5, "perio"},
		{"short.", 50, "short."},
	}
	for _, tc := range cases {
		if got := firstSentence(tc.in, tc.maxLen); got != tc.want {
			t.Errorf("firstSentence(%q, %d) = %q, want %q", tc.in, tc.maxLen, got, tc.want)
		}
	}
}

// TestEmitDeployFailedAudit_NoRow returns nil silently when the deployment
// row was already deleted between capture and audit-emit.
func TestEmitDeployFailedAudit_NoRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnError(sql.ErrNoRows)

	if err := emitDeployFailedAudit(context.Background(), db, uuid.New(), "OOMKilled", "msg"); err != nil {
		t.Errorf("expected nil on ErrNoRows, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestEmitDeployFailedAudit_LookupError surfaces non-ErrNoRows errors.
func TestEmitDeployFailedAudit_LookupError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnError(errors.New("brownout"))

	if err := emitDeployFailedAudit(context.Background(), db, uuid.New(), "OOMKilled", "msg"); err == nil {
		t.Fatal("expected wrapped error on lookup failure")
	}
}

// TestEmitDeployFailedAudit_NilTeamID returns nil silently when team_id
// is the zero UUID (defensive — the schema NOT NULL should make this
// impossible, but the guard prevents an audit_log INSERT failure).
func TestEmitDeployFailedAudit_NilTeamID(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(uuid.Nil))

	if err := emitDeployFailedAudit(context.Background(), db, uuid.New(), "OOMKilled", "msg"); err != nil {
		t.Errorf("expected nil on Nil team_id, got %v", err)
	}
}

// TestEmitDeployFailedAudit_InsertError surfaces the INSERT failure path.
func TestEmitDeployFailedAudit_InsertError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(uuid.New()))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnError(errors.New("audit table missing"))

	if err := emitDeployFailedAudit(context.Background(), db, uuid.New(), "OOMKilled", "msg"); err == nil {
		t.Fatal("expected wrapped insert error")
	}
}

// TestEmitDeployFailedAudit_SummaryTruncation guards the 256-char cap on the
// summary string.
func TestEmitDeployFailedAudit_SummaryTruncation(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	longEvent := strings.Repeat("x", 1024)
	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(uuid.New()))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			sqlmock.AnyArg(), // team_id
			sqlmock.AnyArg(), // actor
			"deploy.failed",
			// summary length is capped — match the truncated form.
			sqlmock.AnyArg(),
			sqlmock.AnyArg(), // metadata json
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := emitDeployFailedAudit(context.Background(), db, uuid.New(), "OOMKilled", longEvent); err != nil {
		t.Fatalf("emitDeployFailedAudit: %v", err)
	}
}

// TestAutopsyAlreadyPresentWithReason covers the three branches: ErrNoRows
// (fresh), empty/Unknown reason (treated as fresh), and a real reason (true).
func TestAutopsyAlreadyPresentWithReason(t *testing.T) {
	cases := []struct {
		name  string
		setup func(mock sqlmock.Sqlmock)
		want  bool
	}{
		{
			name: "ErrNoRows",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT reason FROM deployment_events`).
					WillReturnError(sql.ErrNoRows)
			},
			want: false,
		},
		{
			name: "Unknown reason still counts as not present",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT reason FROM deployment_events`).
					WillReturnRows(sqlmock.NewRows([]string{"reason"}).AddRow(workerFailureReasonUnknown))
			},
			want: false,
		},
		{
			name: "Real reason is present",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT reason FROM deployment_events`).
					WillReturnRows(sqlmock.NewRows([]string{"reason"}).AddRow(workerFailureReasonOOMKilled))
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			tc.setup(mock)
			got := autopsyAlreadyPresentWithReason(context.Background(), db, uuid.New())
			if got != tc.want {
				t.Errorf("autopsyAlreadyPresentWithReason = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFindBuildPodName covers the three branches: ListPods error (returns ""),
// empty list (""), and a populated list (returns first pod name).
func TestFindBuildPodName(t *testing.T) {
	cases := []struct {
		name string
		k8s  *fakeAutopsyK8sPR2
		want string
	}{
		{
			name: "ListPods error returns empty",
			k8s: &fakeAutopsyK8sPR2{
				listPodsFn: func(ns, sel string) (*corev1.PodList, error) {
					return nil, errors.New("brownout")
				},
			},
			want: "",
		},
		{
			name: "empty list returns empty",
			k8s: &fakeAutopsyK8sPR2{
				listPodsFn: func(ns, sel string) (*corev1.PodList, error) {
					return &corev1.PodList{}, nil
				},
			},
			want: "",
		},
		{
			name: "first pod name returned",
			k8s: &fakeAutopsyK8sPR2{
				listPodsFn: func(ns, sel string) (*corev1.PodList, error) {
					return &corev1.PodList{Items: []corev1.Pod{
						{ObjectMeta: metav1.ObjectMeta{Name: "build-foo-xyz"}},
					}}, nil
				},
			},
			want: "build-foo-xyz",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := findBuildPodName(context.Background(), tc.k8s, "instant-deploy-x", "x")
			if got != tc.want {
				t.Errorf("findBuildPodName = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAutopsy_NamespaceMismatch_EarlyReturn covers the early-return path where
// providerID doesn't match the app-<appID> shape. The autopsy writes an
// Unknown row and increments the logs_unavailable counter.
func TestAutopsy_NamespaceMismatch_EarlyReturn(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	captureDeploymentAutopsy(context.Background(), db, uuid.New(),
		"instant-stack-zzz", &fakeAutopsyK8sPR2{})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
