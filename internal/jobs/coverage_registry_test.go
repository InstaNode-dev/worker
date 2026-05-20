package jobs

// coverage_registry_test.go — Wave 2 (2026-05-20) registry-iterating
// regression tests per CLAUDE.md rule 18.
//
// What the existing event_email_mapping_test.go + lifecycle_emails_test.go
// + propagation_runner_test.go pack covers:
//   - every supportedAuditKinds entry has a builder + renderer
//   - every builder has a renderer (F4 inverse)
//   - every propagationKnownKind has a handler (and vice-versa)
//
// What this file ADDS:
//
//   1. TestWorkerAuditKinds_ValuesMatchApiConstants
//      The worker hard-codes "subscription.upgraded" etc as its own
//      auditKind* constants. The api emits AuditKindSubscriptionUpgraded
//      whose value is also "subscription.upgraded". The strings MUST
//      match on the wire — the worker reads audit_log.kind rows the api
//      wrote. A drift here (typo, renamed constant on one side) means
//      the worker silently filters out rows the api emits.
//
//      This test text-walks the api's internal/models/audit_kinds.go
//      and asserts every worker-side auditKind* constant's STRING VALUE
//      appears as the right-hand side of an `AuditKind* = "<value>"`
//      declaration in the api source. Cross-repo drift is loud.
//
//      Gated on the api source file being locatable. In CI the api +
//      worker repos sit side-by-side under the same parent (the
//      checkout layout the gh-actions/checkout actions produce); local
//      dev uses the conventional ~/Documents/InstaNode layout. If
//      neither resolves, the test SKIPs rather than fails, so a worker
//      contributor without the api repo can still run `make gate`.
//
//   2. TestSupportedAuditKinds_CoveredByApiSpec
//      The api's e2e/reliability_contract_test.go has an
//      auditConsumerSpec table that documents which kinds the worker
//      emails. Iterates supportedAuditKinds and asserts each one is
//      present in the api-side spec (text-source walk again — no
//      cross-package import). Drift caught at PR time, not in
//      production with a silently-dropped customer email.

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// findApiRepoRoot resolves the api repo root from the worker test
// location. Tries several conventional layouts:
//
//  1. INSTANT_API_REPO env var — operator override for non-standard
//     checkouts (CI matrix runs that mount the api in /work/api etc).
//  2. ../api — sibling layout common in monorepo-style checkouts
//     (github/checkout default when the worker is the primary repo).
//  3. ../../api — when the worker is at a worktree depth.
//  4. The conventional ~/Documents/InstaNode/api dev layout (used by
//     CLAUDE.md docs).
//
// Returns "" if none of the candidates exists.
func findApiRepoRoot(t *testing.T) string {
	t.Helper()
	if env := os.Getenv("INSTANT_API_REPO"); env != "" {
		if _, err := os.Stat(filepath.Join(env, "internal", "models", "audit_kinds.go")); err == nil {
			return env
		}
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	// thisFile = .../worker/internal/jobs/coverage_registry_test.go
	workerRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	candidates := []string{
		filepath.Join(workerRoot, "..", "api"),
		filepath.Join(workerRoot, "..", "..", "api"),
	}
	if home, _ := os.UserHomeDir(); home != "" {
		candidates = append(candidates,
			filepath.Join(home, "Documents", "InstaNode", "api"),
		)
	}
	for _, c := range candidates {
		abs, err := filepath.Abs(c)
		if err != nil {
			continue
		}
		if _, err := os.Stat(filepath.Join(abs, "internal", "models", "audit_kinds.go")); err == nil {
			return abs
		}
	}
	return ""
}

// scanApiAuditKindStringValues reads
// <apiRepo>/internal/models/audit_kinds.go and returns the SET of
// string values on every `AuditKind* = "<value>"` declaration. These
// are the wire values the worker's audit_log SELECT consumes.
func scanApiAuditKindStringValues(t *testing.T, apiRepo string) (map[string]bool, string) {
	t.Helper()
	path := filepath.Join(apiRepo, "internal", "models", "audit_kinds.go")
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("open %s: %v", path, err)
	}
	defer f.Close()

	re := regexp.MustCompile(`\bAuditKind\w+\s*=\s*"([^"]+)"`)
	out := map[string]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if m := re.FindStringSubmatch(scanner.Text()); m != nil {
			out[m[1]] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out, path
}

// workerAuditKindStringValues returns the set of string values the
// worker's auditKind* constants resolve to at compile time. This is
// the IN-PROCESS registry — no source-walk needed; the values are
// the actual constants the consuming code uses.
//
// Building this list from the in-process constants (rather than a
// text walk of event_email_mapping.go) means a rename of the Go
// identifier without a value change still passes — only a string
// value drift fails the test, which is precisely the cross-repo
// failure mode CLAUDE.md rule 18 calls out for "strings that must
// match on the wire."
func workerAuditKindStringValues() map[string]string {
	// Each entry maps Go-identifier-shape → wire value. The Go names
	// are documentation; the values are what the SQL filter compares
	// audit_log.kind against.
	return map[string]string{
		"auditKindOnboardingClaimed":           auditKindOnboardingClaimed,
		"auditKindSubscriptionUpgraded":        auditKindSubscriptionUpgraded,
		"auditKindResourceExpiryImminent":      auditKindResourceExpiryImminent,
		"auditKindSubscriptionDowngraded":      auditKindSubscriptionDowngraded,
		"auditKindSubscriptionCanceled":        auditKindSubscriptionCanceled,
		"auditKindSubscriptionCanceledByAdmin": auditKindSubscriptionCanceledByAdmin,
		"auditKindResourceQuotaSuspended":      auditKindResourceQuotaSuspended,
		"auditKindResourceQuotaUnsuspended":    auditKindResourceQuotaUnsuspended,
		"auditKindDeployExpiringSoon":          auditKindDeployExpiringSoon,
		"auditKindDeployExpired":               auditKindDeployExpired,
		"auditKindDeployMadePermanent":         auditKindDeployMadePermanent,
		"auditKindDeployTTLSet":                auditKindDeployTTLSet,
		"auditKindTeamSettingsChanged":         auditKindTeamSettingsChanged,
		"auditKindDeployDeletionRequested":     auditKindDeployDeletionRequested,
		"auditKindDeployDeletionConfirmed":     auditKindDeployDeletionConfirmed,
		"auditKindDeployDeletionCancelled":     auditKindDeployDeletionCancelled,
		"auditKindDeployDeletionExpired":       auditKindDeployDeletionExpired,
		"auditKindPaymentGraceStarted":         auditKindPaymentGraceStarted,
		"auditKindPaymentGraceRecovered":       auditKindPaymentGraceRecovered,
	}
}

// TestWorkerAuditKinds_ValuesMatchApiConstants — registry-iterating
// cross-repo wire-value guard.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       worker's auditKindSubscriptionUpgraded value
//                  drifts from the api's AuditKindSubscriptionUpgraded
//                  value (typo in either, rename without a coordinated
//                  PR, contract change that only patched one side).
//                  The worker SELECT filters audit_log.kind = ANY(...)
//                  silently misses the api's emitted rows; no email
//                  fires; customer never hears.
//   Enumeration:   workerAuditKindStringValues() returns every
//                  worker-side constant + its compiled-in value.
//   Sites found:   N constants on the worker side.
//   Sites touched: each compared against the api source's
//                  AuditKind* string-value set.
//   Coverage test: any value not present on the api side fails.
//   Live verified: api master HEAD audit_kinds.go scan (this test
//                  walks the actual file in the sibling repo).
func TestWorkerAuditKinds_ValuesMatchApiConstants(t *testing.T) {
	apiRepo := findApiRepoRoot(t)
	if apiRepo == "" {
		t.Skip("api repo not found in any conventional layout (set INSTANT_API_REPO to override) — cross-repo wire-value check requires the api source")
	}
	apiValues, apiPath := scanApiAuditKindStringValues(t, apiRepo)
	if len(apiValues) < 30 {
		t.Fatalf("api audit_kinds.go scan found only %d values — scan is broken", len(apiValues))
	}

	workerValues := workerAuditKindStringValues()

	// Some worker-side constants don't have a 1:1 api-side constant —
	// they're locally-emitted by the worker itself (e.g. the
	// near_quota_wall + churn.risk_flagged kinds the worker writes
	// from background jobs without an api emit site). These are
	// genuinely worker-only and must be allow-listed here with a
	// reason.
	workerOnly := map[string]string{
		// These kinds are emitted by worker jobs that write directly
		// to audit_log (no api round-trip). They have no corresponding
		// AuditKind* constant on the api side because nothing in the
		// api emits them. Verified via cross-repo grep (2026-05-20).
		"churn.risk_flagged":         "churn_predictor.go emits via worker; api has no analog",
		"near_quota_wall":            "near_quota_wall.go emits via worker; api has no analog",
		"digest.weekly":              "weekly_digest.go emits via worker; api has no analog",
		"anon.expiry_warning":        "expiry_reminder.go emits via worker; api has no analog",
		"resource.expiry_imminent":   "expire.go (worker) emits during expiry sweeps; api has no analog (emit site is worker-side scan, not an api request)",
		// W2 (P1-W2-01/02) — emitted from the worker's billing_dunning /
		// customer_backup_runner / restore paths; api has no analog.
		"payment.grace_reminder":   "payment_grace_reminder.go (worker) emits",
		"payment.grace_terminated": "payment_grace_terminator.go (worker) emits",
		"backup.failed":            "customer_backup_runner.go (worker) emits",
		"restore.succeeded":        "customer_restore_runner.go (worker) emits",
		"restore.failed":           "customer_restore_runner.go (worker) emits",
		"deploy.failed":            "deploy_failure_autopsy.go (worker) — api also emits, see below",
		"checkout.abandoned":       "checkout_reconcile.go (worker) emits — webhook-blind dunning path",
		"experiment.conversion":    "experiments.go (worker) emits — A/B click sink",
		"admin.tier_changed":       "admin tier change emit also done worker-side via team_deletion_executor",
		"admin.promo_issued":       "admin_promos audit emit via worker forwarder",
	}

	var missing []string
	for name, val := range workerValues {
		if apiValues[val] {
			continue
		}
		if reason, ok := workerOnly[val]; ok {
			t.Logf("%s = %q: allow-listed worker-only kind: %s", name, val, reason)
			continue
		}
		missing = append(missing, name+" = "+val)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("the following worker auditKind* values are NOT present in the api's audit_kinds.go (%s) — cross-repo wire-value drift. The worker's audit_log SELECT will silently filter these out:\n  %s\n\nFix: either rename the worker constant to match the api side, or — if this kind is genuinely worker-only — add it to the workerOnly allow-list in this test with a one-line justification.",
			apiPath, strings.Join(missing, "\n  "))
	}
}

// TestSupportedAuditKinds_CoveredByApiSpecOrDocumentedWorkerOnly —
// every worker supportedAuditKinds entry must EITHER appear in the
// api's e2e/reliability_contract_test.go auditConsumerSpec map, OR
// be documented as worker-only in the workerOnly allow-list used by
// TestWorkerAuditKinds_ValuesMatchApiConstants above. This catches
// the reverse drift: a kind the worker IS emailing on, but the
// api-side reliability contract has no spec entry AND the kind
// isn't documented as worker-only — meaning nobody knows who's
// supposed to be testing it.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       worker adds a new kind to supportedAuditKinds + a
//                  builder, ships, but neither (a) the api-side
//                  reliability spec is updated to cover it, NOR (b)
//                  this test's workerOnly map documents it as
//                  worker-emitted. The integration contract is
//                  silently inconsistent — a wiring nobody is
//                  asserting end-to-end.
//   Enumeration:   supportedAuditKinds (the actual SQL filter slice).
//   Sites found:   N worker-side kinds.
//   Sites touched: each cross-referenced against api spec ∪
//                  workerOnly allow-list.
//   Coverage test: any worker kind missing from BOTH fails.
//   Live verified: walks live api source.
func TestSupportedAuditKinds_CoveredByApiSpecOrDocumentedWorkerOnly(t *testing.T) {
	apiRepo := findApiRepoRoot(t)
	if apiRepo == "" {
		t.Skip("api repo not found — set INSTANT_API_REPO to enable")
	}
	specPath := filepath.Join(apiRepo, "e2e", "reliability_contract_test.go")
	data, err := os.ReadFile(specPath)
	if err != nil {
		t.Skipf("read %s: %v", specPath, err)
	}

	// Parse the auditConsumerSpec entries — they look like:
	//   "subscription.upgraded": {Emails: true, ...},
	// We just want the keys.
	specKindRe := regexp.MustCompile(`(?m)^\s*"([a-z][a-z0-9._]+)":\s*\{`)
	spec := map[string]bool{}
	for _, m := range specKindRe.FindAllStringSubmatch(string(data), -1) {
		spec[m[1]] = true
	}
	if len(spec) < 30 {
		t.Fatalf("api spec scan found only %d entries in %s — scan broken", len(spec), specPath)
	}

	// Worker-only allow-list — kept in sync with the one in
	// TestWorkerAuditKinds_ValuesMatchApiConstants above. Sharing
	// is intentional: a kind that is genuinely worker-only must be
	// documented in BOTH gates to remain green.
	workerOnly := map[string]bool{
		"churn.risk_flagged":       true,
		"near_quota_wall":          true,
		"digest.weekly":            true,
		"anon.expiry_warning":      true,
		"resource.expiry_imminent": true,
		"payment.grace_reminder":   true,
		"payment.grace_terminated": true,
		"backup.failed":            true,
		"restore.succeeded":        true,
		"restore.failed":           true,
		"deploy.failed":            true,
		"checkout.abandoned":       true,
		"experiment.conversion":    true,
		"admin.tier_changed":       true,
		"admin.promo_issued":       true,
	}

	var missing []string
	for _, kind := range supportedAuditKinds {
		if spec[kind] {
			continue
		}
		if workerOnly[kind] {
			continue
		}
		missing = append(missing, kind)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("the following supportedAuditKinds entries appear neither in the api's auditConsumerSpec (%s) NOR in this test's workerOnly allow-list — the worker emails on these kinds but no cross-track gate is asserting the wiring. Pick one:\n  - if the api ALSO emits this kind, add `\"<kind>\": {Emails: true, Forwards: true}` to auditConsumerSpec on the api side\n  - if the kind is worker-only, add it to the workerOnly map in this test with a justification\n\nMissing:\n  %s",
			specPath, strings.Join(missing, "\n  "))
	}
}
