package jobs

// resourcestatus_conversion_test.go — proves the worker expiry-stage
// helpers converted to instant.dev/common/resourcestatus behave
// IDENTICALLY to the pre-conversion hand-written logic.
//
// This test lives in `package jobs` (not jobs_test) so it can reach the
// unexported selectStage / hoursLeft helpers directly.
//
// Each test re-implements the ORIGINAL algorithm inline and asserts the
// converted helper agrees for every input. If the shared package ever
// drifts from the worker's original semantics, these tests fail.

import (
	"testing"
	"time"

	"instant.dev/common/resourcestatus"
)

// legacyReminderStage / legacySchedule reproduce the pre-conversion
// hand-written 12h/6h/1h table that lived in expiry_reminder.go.
type legacyReminderStage struct {
	index         int
	expiresWithin time.Duration
}

var legacySchedule = []legacyReminderStage{
	{index: 1, expiresWithin: 12 * time.Hour},
	{index: 2, expiresWithin: 6 * time.Hour},
	{index: 3, expiresWithin: 1 * time.Hour},
}

// legacySelectStage is the ORIGINAL selectStage body, verbatim, used as
// the oracle the converted selectStage must match.
func legacySelectStage(r expiryReminderRow, now time.Time) (int, bool) {
	remaining := r.expiresAt.Sub(now)
	if remaining <= 0 {
		return 0, false
	}
	var bucketIndex int
	found := false
	for _, s := range legacySchedule {
		if remaining <= s.expiresWithin {
			bucketIndex = s.index
			found = true
		}
	}
	if !found {
		return 0, false
	}
	if r.remindersSent >= bucketIndex {
		return 0, false
	}
	return bucketIndex, true
}

// legacyHoursLeft is the ORIGINAL hoursLeft body, verbatim.
func legacyHoursLeft(expires, now time.Time) int {
	delta := expires.Sub(now)
	if delta <= time.Hour {
		return 1
	}
	hours := int(delta.Hours())
	if delta-time.Duration(hours)*time.Hour > 0 {
		hours++
	}
	if hours < 1 {
		hours = 1
	}
	return hours
}

// TestSelectStage_EquivalentToLegacy proves the converted selectStage
// (now delegating to resourcestatus.DeriveExpiryStage) returns the same
// (stage index, eligible) result as the original hand-written version,
// across every time-bucket boundary and every reminders_sent value.
func TestSelectStage_EquivalentToLegacy(t *testing.T) {
	now := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	offsets := []time.Duration{
		-time.Hour,              // past TTL
		0,                       // exactly now
		time.Nanosecond,         // 1ns out
		40 * time.Minute,        // inside 1h
		time.Hour,               // exactly 1h
		time.Hour + time.Minute, // just over 1h
		4 * time.Hour,           // inside 6h
		6 * time.Hour,           // exactly 6h
		8 * time.Hour,           // inside 12h
		12 * time.Hour,          // exactly 12h
		13 * time.Hour,          // beyond 12h
		24 * time.Hour,          // far out
	}
	for _, off := range offsets {
		for remindersSent := 0; remindersSent <= 4; remindersSent++ {
			r := expiryReminderRow{
				expiresAt:     now.Add(off),
				remindersSent: remindersSent,
			}
			wantIdx, wantOK := legacySelectStage(r, now)
			gotStage, gotOK := selectStage(r, now)
			if gotOK != wantOK {
				t.Errorf("offset=%v remindersSent=%d: ok=%v, legacy ok=%v",
					off, remindersSent, gotOK, wantOK)
				continue
			}
			if gotOK && gotStage.index != wantIdx {
				t.Errorf("offset=%v remindersSent=%d: index=%d, legacy index=%d",
					off, remindersSent, gotStage.index, wantIdx)
			}
			// Label and shared-enum agreement.
			if gotOK {
				es := resourcestatus.DeriveExpiryStage(r.expiresAt, now)
				if gotStage.label != es.Label() || gotStage.stage != es {
					t.Errorf("offset=%v: stage wrapper out of sync with shared enum", off)
				}
			}
		}
	}
}

// TestHoursLeft_EquivalentToLegacy proves the converted hoursLeft (now
// delegating to resourcestatus.HoursUntilExpiry) matches the original
// hand-written rounding for every future-expiry input.
func TestHoursLeft_EquivalentToLegacy(t *testing.T) {
	now := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	offsets := []time.Duration{
		time.Nanosecond,
		30 * time.Minute,
		time.Hour,
		time.Hour + time.Minute,
		2 * time.Hour,
		10 * time.Hour,
		10*time.Hour + 30*time.Minute,
		23 * time.Hour,
	}
	for _, off := range offsets {
		expires := now.Add(off)
		want := legacyHoursLeft(expires, now)
		got := hoursLeft(expires, now)
		if got != want {
			t.Errorf("offset=%v: hoursLeft=%d, legacy=%d", off, got, want)
		}
	}
}
