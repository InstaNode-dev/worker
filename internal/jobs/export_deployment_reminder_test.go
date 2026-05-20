package jobs

// export_deployment_reminder_test.go — test-only exports for
// deployment_reminder.go internals. Lets the jobs_test external test
// package assert the F3 escalating-cadence shape without making the
// constants part of the public API.

import "time"

// DeployReminderStageThresholds is a test-only view of the per-stage
// time-to-expiry thresholds that gate the escalating cadence.
// Index = reminders_sent (0/1/2 → stage 1/2/3).
func DeployReminderStageThresholds() []time.Duration {
	out := make([]time.Duration, 0, len(deployReminderStageThresholds))
	out = append(out, deployReminderStageThresholds[:]...)
	return out
}

// NextReminderThreshold exports nextReminderThreshold for stage-progression tests.
func NextReminderThreshold(remindersSent int) time.Duration {
	return nextReminderThreshold(remindersSent)
}

// MaxDeployReminders exports maxDeployReminders so the pinning test can
// assert the slice length matches without re-declaring the constant.
const MaxDeployReminders = maxDeployReminders
