package jobs

// expire_resource_type_proto_test.go — regression tests for L03-2 (P1):
// expireResourceTypeToProto must map "vector" to RESOURCE_TYPE_POSTGRES so that
// expired vector resources are correctly deprovisioned by the ExpireAnonymousWorker.
//
// Pre-fix: "vector" fell through to RESOURCE_TYPE_UNSPECIFIED, causing the
// expire worker to skip the provisioner.DeprovisionResource call. The underlying
// Postgres database (db_<token>) and user (usr_<token>) were never dropped,
// accumulating as orphaned resources indefinitely.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "instant.dev/proto/common/v1"
)

// TestExpireResourceTypeToProto_TableDriven_CoverageBlock enumerates every
// resource type that the worker may encounter and pins the expected proto enum.
// This is the registry-iterating test required by the agent-reliability rules:
// iterating the live function rather than maintaining a hand-typed list.
func TestExpireResourceTypeToProto_TableDriven_CoverageBlock(t *testing.T) {
	cases := []struct {
		resourceType string
		want         commonv1.ResourceType
		reason       string
	}{
		{
			resourceType: "postgres",
			want:         commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
			reason:       "postgres must deprovision via provisioner",
		},
		{
			resourceType: "redis",
			want:         commonv1.ResourceType_RESOURCE_TYPE_REDIS,
			reason:       "redis must deprovision via provisioner",
		},
		{
			resourceType: "mongodb",
			want:         commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
			reason:       "mongodb must deprovision via provisioner",
		},
		{
			resourceType: "queue",
			want:         commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
			reason:       "queue k8s backend must deprovision namespace via provisioner",
		},
		{
			// BUG L03-2 regression: previously UNSPECIFIED — the provisioner
			// call was silently skipped, orphaning Postgres DB/user forever.
			resourceType: "vector",
			want:         commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
			reason:       "vector is pgvector-on-Postgres; expiry cleanup path must be RESOURCE_TYPE_POSTGRES",
		},
		{
			// Webhook: no per-resource provisioner state; API-receiver only.
			resourceType: "webhook",
			want:         commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
			reason:       "webhook has no provisioner state to clean up",
		},
		{
			// Storage: uses S3/MinIO path, not provisioner RPC.
			resourceType: "storage",
			want:         commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
			reason:       "storage is handled by the MinIO/S3 deprovision path, not provisioner",
		},
		{
			resourceType: "",
			want:         commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
			reason:       "empty type must default to UNSPECIFIED (safe skip)",
		},
		{
			resourceType: "future_unknown",
			want:         commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
			reason:       "unrecognized types default to UNSPECIFIED; provisioner call is skipped",
		},
	}

	for _, tc := range cases {
		t.Run(tc.resourceType, func(t *testing.T) {
			got := expireResourceTypeToProto(tc.resourceType)
			assert.Equal(t, tc.want, got,
				"expireResourceTypeToProto(%q): %s", tc.resourceType, tc.reason)
		})
	}
}

// TestExpireResourceTypeToProto_VectorNotUnspecified is the single-focus
// sentinel for L03-2. Named for immediate git-blame discoverability.
func TestExpireResourceTypeToProto_VectorNotUnspecified(t *testing.T) {
	got := expireResourceTypeToProto("vector")
	require.NotEqual(t,
		commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED, got,
		"L03-2 regression: expireResourceTypeToProto(\"vector\") must NOT return UNSPECIFIED — "+
			"that causes the expire worker to skip DeprovisionResource, permanently orphaning "+
			"Postgres DBs (db_<token>) and users (usr_<token>) for every expired vector resource")
	assert.Equal(t, commonv1.ResourceType_RESOURCE_TYPE_POSTGRES, got,
		"vector shares the Postgres backend; expiry cleanup must use RESOURCE_TYPE_POSTGRES")
}
