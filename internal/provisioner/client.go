package provisioner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"
)

// Client wraps the gRPC ProvisionerServiceClient with convenience methods.
type Client struct {
	grpc   provisionerv1.ProvisionerServiceClient
	secret string
}

// NewClient dials the provisioner gRPC server and returns a Client.
// The caller is responsible for calling conn.Close() on shutdown.
func NewClient(addr, secret string) (*Client, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("provisioner.NewClient: %w", err)
	}
	return &Client{
		grpc:   provisionerv1.NewProvisionerServiceClient(conn),
		secret: secret,
	}, conn, nil
}

func (c *Client) ctxWithAuth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-instant-provisioner-token", c.secret)
}

// StorageBytes fetches current storage usage for a resource.
func (c *Client) StorageBytes(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType) (int64, error) {
	ctx, cancel := context.WithTimeout(c.ctxWithAuth(ctx), 30*time.Second)
	defer cancel()
	resp, err := c.grpc.GetStorageBytes(ctx, &provisionerv1.StorageRequest{
		Token:              token,
		ProviderResourceId: providerResourceID,
		ResourceType:       resType,
	})
	if err != nil {
		return 0, fmt.Errorf("provisioner.StorageBytes: %w", err)
	}
	return resp.StorageBytes, nil
}

// RegradeResult is the worker-facing projection of provisionerv1.RegradeResponse.
type RegradeResult struct {
	Applied          bool
	AppliedConnLimit int32
	SkipReason       string
}

// RegradeResource re-applies the entitled connection cap for a resource at the
// given tier. Used by the entitlement reconciler to fix "upgrade drift" — a
// resource whose tier was bumped on plan upgrade but whose actual Postgres
// connection cap was never re-applied to the database.
//
// requestID is an idempotency token the provisioner echoes/uses for dedup.
func (c *Client) RegradeResource(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType, tier, requestID string) (RegradeResult, error) {
	ctx, cancel := context.WithTimeout(c.ctxWithAuth(ctx), 30*time.Second)
	defer cancel()
	resp, err := c.grpc.RegradeResource(ctx, &provisionerv1.RegradeRequest{
		Token:              token,
		ProviderResourceId: providerResourceID,
		ResourceType:       resType,
		Tier:               tier,
		RequestId:          requestID,
	})
	if err != nil {
		return RegradeResult{}, fmt.Errorf("provisioner.RegradeResource: %w", err)
	}
	return RegradeResult{
		Applied:          resp.Applied,
		AppliedConnLimit: resp.AppliedConnLimit,
		SkipReason:       resp.SkipReason,
	}, nil
}

// DeprovisionResource removes a provisioned resource.
func (c *Client) DeprovisionResource(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType) error {
	ctx, cancel := context.WithTimeout(c.ctxWithAuth(ctx), 30*time.Second)
	defer cancel()
	_, err := c.grpc.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:              token,
		ProviderResourceId: providerResourceID,
		ResourceType:       resType,
	})
	if err != nil {
		slog.Error("provisioner.DeprovisionResource failed", "error", err, "token", token)
		return fmt.Errorf("provisioner.DeprovisionResource: %w", err)
	}
	return nil
}
