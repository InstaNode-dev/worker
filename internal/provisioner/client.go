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
