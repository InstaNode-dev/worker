package provisioner

// client_test.go — coverage for the worker→provisioner gRPC client.
//
// Uses an in-process bufconn listener + a fake ProvisionerServiceServer
// so the tests never need a real gRPC dial-target. Each test exercises
// one Client method end-to-end and asserts:
//   - the request payload reaches the server with the expected fields
//   - the auth metadata header is attached
//   - errors from the server are wrapped with the method-name prefix

import (
	"context"
	"errors"
	"net"
	"testing"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// fakeProvisionerServer is the in-process gRPC server stand-in. Each
// method records the last incoming request + auth header and returns a
// canned response (or canned error). New methods get added here, NOT
// re-mocked per test.
type fakeProvisionerServer struct {
	provisionerv1.UnimplementedProvisionerServiceServer

	// Recorded inputs (last call wins).
	storageReq      *provisionerv1.StorageRequest
	storageAuth     string
	regradeReq      *provisionerv1.RegradeRequest
	regradeAuth     string
	deprovisionReq  *provisionerv1.DeprovisionRequest
	deprovisionAuth string

	// Canned responses / errors.
	storageResp     *provisionerv1.StorageResponse
	storageErr      error
	regradeResp     *provisionerv1.RegradeResponse
	regradeErr      error
	deprovisionResp *provisionerv1.DeprovisionResponse
	deprovisionErr  error
}

func (f *fakeProvisionerServer) GetStorageBytes(ctx context.Context, req *provisionerv1.StorageRequest) (*provisionerv1.StorageResponse, error) {
	f.storageReq = req
	f.storageAuth = firstMetaValue(ctx, "x-instant-provisioner-token")
	if f.storageErr != nil {
		return nil, f.storageErr
	}
	if f.storageResp == nil {
		return &provisionerv1.StorageResponse{StorageBytes: 0}, nil
	}
	return f.storageResp, nil
}

func (f *fakeProvisionerServer) RegradeResource(ctx context.Context, req *provisionerv1.RegradeRequest) (*provisionerv1.RegradeResponse, error) {
	f.regradeReq = req
	f.regradeAuth = firstMetaValue(ctx, "x-instant-provisioner-token")
	if f.regradeErr != nil {
		return nil, f.regradeErr
	}
	if f.regradeResp == nil {
		return &provisionerv1.RegradeResponse{Applied: true, AppliedConnLimit: 8}, nil
	}
	return f.regradeResp, nil
}

func (f *fakeProvisionerServer) DeprovisionResource(ctx context.Context, req *provisionerv1.DeprovisionRequest) (*provisionerv1.DeprovisionResponse, error) {
	f.deprovisionReq = req
	f.deprovisionAuth = firstMetaValue(ctx, "x-instant-provisioner-token")
	if f.deprovisionErr != nil {
		return nil, f.deprovisionErr
	}
	if f.deprovisionResp == nil {
		return &provisionerv1.DeprovisionResponse{}, nil
	}
	return f.deprovisionResp, nil
}

// firstMetaValue returns the first value of the given metadata key in
// the incoming context, or "" if absent.
func firstMetaValue(ctx context.Context, key string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// dialBufconn boots an in-process gRPC server backed by fake and returns
// a Client wired to it. The cleanup function tears down the listener
// + server + connection so a single failure can't leak goroutines.
func dialBufconn(t *testing.T, fake *fakeProvisionerServer, secret string) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	provisionerv1.RegisterProvisionerServiceServer(srv, fake)
	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough://bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	c := &Client{
		grpc:   provisionerv1.NewProvisionerServiceClient(conn),
		secret: secret,
	}
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
	return c, cleanup
}

// TestNewClient_OK — the constructor must produce a usable *Client +
// *grpc.ClientConn pair for any plausible address. The dial is lazy in
// grpc.NewClient so unreachable hosts still succeed at construction; the
// connection error only surfaces on first RPC. We exercise that path
// downstream — here we just prove the constructor itself returns a
// non-nil triple and that conn.Close() works.
func TestNewClient_OK(t *testing.T) {
	c, conn, err := NewClient("127.0.0.1:1", "secret-test")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c == nil || conn == nil {
		t.Fatal("NewClient returned nil Client or conn")
	}
	if err := conn.Close(); err != nil {
		t.Errorf("conn.Close: %v", err)
	}
}

// TestStorageBytes_SuccessAndAuth — happy path: returns the upstream
// StorageBytes value and the request carries the auth metadata.
func TestStorageBytes_SuccessAndAuth(t *testing.T) {
	fake := &fakeProvisionerServer{
		storageResp: &provisionerv1.StorageResponse{StorageBytes: 1024 * 1024},
	}
	c, cleanup := dialBufconn(t, fake, "tok-storage")
	defer cleanup()

	got, err := c.StorageBytes(context.Background(), "inst_token_1", "pg-1", commonv1.ResourceType_RESOURCE_TYPE_POSTGRES)
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got != 1024*1024 {
		t.Errorf("StorageBytes = %d; want %d", got, 1024*1024)
	}
	if fake.storageReq.Token != "inst_token_1" {
		t.Errorf("server saw Token=%q; want inst_token_1", fake.storageReq.Token)
	}
	if fake.storageReq.ProviderResourceId != "pg-1" {
		t.Errorf("server saw ProviderResourceId=%q; want pg-1", fake.storageReq.ProviderResourceId)
	}
	if fake.storageReq.ResourceType != commonv1.ResourceType_RESOURCE_TYPE_POSTGRES {
		t.Errorf("server saw ResourceType=%v; want POSTGRES", fake.storageReq.ResourceType)
	}
	if fake.storageAuth != "tok-storage" {
		t.Errorf("auth header = %q; want tok-storage (ctxWithAuth must attach x-instant-provisioner-token)", fake.storageAuth)
	}
}

// TestStorageBytes_ServerError_IsWrapped — upstream error is wrapped
// with the method-name prefix so an operator grepping logs sees
// "provisioner.StorageBytes:" before the underlying gRPC code.
func TestStorageBytes_ServerError_IsWrapped(t *testing.T) {
	fake := &fakeProvisionerServer{storageErr: errors.New("upstream-down")}
	c, cleanup := dialBufconn(t, fake, "s")
	defer cleanup()

	_, err := c.StorageBytes(context.Background(), "t", "r", commonv1.ResourceType_RESOURCE_TYPE_POSTGRES)
	if err == nil {
		t.Fatal("StorageBytes on server error = nil; want wrapped error")
	}
	if !contains(err.Error(), "provisioner.StorageBytes") {
		t.Errorf("error not wrapped with method name: %v", err)
	}
}

// TestRegradeResource_SuccessShape — happy path: every projection field
// (Applied, AppliedConnLimit, SkipReason) reaches the caller correctly,
// and the request carries Tier + RequestId so the provisioner can dedupe.
func TestRegradeResource_SuccessShape(t *testing.T) {
	fake := &fakeProvisionerServer{
		regradeResp: &provisionerv1.RegradeResponse{
			Applied:          true,
			AppliedConnLimit: 16,
			SkipReason:       "",
		},
	}
	c, cleanup := dialBufconn(t, fake, "tok-regrade")
	defer cleanup()

	res, err := c.RegradeResource(
		context.Background(),
		"inst_live_xyz",
		"pg-7",
		commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		"pro",
		"req-abc",
	)
	if err != nil {
		t.Fatalf("RegradeResource: %v", err)
	}
	if !res.Applied {
		t.Errorf("Applied = false; want true")
	}
	if res.AppliedConnLimit != 16 {
		t.Errorf("AppliedConnLimit = %d; want 16", res.AppliedConnLimit)
	}
	if res.SkipReason != "" {
		t.Errorf("SkipReason = %q; want empty", res.SkipReason)
	}
	if fake.regradeReq.Tier != "pro" {
		t.Errorf("server saw Tier=%q; want pro", fake.regradeReq.Tier)
	}
	if fake.regradeReq.RequestId != "req-abc" {
		t.Errorf("server saw RequestId=%q; want req-abc — idempotency key was dropped", fake.regradeReq.RequestId)
	}
	if fake.regradeAuth != "tok-regrade" {
		t.Errorf("regrade auth = %q; want tok-regrade", fake.regradeAuth)
	}
}

// TestRegradeResource_SkipReason — the SkipReason flows through.
// Operators rely on this to distinguish "regrade not needed" from a
// genuine apply at the dashboard.
func TestRegradeResource_SkipReason(t *testing.T) {
	fake := &fakeProvisionerServer{
		regradeResp: &provisionerv1.RegradeResponse{
			Applied:    false,
			SkipReason: "already_at_tier",
		},
	}
	c, cleanup := dialBufconn(t, fake, "s")
	defer cleanup()

	res, err := c.RegradeResource(context.Background(), "t", "r", commonv1.ResourceType_RESOURCE_TYPE_REDIS, "pro", "req-1")
	if err != nil {
		t.Fatalf("RegradeResource: %v", err)
	}
	if res.SkipReason != "already_at_tier" {
		t.Errorf("SkipReason = %q; want already_at_tier", res.SkipReason)
	}
	if res.Applied {
		t.Errorf("Applied = true; want false (we skipped)")
	}
}

// TestRegradeResource_ServerError_IsWrapped — upstream error wrapped.
func TestRegradeResource_ServerError_IsWrapped(t *testing.T) {
	fake := &fakeProvisionerServer{regradeErr: errors.New("conn-refused")}
	c, cleanup := dialBufconn(t, fake, "s")
	defer cleanup()

	_, err := c.RegradeResource(context.Background(), "t", "r", commonv1.ResourceType_RESOURCE_TYPE_REDIS, "pro", "req")
	if err == nil {
		t.Fatal("RegradeResource on server error = nil; want wrapped error")
	}
	if !contains(err.Error(), "provisioner.RegradeResource") {
		t.Errorf("error not wrapped with method name: %v", err)
	}
}

// TestDeprovisionResource_Success — happy path: server invoked with the
// right fields, no error returned.
func TestDeprovisionResource_Success(t *testing.T) {
	fake := &fakeProvisionerServer{}
	c, cleanup := dialBufconn(t, fake, "tok-deprov")
	defer cleanup()

	if err := c.DeprovisionResource(context.Background(), "inst_t", "redis-3", commonv1.ResourceType_RESOURCE_TYPE_REDIS); err != nil {
		t.Fatalf("DeprovisionResource: %v", err)
	}
	if fake.deprovisionReq == nil {
		t.Fatal("server did not see a DeprovisionRequest")
	}
	if fake.deprovisionReq.Token != "inst_t" || fake.deprovisionReq.ProviderResourceId != "redis-3" {
		t.Errorf("server saw req=%+v; want Token=inst_t ProviderResourceId=redis-3", fake.deprovisionReq)
	}
	if fake.deprovisionReq.ResourceType != commonv1.ResourceType_RESOURCE_TYPE_REDIS {
		t.Errorf("ResourceType = %v; want REDIS", fake.deprovisionReq.ResourceType)
	}
	if fake.deprovisionAuth != "tok-deprov" {
		t.Errorf("deprovision auth = %q; want tok-deprov", fake.deprovisionAuth)
	}
}

// TestDeprovisionResource_ServerError_IsWrapped — failure is wrapped
// AND logged at ERROR (rare for the client wrapper to log on its own;
// the destructor path is one of the few places we want a loud log even
// before the caller decides what to do).
func TestDeprovisionResource_ServerError_IsWrapped(t *testing.T) {
	fake := &fakeProvisionerServer{deprovisionErr: errors.New("backend-broke")}
	c, cleanup := dialBufconn(t, fake, "s")
	defer cleanup()

	err := c.DeprovisionResource(context.Background(), "tok-123", "r-1", commonv1.ResourceType_RESOURCE_TYPE_POSTGRES)
	if err == nil {
		t.Fatal("DeprovisionResource on server error = nil; want wrapped error")
	}
	if !contains(err.Error(), "provisioner.DeprovisionResource") {
		t.Errorf("error not wrapped: %v", err)
	}
}

// TestCtxWithAuth_AttachesHeader — ctxWithAuth is the centralised auth
// path. A regression that drops the metadata key would leak unauthenticated
// requests to the provisioner; this pins the contract.
func TestCtxWithAuth_AttachesHeader(t *testing.T) {
	c := &Client{secret: "secret-token-value"}
	ctx := c.ctxWithAuth(context.Background())
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("ctxWithAuth did not attach outgoing metadata")
	}
	vals := md.Get("x-instant-provisioner-token")
	if len(vals) != 1 || vals[0] != "secret-token-value" {
		t.Errorf("metadata key x-instant-provisioner-token = %v; want [secret-token-value]", vals)
	}
}

// contains is a tiny substring helper so this file doesn't import strings
// for one match.
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
