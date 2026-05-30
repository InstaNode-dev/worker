FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY proto/ /proto/
COPY common/ /common/
COPY worker/go.mod worker/go.sum ./
RUN go mod download
COPY worker/ .
# Build-time metadata injected via -ldflags into instant.dev/common/buildinfo.
# Defaults keep the build runnable without --build-arg; CI passes real values.
ARG GIT_SHA=dev
ARG BUILD_TIME=unknown
ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -ldflags "-X instant.dev/common/buildinfo.GitSHA=${GIT_SHA} -X instant.dev/common/buildinfo.BuildTime=${BUILD_TIME} -X instant.dev/common/buildinfo.Version=${VERSION}" \
    -o /worker .

# Runtime: postgres:16-alpine ships pg_dump matching the customer-pg server
# version (infra/k8s/postgres-customers.yaml uses postgres:16-alpine). The
# previous distroless/static-debian12 base had NO shell, NO package manager,
# and NO pg_dump — which silently broke customer_backup_runner.go's
# `exec.CommandContext(ctx, "pg_dump", ...)` for every Pro+ tier customer's
# scheduled backup (data-loss risk: P0 incident 2026-05-30).
#
# Our worker binary is built with CGO_ENABLED=0 so it's fully static and
# runs unmodified on alpine. Alpine also gives us sh+wget+curl for the
# in-pod /healthz SHA check at the end of deploy.yml (was a warning-only
# fallback on distroless; now a hard gate).
#
# When the customer-pg image bumps to postgres:17-alpine, bump this tag in
# the same PR — pg_dump major version must be >= server major version, and
# matching exactly keeps the dump format ABI predictable.
FROM postgres:16-alpine
COPY --from=builder /worker /worker
ENTRYPOINT ["/worker"]
