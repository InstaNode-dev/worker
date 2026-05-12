FROM golang:1.24-alpine AS builder
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

FROM gcr.io/distroless/static-debian12
COPY --from=builder /worker /worker
ENTRYPOINT ["/worker"]
