FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY proto/ /proto/
COPY common/ /common/
COPY worker/go.mod worker/go.sum ./
RUN go mod download
COPY worker/ .
RUN CGO_ENABLED=0 go build -o /worker .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /worker /worker
ENTRYPOINT ["/worker"]
