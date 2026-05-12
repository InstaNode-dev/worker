.PHONY: build test docker-build run smoke-buildinfo

# Build-time metadata injected into instant.dev/common/buildinfo via -ldflags.
# Override on the make line if needed. GIT_SHA falls back to "dev" when not
# in a git checkout (e.g. CI tarball builds).
GIT_SHA    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION    ?= dev

build:
	go build ./...

test:
	go test ./... -race -count=1

docker-build:
	docker build -f Dockerfile -t instant-worker:local \
	  --build-arg GIT_SHA=$(GIT_SHA) \
	  --build-arg BUILD_TIME=$(BUILD_TIME) \
	  --build-arg VERSION=$(VERSION) \
	  ..

run:
	go run .

# Verifies the -ldflags injection wires through to instant.dev/common/buildinfo.
# Builds the smoke helper with override values and asserts they appear at
# runtime. CI runs this on every PR to catch a regression where someone
# breaks the ldflag path.
smoke-buildinfo:
	@tmpdir=$$(mktemp -d) && \
	  go build -ldflags "-X instant.dev/common/buildinfo.GitSHA=smoke-sha -X instant.dev/common/buildinfo.BuildTime=smoke-time -X instant.dev/common/buildinfo.Version=smoke-ver" \
	    -o $$tmpdir/smoke ./cmd/smoke-buildinfo && \
	  out=$$($$tmpdir/smoke) && \
	  echo "$$out" | grep -q "GitSHA=smoke-sha" || (echo "FAIL: $$out" && exit 1) && \
	  echo "$$out" | grep -q "BuildTime=smoke-time" || (echo "FAIL: $$out" && exit 1) && \
	  echo "$$out" | grep -q "Version=smoke-ver" || (echo "FAIL: $$out" && exit 1) && \
	  echo "smoke-buildinfo: OK ($$out)" && \
	  rm -rf $$tmpdir
