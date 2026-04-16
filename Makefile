.PHONY: build test docker-build run

build:
	go build ./...

test:
	go test ./... -race -count=1

docker-build:
	docker build -f Dockerfile -t instant-worker:local ..

run:
	go run .
