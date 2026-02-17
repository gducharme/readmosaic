.PHONY: run build test vet lint ci-offline

run:
	go run ./cmd/server

build:
	go build -o bin/mosaic-server ./cmd/server

test:
	go test ./...

vet:
	go vet ./...

lint:
	staticcheck ./...

ci-offline:
	GOPROXY=off GOSUMDB=off go test ./...
	GOPROXY=off GOSUMDB=off go build ./cmd/server
