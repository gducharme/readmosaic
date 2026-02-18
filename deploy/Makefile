.PHONY: run build test vet lint ci-offline build-image up down logs docker-test

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

build-image:
	docker build -t mosaic-terminal:local -f Dockerfile .

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f app

docker-test:
	docker run --rm mosaic-terminal:local /usr/local/bin/mosaic-server
