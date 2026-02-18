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
	@test -f ./data/ssh/host_ed25519 || (echo "missing ./data/ssh/host_ed25519" && exit 1)
	docker run --rm \
	  -e MOSAIC_SSH_HOST=0.0.0.0 \
	  -e MOSAIC_SSH_PORT=2222 \
	  -e MOSAIC_SSH_HOST_KEY_PATH=/run/keys/ssh_host_ed25519 \
	  -e LISTEN_ADDR=0.0.0.0:8080 \
	  -e ARWEAVE_TXID_MANIFESTO_EN=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA \
	  -e ARWEAVE_TXID_MANIFESTO_AR=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB \
	  -e ARWEAVE_TXID_MANIFESTO_ZH=CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC \
	  -e ARWEAVE_TXID_GENESIS=DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD \
	  -e BTC_ANCHOR_HEIGHT=0 \
	  -v "$(PWD)/data/ssh/host_ed25519:/run/keys/ssh_host_ed25519:ro" \
	  --entrypoint /bin/sh \
	  mosaic-terminal:local -c 'test -x /usr/local/bin/mosaic-server'
