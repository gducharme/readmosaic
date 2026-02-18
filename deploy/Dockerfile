# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.24.3-alpine3.20 AS builder
WORKDIR /src

RUN apk add --no-cache ca-certificates

# Copy module metadata for dependency resolution first.
COPY go.mod go.sum ./
RUN go mod download

# Copy source after deps to preserve layer cache efficiency.
COPY cmd ./cmd
COPY internal ./internal

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' \
    -o /out/mosaic-server ./cmd/server

FROM alpine:3.20.6 AS runtime
WORKDIR /app

RUN addgroup -S -g 65532 mosaic && adduser -S -D -H -u 65532 -G mosaic mosaic \
    && apk add --no-cache ca-certificates netcat-openbsd \
    && mkdir -p /run/keys \
    && chown -R mosaic:mosaic /run/keys /app

COPY --from=builder /out/mosaic-server /usr/local/bin/mosaic-server
COPY internal/content/vector_a_read_fragment.txt /app/internal/content/vector_a_read_fragment.txt

USER 65532:65532
EXPOSE 2222

ENTRYPOINT ["/usr/local/bin/mosaic-server"]
CMD []
