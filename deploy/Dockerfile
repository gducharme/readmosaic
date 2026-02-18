# syntax=docker/dockerfile:1.7

FROM golang:1.22-alpine AS builder
WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod ./
COPY third_party ./third_party
COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w -extldflags "-static"' \
    -o /out/mosaic-server ./cmd/server

# Provide minimal identity metadata for non-root execution in scratch.
RUN printf 'mosaic:x:65532:65532:Mosaic:/nonexistent:/sbin/nologin\n' > /out/passwd \
    && printf 'mosaic:x:65532:\n' > /out/group

FROM scratch

COPY --from=builder /out/mosaic-server /mosaic-server
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/passwd /etc/passwd
COPY --from=builder /out/group /etc/group

USER 65532:65532
EXPOSE 2222

ENTRYPOINT ["/mosaic-server"]
