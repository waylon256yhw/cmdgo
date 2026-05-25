# syntax=docker/dockerfile:1.7

# ---- build ----
FROM golang:1.26.1-alpine AS builder
WORKDIR /src

# go.mod is the only manifest; cmdgo has no external dependencies, so
# there's no go.sum and `go mod download` is a no-op. We still copy
# the manifest first so future deps benefit from layer caching.
COPY go.mod ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
# CGO_ENABLED=0 → statically linked, runs on scratch without libc.
# -trimpath + -s -w → reproducible, smaller (~9 MiB stripped).
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/cmdgo .

# ---- runtime ----
# scratch + ca-certificates lifted from alpine. The binary writes
# state.json into /data, so mount a volume there in production.
FROM alpine:3.20 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/cmdgo /cmdgo

VOLUME ["/data"]
ENV CMDGO_DATA=/data/state.json
EXPOSE 8080
ENTRYPOINT ["/cmdgo"]
CMD ["--listen", "0.0.0.0:8080"]
