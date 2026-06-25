# Stage 1: Build binaries — pinned to BUILDPLATFORM so Go runs natively (fast).
# This is a download-to-disk client now: the FUSE mount layer was stripped, so the
# build no longer needs CGO/fuse-dev/clang — a plain CGO_ENABLED=0 cross-compile.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=0.0.0
ARG CHANNEL=dev

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download -x

COPY . .

# Build main binary (no CGO — pure-Go cross-compile)
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
    -ldflags="-w -s -X github.com/sirrobot01/decypharr/pkg/version.Version=${VERSION} -X github.com/sirrobot01/decypharr/pkg/version.Channel=${CHANNEL}" \
    -o /hoardarr

# Build healthcheck
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-w -s" \
    -o /healthcheck cmd/healthcheck/main.go

# Stage 2: Final image
FROM alpine:latest

ARG VERSION=0.0.0
ARG CHANNEL=dev

LABEL version="${VERSION}-${CHANNEL}"
LABEL org.opencontainers.image.source="https://github.com/sirrobot01/decypharr"
LABEL org.opencontainers.image.title="hoardarr"
LABEL org.opencontainers.image.authors="sirrobot01"
LABEL org.opencontainers.image.documentation="https://github.com/sirrobot01/decypharr/blob/main/README.md"

# No FUSE, no rclone, no ffprobe — this client downloads real files to disk over HTTP.
RUN apk add --no-cache ca-certificates su-exec shadow tzdata

# Copy binaries and entrypoint
COPY --from=builder /hoardarr /usr/bin/hoardarr
COPY --from=builder /healthcheck /usr/bin/healthcheck
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set environment variables
ENV PUID=1000
ENV PGID=1000
ENV LOG_PATH=/app/logs

EXPOSE 8282
VOLUME ["/app"]

HEALTHCHECK --interval=10s --retries=10 CMD ["/usr/bin/healthcheck", "--config", "/app"]

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/bin/hoardarr", "--config", "/app"]
