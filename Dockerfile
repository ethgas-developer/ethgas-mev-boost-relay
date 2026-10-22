# syntax=docker/dockerfile:1
FROM golang:1.26.10-bookworm AS builder
ARG VERSION
WORKDIR /build

# Refresh build-time HTTPS trust after the eMudhra CA incident (DLA-4845-1).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge '20250419~deb12u2' \
    && rm -rf /var/lib/apt/lists/*

# Cache for the modules
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

# Now adding all the code and start building
ADD . .
#RUN --mount=type=cache,target=/root/.cache/go-build GOOS=linux go build -trimpath -ldflags "-s -X cmd.Version=$VERSION -X main.Version=$VERSION -linkmode external -extldflags '-static'" -v -o mev-boost-relay .
RUN --mount=type=cache,target=/root/.cache/go-build GOOS=linux go build -trimpath -ldflags "-X cmd.Version=$VERSION -X main.Version=$VERSION" -v -o mev-boost-relay .

FROM alpine:3.24
# Use the patched runtime trust store instead of copying the builder's bundle.
RUN apk add --no-cache 'ca-certificates>=20261020-r0' libstdc++ libc6-compat
WORKDIR /app
COPY --from=builder /build/mev-boost-relay /app/mev-boost-relay
ENTRYPOINT ["/app/mev-boost-relay"]
