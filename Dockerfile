# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src/go

COPY go/go.mod go/go.sum ./
RUN go mod download

COPY go/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/emby-range-cache-proxy ./cmd/emby-range-cache-proxy

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
	&& adduser -D -H -u 10001 -s /sbin/nologin range-cache

COPY --from=build /out/emby-range-cache-proxy /usr/local/bin/emby-range-cache-proxy

USER 10001:10001
ENTRYPOINT ["/usr/local/bin/emby-range-cache-proxy"]
CMD ["--config", "/config/config.json"]
