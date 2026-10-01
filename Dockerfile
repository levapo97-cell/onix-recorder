# Multi-stage: compila estático y corre en distroless. El host NO necesita Go.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/onix-recorder ./cmd/onix-recorder

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/onix-recorder /onix-recorder
EXPOSE 8082
USER nonroot:nonroot
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/onix-recorder", "-healthcheck"]
ENTRYPOINT ["/onix-recorder"]
