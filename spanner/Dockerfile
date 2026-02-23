FROM golang:1.24 AS build

WORKDIR /workspace
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/bypass-performance ./bypass-performance

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=build /out/bypass-performance /app/bypass-performance

ENTRYPOINT ["/app/bypass-performance"]
