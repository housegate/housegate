# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY main.go .
RUN go mod init kproxy >/dev/null 2>&1 && CGO_ENABLED=0 go build -o /kproxy .

FROM alpine:3.20
COPY --from=build /kproxy /kproxy
ENTRYPOINT ["/kproxy"]
