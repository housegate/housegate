# syntax=docker/dockerfile:1
# Container image used by the release pipeline. The linux/amd64 binary
# is built outside of docker via Bazel + zig and staged alongside this
# Dockerfile as `./housegate` before `docker build` runs; we don't do a
# multi-stage build here so the image build skips the Go module download
# and Bazel toolchain download that already happened in the release job.
FROM gcr.io/distroless/base-debian12:nonroot

COPY housegate /housegate

EXPOSE 9000

ENTRYPOINT ["/housegate"]
