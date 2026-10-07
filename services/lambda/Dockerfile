# Standalone devcloud Lambda endpoint (no orchestrator, dashboard, or event relay).
#
# Build from the repository root:
#   docker build -f services/lambda/Dockerfile -t devcloud-lambda .
# Multi-arch (the build stage runs natively and cross-compiles, because rustc
# crashes under amd64 emulation on arm64 hosts):
#   docker buildx build --platform linux/amd64,linux/arm64 -f services/lambda/Dockerfile .
# Run (port 3001, like `sam local start-lambda`):
#   docker run --rm -p 3001:3001 -v devcloud-lambda-data:/data devcloud-lambda
#
# Handlers run as python3 / node child processes inside this container: Python
# 3.12 (with boto3, like Lambda's Python runtime) and Node.js 22, whatever the
# function's Runtime version says; a mismatch is flagged as a [WARNING] line in
# the invocation log and on stderr. Environments stay warm between invocations
# for DEVCLOUD_LAMBDA_IDLE_TIMEOUT_SECONDS (default 300; 0 = always cold), and
# every invocation's log is printed to stdout (DEVCLOUD_LAMBDA_LOG_INVOCATIONS).
# Layer contents: mount them at /opt (e.g. -v ./deps:/opt/python); /opt/python
# and /opt/nodejs/node_modules are on the default PYTHONPATH / NODE_PATH.
#
# PackageType Image functions need Docker. The default target has none; build
# the `docker` target (adds the docker CLI) and run it with the host's socket
# and the Docker network it is on, e.g. in compose:
#   image: <this image built with --target docker>
#   environment: { DEVCLOUD_LAMBDA_DOCKER: "true", DEVCLOUD_LAMBDA_DOCKER_NETWORK: <project>_default }
#   volumes: ["/var/run/docker.sock:/var/run/docker.sock"]
# Access to the socket is root on the Docker host; that target runs as root.
# Credentials for handlers: DEVCLOUD_LAMBDA_FUNCTION_ACCESS_KEY_ID /
# DEVCLOUD_LAMBDA_FUNCTION_SECRET_ACCESS_KEY (/ DEVCLOUD_LAMBDA_FUNCTION_SESSION_TOKEN).
# Auth defaults to relaxed. Strict auth: pass DEVCLOUD_LAMBDA_AUTH_MODE=strict together with
# DEVCLOUD_LAMBDA_ACCESS_KEY_ID / DEVCLOUD_LAMBDA_SECRET_ACCESS_KEY / DEVCLOUD_LAMBDA_REGION.
# Code.S3Bucket/S3Key: mount a devcloud-s3 data volume and set DEVCLOUD_LAMBDA_S3_STORAGE to it.
# Set DEVCLOUD_LAMBDA_ENDPOINT when clients reach the container under another host/port.

FROM --platform=$BUILDPLATFORM rust:1-slim-bookworm AS build
ARG TARGETARCH
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu; gnu=x86_64-linux-gnu; pkg=x86-64-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu; pkg=aarch64-linux-gnu ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    echo "$triple" > /rust-target; \
    rustup target add "$triple"; \
    if [ "$TARGETARCH" != "$(dpkg --print-architecture)" ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends "gcc-$pkg" "libc6-dev-$TARGETARCH-cross"; \
      rm -rf /var/lib/apt/lists/*; \
      printf '[target.%s]\nlinker = "%s-gcc"\n' "$triple" "$gnu" >> "$CARGO_HOME/config.toml"; \
    fi
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    triple="$(cat /rust-target)" \
    && cargo build --release --locked -p devcloud-lambda --bin devcloud-lambda --target "$triple" \
    && cp "target/$triple/release/devcloud-lambda" /devcloud-lambda

FROM node:22-bookworm-slim AS node

FROM python:3.12-slim-bookworm AS runtime
# tini reaps processes that handlers leave behind (PID 1 would not).
# libstdc++6 is Node's only library dependency missing from the Python base.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --root-user-action=ignore boto3 \
    && useradd --system --uid 10001 --no-create-home devcloud \
    && mkdir /data && chown devcloud /data
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=build /devcloud-lambda /usr/local/bin/devcloud-lambda
USER devcloud
ENV DEVCLOUD_LAMBDA_ADDR=0.0.0.0:3001 \
    DEVCLOUD_LAMBDA_ENDPOINT=http://localhost:3001 \
    DEVCLOUD_LAMBDA_STORAGE=/data
VOLUME /data
EXPOSE 3001
ENTRYPOINT ["tini", "--", "devcloud-lambda"]

# `--target docker`: adds the docker CLI for PackageType Image functions.
FROM docker:27-cli AS docker-cli
FROM runtime AS docker
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
USER root
ENV DEVCLOUD_LAMBDA_DOCKER=true

# The default target stays Docker-free.
FROM runtime
