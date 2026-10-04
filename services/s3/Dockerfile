# Standalone devcloud S3 endpoint (no orchestrator, dashboard, or event relay).
#
# Build from the repository root:
#   docker build -f services/s3/Dockerfile -t devcloud-s3 .
# Multi-arch (the build stage runs natively and cross-compiles, because rustc
# crashes under amd64 emulation on arm64 hosts):
#   docker buildx build --platform linux/amd64,linux/arm64 -f services/s3/Dockerfile .
# Run:
#   docker run --rm -p 9000:9000 -v devcloud-s3-data:/data devcloud-s3
#
# Auth defaults to relaxed. Strict auth: pass DEVCLOUD_S3_AUTH_MODE=strict together with
# DEVCLOUD_S3_ACCESS_KEY_ID / DEVCLOUD_S3_SECRET_ACCESS_KEY / DEVCLOUD_S3_REGION.

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
    && cargo build --release --locked -p devcloud-s3 --bin devcloud-s3 --target "$triple" \
    && cp "target/$triple/release/devcloud-s3" /devcloud-s3

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --no-create-home devcloud \
    && mkdir /data && chown devcloud /data
COPY --from=build /devcloud-s3 /usr/local/bin/devcloud-s3
USER devcloud
ENV DEVCLOUD_S3_ADDR=0.0.0.0:9000 \
    DEVCLOUD_S3_STORAGE=/data
VOLUME /data
EXPOSE 9000
ENTRYPOINT ["devcloud-s3"]
