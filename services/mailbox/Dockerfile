# Build from the repository root:
#   docker build -f services/mailbox/Dockerfile -t devcloud-mailbox .
#   docker buildx build --platform linux/amd64,linux/arm64 -f services/mailbox/Dockerfile .
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
    && cargo build --release --locked -p devcloud-mailbox --bin devcloud-mailbox --target "$triple" \
    && cp "target/$triple/release/devcloud-mailbox" /devcloud-mailbox

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --no-create-home devcloud \
    && mkdir /data && chown devcloud /data
COPY --from=build /devcloud-mailbox /usr/local/bin/devcloud-mailbox
USER devcloud
ENV DEVCLOUD_MAILBOX_SMTP_ADDR=0.0.0.0:1025 \
    DEVCLOUD_MAILBOX_HTTP_ADDR=0.0.0.0:8025 \
    DEVCLOUD_MAILBOX_STORAGE=/data
VOLUME /data
EXPOSE 1025 8025
ENTRYPOINT ["devcloud-mailbox"]
