# Standalone devcloud S3 endpoint (no orchestrator, dashboard, or event relay).
#
# Build from the repository root:
#   docker build -f services/s3/Dockerfile -t devcloud-s3 .
# Run:
#   docker run --rm -p 9000:9000 -v devcloud-s3-data:/data devcloud-s3
#
# Auth defaults to relaxed. Strict auth: pass DEVCLOUD_S3_AUTH_MODE=strict together with
# DEVCLOUD_S3_ACCESS_KEY_ID / DEVCLOUD_S3_SECRET_ACCESS_KEY / DEVCLOUD_S3_REGION.

FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --locked -p devcloud-s3 --bin devcloud-s3

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --no-create-home devcloud \
    && mkdir /data && chown devcloud /data
COPY --from=build /src/target/release/devcloud-s3 /usr/local/bin/devcloud-s3
USER devcloud
ENV DEVCLOUD_S3_ADDR=0.0.0.0:9000 \
    DEVCLOUD_S3_STORAGE=/data
VOLUME /data
EXPOSE 9000
ENTRYPOINT ["devcloud-s3"]
