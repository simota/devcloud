# Radar Notes

| Date | Area | Action | Verification |
| --- | --- | --- | --- |
| 2026-05-04 | Pub/Sub tests | Added gRPC pagination, ack deadline, and ack release regression coverage. | `go test ./...`; `VERIFY_STAGE=full-compat bash scripts/pubsub-full-compat-autoloop/verify.sh`; Pub/Sub coverage 77.2%. |
