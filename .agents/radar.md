# Radar Notes

| Date | Area | Action | Verification |
| --- | --- | --- | --- |
| 2026-05-04 | Pub/Sub tests | Added gRPC pagination, ack deadline, and ack release regression coverage. | `go test ./...`; `VERIFY_STAGE=full-compat bash scripts/pubsub-full-compat-autoloop/verify.sh`; Pub/Sub coverage 77.2%. |
| 2026-05-05 | Redshift tests | Added PostgreSQL backend type/null/command-tag safety tests and Redshift translator edge coverage for function rewrites, metadata extraction, and malformed DDL. | `go test ./internal/services/redshift/... -cover`; `VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh`. |
