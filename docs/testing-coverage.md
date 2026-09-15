# Rust coverage gates

The S3 and GCS `full-test-coverage` stages measure Rust source-line coverage with
`cargo-llvm-cov`. Passing `cargo test` alone is not a coverage measurement.

Install the local tools (in addition to the repository's normal Rust/protoc prerequisites):

```sh
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
```

Run the existing gates:

```sh
VERIFY_STAGE=full-test-coverage bash scripts/s3-test-coverage-autoloop/verify.sh
VERIFY_STAGE=full-test-coverage bash scripts/gcs-test-coverage-autoloop/verify.sh
```

Each gate runs a fresh `cargo llvm-cov --workspace --json --summary-only`
measurement. The helper aggregates covered/total lines for the selected crate's
`src/` directory, not its integration test files, other workspace packages, or a
hardcoded percentage. Temporary JSON reports are removed on exit. Existing
thresholds are unchanged: S3 72% / Dashboard 68.5%, and GCS 72% / Dashboard 70%.
Comparisons use exact fractions before rounding displayed percentages.

Missing tools, failed tests, missing/malformed coverage reports, invalid line
counts, or no measurable source lines fail the gate. No stale report is reused,
and `--ignore-run-fail`, `--no-clean` and `--no-run` are deliberately not used.
These are line-coverage thresholds, not claims of branch coverage, E2E coverage,
provider feature completeness, or bug absence.

Test the gate's failure handling without installing Rust or LLVM:

```sh
python3 -m unittest discover -s scripts/tests -p 'test_coverage_gate.py' -v
```

These regression tests invoke the actual S3/GCS shell threshold functions with
an isolated fake Cargo executable. They cover failure propagation, low coverage,
exact boundaries, rounding, malformed reports, and irrelevant files.
