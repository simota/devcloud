"""Regression tests for the actual S3/GCS shell coverage gates (no Rust needed)."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class CoverageGateTests(unittest.TestCase):
    def report(self, covered=50, count=100):
        return {
            "data": [{"files": [
                {"filename": str(ROOT / "services" / name / "src" / "lib.rs"),
                 "summary": {"lines": {"count": count, "covered": covered}}}
                for name in ("s3", "gcs", "dashboard")
            ]}]
        }

    def gate(self, service, report, cargo_status=0):
        script = ROOT / "scripts" / f"{service}-test-coverage-autoloop" / "verify.sh"
        functions = re.findall(r"^(?:coverage_value|assert_min_coverage|assert_coverage_thresholds)\(\) \{\n.*?^\}", script.read_text(), re.M | re.S)
        self.assertTrue(functions, "exercise the real gate functions")
        with tempfile.TemporaryDirectory(prefix="devcloud-coverage-test-") as temp:
            temp = Path(temp)
            fixture = temp / "fixture.json"
            fixture.write_text(report if isinstance(report, str) else json.dumps(report))
            cargo = temp / "cargo"
            cargo.write_text('''#!/usr/bin/env bash
if [[ "$FAKE_CARGO_STATUS" != 0 ]]; then exit "$FAKE_CARGO_STATUS"; fi
while [[ "$#" -gt 0 ]]; do
    if [[ "$1" == --output-path ]]; then
        cp "$COVERAGE_FIXTURE" "$2"
        exit "$?"
    fi
    shift
done
''')
            cargo.chmod(0o700)
            env = dict(os.environ, ROOT_DIR=str(ROOT), COVERAGE_FIXTURE=str(fixture),
                       FAKE_CARGO_STATUS=str(cargo_status), PATH=str(temp) + os.pathsep + os.environ['PATH'])
            # run_check invokes its argument in an if condition, suppressing
            # Bash errexit. The gate must propagate errors explicitly.
            command = "set -eu\n" + "\n".join(functions) + "\nif assert_coverage_thresholds; then exit 0; else exit 1; fi\n"
            return subprocess.run(["bash", "-c", command], env=env,
                                  text=True, capture_output=True, timeout=15)

    def check_rejected(self, report, cargo_status=0):
        for service in ("s3", "gcs"):
            with self.subTest(service=service):
                result = self.gate(service, report, cargo_status)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_low_coverage_is_not_reported_as_100_percent(self):
        self.check_rejected(self.report(50))

    def test_failed_cargo_command_cannot_pass(self):
        self.check_rejected(self.report(100), cargo_status=1)

    def test_malformed_json_cannot_pass(self):
        self.check_rejected('{"data":')

    def test_missing_files_cannot_pass(self):
        self.check_rejected({"data": [{"files": []}]})

    def test_zero_measurable_lines_cannot_pass(self):
        self.check_rejected(self.report(0, 0))

    def test_invalid_counts_cannot_pass(self):
        self.check_rejected(self.report(101, 100))

    def test_rounding_cannot_promote_below_threshold(self):
        self.check_rejected(self.report(71_999, 100_000))

    def test_other_package_and_test_files_cannot_inflate_coverage(self):
        report = self.report(50)
        for service in ("s3", "gcs", "dashboard", "unrelated"):
            report['data'][0]['files'].append({
                'filename': str(ROOT / 'services' / service / 'tests' / 'fixture.rs'),
                'summary': {'lines': {'count': 100_000, 'covered': 100_000}},
            })
        self.check_rejected(report)

    def test_actual_coverage_above_threshold_passes(self):
        for service in ("s3", "gcs"):
            with self.subTest(service=service):
                result = self.gate(service, self.report(80))
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_exact_existing_thresholds_pass(self):
        for service, dashboard_covered in (("s3", 685), ("gcs", 700)):
            with self.subTest(service=service):
                report = self.report(720, 1000)
                report['data'][0]['files'][2]['summary']['lines']['covered'] = dashboard_covered
                result = self.gate(service, report)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
