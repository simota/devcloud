#!/usr/bin/env python3
"""Measure fresh workspace line coverage and enforce package/source thresholds."""
import json
from fractions import Fraction
from pathlib import Path
import subprocess
import sys
import tempfile


def covered_lines(report, source_root, workspace):
    """Sum LLVM line counts only inside the requested crate's src directory."""
    data = report.get("data")
    if not isinstance(data, list) or len(data) != 1:
        raise ValueError("expected one LLVM coverage data set")
    files = data[0].get("files")
    if not isinstance(files, list):
        raise ValueError("coverage report has no file summaries")
    covered = total = 0
    seen = set()
    for entry in files:
        filename = entry["filename"]
        if not isinstance(filename, str):
            raise ValueError("invalid coverage filename")
        path = Path(filename)
        path = (workspace / path).resolve() if not path.is_absolute() else path.resolve()
        if source_root not in path.parents:
            continue
        if path in seen:
            raise ValueError("duplicate source file in coverage report")
        seen.add(path)
        lines = entry["summary"]["lines"]
        count, hit = lines["count"], lines["covered"]
        if type(count) is not int or type(hit) is not int or not 0 <= hit <= count:
            raise ValueError("invalid source line counts in coverage report")
        total += count
        covered += hit
    if not seen or total == 0:
        raise ValueError("no measurable source lines for requested package")
    return covered, total


def main():
    if len(sys.argv) < 3:
        print("usage: rust-coverage.py WORKSPACE PACKAGE:MIN_PERCENT [...]", file=sys.stderr)
        return 2
    try:
        workspace = Path(sys.argv[1]).resolve()
        thresholds = []
        for value in sys.argv[2:]:
            package, raw_minimum = value.rsplit(":", 1)
            minimum = Fraction(raw_minimum)
            source = (workspace / package / "src").resolve()
            if workspace not in source.parents or not source.is_dir():
                raise ValueError("coverage package must have a src directory inside the workspace")
            if not 0 <= minimum <= 100:
                raise ValueError("coverage threshold must be between zero and 100")
            thresholds.append((package, source, minimum))
        with tempfile.TemporaryDirectory(prefix="devcloud-rust-coverage-") as temp:
            report_path = Path(temp) / "coverage.json"
            # A fresh invocation (without --no-clean/--no-run) avoids stale
            # profiles. Never use --ignore-run-fail: failing tests fail the gate.
            subprocess.run(
                ["cargo", "llvm-cov", "--workspace", "--json", "--summary-only",
                 "--output-path", str(report_path)],
                cwd=workspace, stdout=sys.stderr, check=True,
            )
            with report_path.open(encoding="utf-8") as stream:
                report = json.load(stream)
            failed = False
            for package, source, minimum in thresholds:
                covered, total = covered_lines(report, source, workspace)
                percentage = Fraction(100 * covered, total)
                passed = percentage >= minimum  # Compare before display rounding.
                print(f"{'PASS' if passed else 'FAIL'} {package}: "
                      f"{covered}/{total} source lines ({float(percentage):.4f}%), "
                      f"minimum {float(minimum):g}%")
                failed |= not passed
            return 1 if failed else 0
    except subprocess.CalledProcessError as error:
        print(f"coverage measurement failed (exit {error.returncode}); "
              "no coverage percentage accepted", file=sys.stderr)
        return 2
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        print(f"coverage gate error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
