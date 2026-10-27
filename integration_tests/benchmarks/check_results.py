#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

FIELDS = [
    "record_num",
    "write_req_per_sec",
    "write_bytes_per_sec",
    "read_req_per_sec",
    "read_bytes_per_sec",
    "update_req_per_sec",
    "remove_req_per_sec",
]

"""
This module compares the results of a benchmark run against a reference (ground truth) file
using a relative threshold specified in a separate text file.

It expects three inputs:
1. A CSV file containing benchmark results.
2. A CSV file with reference (ground truth) results in the same format.
3. A text file containing a single float value, representing the acceptable threshold
   for relative deviation.

The CSV files must have identical structure and contain the following columns:
    - record_size
    - record_num
    - write_req_per_sec
    - write_bytes_per_sec
    - read_req_per_sec
    - read_bytes_per_sec
    - update_req_per_sec
    - remove_req_per_sec

Each line in the CSV files represents a test case, where:
- The first column is treated as a key (e.g., record size),
- The remaining columns are performance metrics.

The script validates that:
- Record sizes, record counts, and row widths match.
- Values and the threshold are valid and non-negative.
- For each throughput value, the candidate's regression relative to the reference does
  not exceed the given threshold.
- A zero reference can be matched or improved without division by zero.

All regressions are reported before the script exits with an error.
"""


def load_threshold(file: Path) -> float:
    """Load threshold value from txt file"""
    with open(file, "r") as f:
        threshold = float(f.read())
    if not 0 <= threshold <= 1:
        raise ValueError(f"Threshold must be between 0 and 1, got {threshold}")
    return threshold


def load_result(file: Path) -> dict[int, list[int]]:
    """Load result from csv file, key is record_size"""
    result = {}
    with open(file, "r") as f:
        for line_number, line in enumerate(f, start=1):
            parts = line.strip().split(",")
            if len(parts) != len(FIELDS) + 1:
                raise ValueError(
                    f"{file}:{line_number}: expected {len(FIELDS) + 1} columns, "
                    f"got {len(parts)}"
                )
            record_size = int(parts[0])
            if record_size in result:
                raise ValueError(f"{file}:{line_number}: duplicate record size {record_size}")
            values = list(map(int, parts[1:]))
            if any(value < 0 for value in values):
                raise ValueError(f"{file}:{line_number}: values must be non-negative")
            result[record_size] = values
    return result


def compare_results(
    threshold: float, result: dict[int, list[int]], result_gt: dict[int, list[int]]
) -> bool:
    """Compare candidate results with a reference, failing only on regressions."""
    if not 0 <= threshold <= 1:
        raise ValueError(f"Threshold must be between 0 and 1, got {threshold}")
    if result.keys() != result_gt.keys():
        raise ValueError(
            "Candidate and reference record sizes differ: "
            f"candidate={sorted(result)}, reference={sorted(result_gt)}"
        )
    if not all(
        len(result[key]) == len(result_gt[key]) == len(FIELDS) for key in result
    ):
        raise ValueError(f"Each result row must contain {len(FIELDS)} values")

    report = []
    errors = []
    for key in result:
        if result[key][0] != result_gt[key][0]:
            raise ValueError(
                f"Record count differs for record_size {key}: "
                f"candidate={result[key][0]}, reference={result_gt[key][0]}"
            )

        for field, value, reference in zip(
            FIELDS[1:], result[key][1:], result_gt[key][1:]
        ):
            if reference == 0:
                pct = "0.00%" if value == 0 else "+inf%"
            else:
                difference = (reference - value) / reference
                if difference > threshold:
                    errors.append(
                        f"record_size={key} {field}: regression {difference:.2%} "
                        f"exceeds threshold {threshold:.2%} "
                        f"(ref={reference}, current={value})"
                    )
                pct = f"{((value - reference) / reference) * 100:+.2f}%"
            report.append(
                f"record_size={key} {field}: {pct} (ref={reference}, current={value})"
            )

    print("Benchmark comparison report:")
    for line in report:
        print(f" - {line}")
    if errors:
        print("Benchmark regressions:")
        for line in errors:
            print(f" - {line}")
        return False
    return True


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check results")
    parser.add_argument("threshold", type=Path)
    parser.add_argument("result", type=Path)
    parser.add_argument("result_gt", type=Path)
    return parser.parse_args()


def main():
    args = get_args()
    try:
        threshold = load_threshold(args.threshold)
        result = load_result(args.result)
        result_gt = load_result(args.result_gt)
        if not compare_results(threshold, result, result_gt):
            sys.exit(1)
    except (OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
