#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

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
- The number of rows and columns in both CSV files match.
- For each value, the relative difference between the result and the reference does not
  exceed the given threshold.
- If a reference value is 0, any non-zero result is considered an error.

If any value exceeds the allowed deviation, the script exits with an error.
"""


def load_threshold(file: Path) -> float:
    """Load threshold value from txt file"""
    with open(file, "r") as f:
        return float(f.read())


def load_result(file: Path) -> dict[int, list[int]]:
    """Load result from csv file, key is record_size"""
    result = {}
    with open(file, "r") as f:
        for line in f:
            parts = line.strip().split(",")
            result[int(parts[0])] = list(map(int, parts[1:]))
    return result


def compare_results(
    threshold: float, result: dict[int, list[int]], result_gt: dict[int, list[int]]
) -> bool:
    fields = [
        "record_num",
        "write_req_per_sec",
        "write_bytes_per_sec",
        "read_req_per_sec",
        "read_bytes_per_sec",
        "update_req_per_sec",
        "remove_req_per_sec",
    ]

    "Compare results with relative threshold"
    assert len(result) == len(result_gt)
    assert all(len(result[key]) == len(result_gt[key]) == len(fields) for key in result)

    for key in result:
        for field, v, v_gt in zip(fields, result[key], result_gt[key]):
            if v_gt == 0 and v != 0:
                print(
                    f"Error: Reference value is 0, but result is {v} for record_size: {key} and field: {field}"
                )
                return False
            else:
                d = (v - v_gt) / v_gt
                if d > threshold:
                    print(
                        f"Error: Relative difference {d:.2f} exceeds threshold {threshold} for record_size: {key} and field: {field}"
                    )
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

    threshold = load_threshold(args.threshold)
    result = load_result(args.result)
    result_gt = load_result(args.result_gt)

    r = compare_results(threshold, result, result_gt)

    if not r:
        sys.exit(1)


if __name__ == "__main__":
    main()
