#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path


def load_threshold(file: Path) -> float:
    """Load threshold value from txt file"""
    with open(file, "r") as f:
        return float(f.read())


def load_result(file: Path) -> dict[int, list[int]]:
    """Load result from csv file"""
    result = {}
    with open(file, "r") as f:
        for line in f:
            parts = line.strip().split(",")
            result[int(parts[0])] = list(map(int, parts[1:]))
    return result


def compare_results(
    threshold: float, result: dict[int, list[int]], result_gt: dict[int, list[int]]
) -> bool:
    """Compare results with relative threshold"""
    assert len(result) == len(result_gt)
    assert all(len(result[key]) == len(result_gt[key]) for key in result)

    for key in result:
        for v, v_gt in zip(result[key], result_gt[key]):
            if v_gt == 0 and v != 0:
                return False
            else:
                d = (v - v_gt) / v_gt
                if d > threshold:
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

    if r:
        print("Results are correct")
    else:
        sys.exit("Results are incorrect")


if __name__ == "__main__":
    main()
