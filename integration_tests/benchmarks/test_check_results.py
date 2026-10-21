import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from check_results import compare_results, load_result, load_threshold


def row(value: int, record_num: int = 100) -> list[int]:
    return [record_num] + [value] * 6


class CompareResultsTest(unittest.TestCase):
    def test_passes_below_threshold_and_at_boundary(self):
        self.assertTrue(compare_results(0.33, {1: row(67)}, {1: row(100)}))
        self.assertTrue(compare_results(0.33, {1: row(68)}, {1: row(100)}))

    def test_fails_above_threshold_and_reports_every_regression(self):
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertFalse(compare_results(0.33, {1: row(60)}, {1: row(100)}))
        self.assertEqual(output.getvalue().count("regression 40.00%"), 6)

    def test_improvement_does_not_fail(self):
        self.assertTrue(compare_results(0.33, {1: row(200)}, {1: row(100)}))

    def test_zero_reference_is_handled(self):
        self.assertTrue(compare_results(0.33, {1: row(0)}, {1: row(0)}))
        self.assertTrue(compare_results(0.33, {1: row(1)}, {1: row(0)}))

    def test_rejects_mismatched_record_sizes_and_counts(self):
        with self.assertRaises(ValueError):
            compare_results(0.33, {1: row(100)}, {2: row(100)})
        with self.assertRaises(ValueError):
            compare_results(0.33, {1: row(100, 99)}, {1: row(100, 100)})

    def test_rejects_malformed_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.csv"
            path.write_text("1,2,3\n")
            with self.assertRaises(ValueError):
                load_result(path)

    def test_rejects_invalid_threshold(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "threshold"
            path.write_text("1.1")
            with self.assertRaises(ValueError):
                load_threshold(path)


if __name__ == "__main__":
    unittest.main()
