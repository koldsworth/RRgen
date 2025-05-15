"""
Usage examples

$ python run_validation_tests.py  # run all validation tests
$ python run_validation_tests.py --path src/validation/test_isikuaadress.py  # specific file
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys


def _parse_args():
    p = argparse.ArgumentParser(description="Run validation tests using pytest.")
    p.add_argument(
        "--path",
        default="src/validation",
        help="Path to folder or test file(s) to run with pytest"
    )
    p.add_argument(
        "--capture",
        choices=["no", "sys", "fd"],
        default="no",
        help="How to capture output (default: no)"
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    test_path = os.path.abspath(args.path)
    print(f"Running pytest on: {test_path}\n")

    result = subprocess.run(["pytest", test_path, f"--capture={args.capture}"])
    sys.exit(result.returncode)
