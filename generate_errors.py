"""
Usage examples

$ python generate_errors.py # run all cases
$ python generate_errors.py --tests 1,2  # only cases 1 and 2
"""
from __future__ import annotations
import argparse
from src.error_generation.main import run


def _parse_args():
    p = argparse.ArgumentParser(description="Inject deliberate data errors.")
    p.add_argument(
        "--tests", "-t",
        metavar="IDS",
        help="Comma-separated list of test numbers to run (default: all)",
    )
    ns = p.parse_args()
    if ns.tests in (None, "", "all", "ALL"):
        return None
    try:
        return [int(x) for x in ns.tests.split(",")]
    except ValueError as exc:
        raise SystemExit("--tests must be integers, e.g. 1,2,4") from exc


if __name__ == "__main__":
    run(_parse_args())
