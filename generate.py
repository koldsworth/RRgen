"""
Usage examples

$ python generate.py  # generate default 10k rows
$ python generate.py --records 5000 --output output/test  # custom record count and output
"""
from __future__ import annotations
import argparse
import sys
import os
from src.generation.main import main


def _parse_args():
    p = argparse.ArgumentParser(description="Generate synthetic address data.")
    p.add_argument(
        "--output",
        default="output",
        help="Where to save generated files"
    )
    p.add_argument(
        "--records",
        type=int,
        default=10000,
        help="How many address records to generate"
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed"
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(num_records=args.records, output_folder=args.output, seed=args.seed)
