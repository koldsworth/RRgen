import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from src.generation.main import main

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output", 
        default="output", 
        help="Where to save generated files"
        )
    
    parser.add_argument(
        "--records", 
        type=int, 
        default=10000, 
        help="How many address records to generate")
    
    parser.add_argument(
        "--seed", 
        type=int, 
        default=42, 
        help="Random seed"
        )

    args = parser.parse_args()

    main(num_records=args.records, output_folder=args.output, seed=args.seed)

    #python generate.py --records 5000 --output output -seed 11
