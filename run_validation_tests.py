import sys
import os
import subprocess

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        default="src/validation",
        help="Path to folder or test file(s) to run with pytest"
    )
    
    # Pytest outputs
    parser.add_argument(
        "--capture",
        choices=["no", "sys", "fd"],
        default="no",
        help="How to capture output (default: no)"
    )

    args = parser.parse_args()

    test_path = os.path.abspath(args.path)
    print(f"Running pytest on: {test_path}\n")

    result = subprocess.run(["pytest", test_path, f"--capture={args.capture}"])
    sys.exit(result.returncode)
