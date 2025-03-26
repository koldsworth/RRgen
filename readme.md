## How to Use

### 1. Generate a Dataset

Use `generate.py` to generate a synthetic dataset:

```
python generate.py --records 5000 --output output_folder --seed 11
```

- `--records` - Number of records to generate  
- `--output` - Path to output folder  
- `--seed` - *(Optional)* Seed for reproducibility

---

### 2. Run Validation Tests

Use `run_tests.py` to validate the generated data using **pytest**:

```
python run_tests.py
```

This runs all test files in the default folder: `src/validation`.

To run a specific test file or folder:

```
python run_tests.py --path src/validation/test_rules.py
```

#### Optional Arguments

- `--path` - Path to folder or test file(s) to run  
  *(Default: `src/validation`)*
- `--capture` - How to capture pytest output (`no`, `sys`, or `fd`)  
  *(Default: `no`)*

#### Example with Custom Output Capture

```
python run_tests.py --path src/validation --capture=sys
```

This will check for logical inconsistencies and rule violations in the dataset using the defined test rules.
