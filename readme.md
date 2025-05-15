## How to Use

### 1. Generate a Dataset

Use `generate.py` to generate a synthetic dataset:

```
python generate.py --records 5000 --output output_folder --seed 11
```

**Arguments:**
- `--records` - Number of records to generate  
- `--output` - Path to output folder  
- `--seed` - *(Optional)* Random seed for reproducibility

---

### 2. Inject Errors (Optional)

Use `generate_errors.py` to inject rule-violating data for testing validation logic:

```
python generate_errors.py --tests 1,2
```

**Arguments:**
- `--tests` - Comma-separated list of test cases to run (e.g. `1,2,3`). If omitted, runs all.

---

### 3. Run Validation Tests

Use `run_validation_tests.py` to validate the generated dataset using **pytest**:

```
python run_validation_tests.py
```

By default, this runs all tests in the folder: `src/validation`.

You can also specify a file or folder with `--path`, and configure pytest output with `--capture`.

**Example:**

```
python run_validation_tests.py --path src/validation/test_isik.py --capture sys
```
