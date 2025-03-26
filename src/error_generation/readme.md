## Invalid Test Cases Provided by SMIT

The following test cases are designed to generate invalid data that intentionally contradict specific validation rules:

1. **Multiple Active Residences**  
   A person who is marked as alive has **two or more** residences with status **"KEHTIV"** (active).  

2. **Expired Driving License Still Active**  
   A person has a driving license with status **"KEHTIV"**, but the **expiry date has already passed**.

3. **Future-Dated Residence Start**  
   A person’s residence **start date is in the future** — they have not yet started living at the address.

4. **Conflicting Citizenship Information**  
   A person’s **main citizenship is listed as "MÄÄRATLEMATA"** (unknown), yet they also have a **known citizenship** assigned.

---

Tests are currently executed using the `run_tests.py` script.
