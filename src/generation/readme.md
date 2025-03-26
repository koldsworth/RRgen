## Overview

This folder contains the **main files for generating synthetic data**.  
It is designed for generating **large volumes of data** (full tables), rather than individual rows.

### Structure

- Each file defines **rules for generating one or two tables**.
- Tables prefixed with `temp_` are **temporary helper tables** used in intermediate steps.
- `utils.py` contains **shared helper functions** for generation logic.
- `main.py` is the **entry point** for generating the full dataset.
- `main.py` is imported and used in `generate.py`.

### Reference Materials

All data generation logic was based on public references from  
[RIHA – Rahvastikuregister infosüsteem](https://www.riha.ee/Infos%C3%BCsteemid/Vaata/rr):

- **RR_andmekoosseis_20220622.csv** – for schema structure and field definitions  
- **X-tee_teenused.docx** – for understanding service queries, relationships, and overall logic

---

This setup allows automated generation of interrelated synthetic datasets that closely follow the official population register data structure.
