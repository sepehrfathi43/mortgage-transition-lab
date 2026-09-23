# Validation log

Checked on September 23, 2026 with Python 3.12.14 on Windows 11, Dask 2026.8.0, pandas 3.0.6, and PyArrow 25.0.1.

- 16 automated tests passed, including a full ingestion-to-survival test with deliberately constructed edge cases.
- The official Release 47 format sample completed end to end.
- One loan with inconsistent exit timing was quarantined.
- The sample's origination and performance files have no overlapping loan IDs. The output reports unmatched records; missing origination covariates remain missing.
- The pipeline was run with local threads and with two separate local Dask worker processes. Both used four output partitions and 8 KiB input blocks to exercise histories crossing input boundaries.
- Transition counts/probabilities, loan-level survival records, and competing-risk curves matched between those executions.
- Observed transition probabilities sum to one within each starting state.

These checks establish behavior on a small official sample and targeted test cases. They do not validate historical-cohort estimates, neural models, multi-machine deployment, or performance at full dataset scale. Downloaded records and numerical analysis outputs are not included in the repository.
