# First lesson: turn monthly records into loan histories

## 1. Get the project

```powershell
git clone https://github.com/sepehrfathi43/mortgage-transition-lab.git
cd mortgage-transition-lab
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

GitHub's **Code > Download ZIP** works too. Extract it, open PowerShell in the extracted folder, and run the last two commands. You don't need Java, a GPU, or a cloud account for this lesson.

## 2. Download the official format sample

```powershell
.\.venv\Scripts\python.exe download_sample.py
```

This downloads the sample linked on Freddie Mac's dataset page, saves its archive and hash, and extracts two known files into `data/raw/`. It doesn't generate fake mortgage observations. The tiny invented records in the test suite only test edge cases.

The release sample is for checking file handling. It is not the 50,000-loan annual historical sample available through Clarity, and its origination and performance IDs don't match.

## 3. Run the pipeline

```powershell
.\.venv\Scripts\python.exe pipeline.py --orig data/raw/origination_sample_file.txt --perf data/raw/performance_sample_file.txt --output output/pilot
```

Read the code in this order:

1. `schema.py`: select fields by their documented positions and interpret status codes.
2. `pipeline.py`: read blocks, write Parquet, check quality, join origination metadata, and shuffle by loan ID.
3. `analysis.py`: sort each loan's timeline, count adjacent-month transitions, and build the event/censoring records.

A shuffle moves records between partitions. Without it, one loan could be split across workers and its transitions would be missed. Each shuffled partition still has to fit in worker memory; more machines do not fix an oversized partition automatically.

The script refuses to overwrite an existing output folder. Use `--output output/pilot-2` for your next run. Failed runs keep a report and partial outputs for inspection.

## 4. Inspect the report

```powershell
Get-Content output/pilot/run_report.json
Get-Content output/pilot/transitions.csv -TotalCount 12
Get-Content output/pilot/competing_risk_curve.csv -TotalCount 12
```

`transitions.csv` has the starting state, next month's state, observed count, and row-normalized probability. A row without observations has no estimated probability; the pipeline does not invent transitions for it. Repeated months from the same loan are correlated, so these frequencies are not independent borrower-level observations.

`competing_risk_curve.csv` tracks first-event cumulative incidence and survival. At each observed duration, survival plus the three event probabilities should sum to one. This is a descriptive baseline on the selected cohort, not an individual prediction.

Parquet folders contain typed origination fields as selected strings, normalized performance records, enriched timelines, and one survival record per retained loan. Raw dollar amounts and identifiers remain local.

Questions to answer before training anything:

- Why did the pipeline exclude one loan?
- Why can't the two sample files supply matched borrower covariates?
- How does a 30-days-late loan returning to current appear in the transition table?
- Why do we stop following a loan across a missing month?
- Why is paying off a loan different from censoring a still-active loan?

## 5. Exercise separate worker processes

```powershell
.\.venv\Scripts\python.exe pipeline.py --orig data/raw/origination_sample_file.txt --perf data/raw/performance_sample_file.txt --output output/workers --workers 2 --memory-limit "1 GiB" --partitions 4
```

This starts two Dask workers on your own machine. It is not a multi-machine benchmark. On a cluster, use `--scheduler tcp://YOUR-SCHEDULER:8786`; all workers need the same code, packages, and shared input/output paths. The current command-line file manifest expects filesystem paths, not cloud URLs.

## 6. Move to historical data

Open [Freddie Mac's dataset page](https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset), follow **Access Historical Data**, and sign in or register with Clarity. Review and accept the provider's terms yourself. Download one annual historical sample with both origination and performance files, then extract it locally.

Verify it uses the July 2026 layout: 31 origination fields and 35 performance fields, separated by pipes, without a header row. Earlier layouts need a separate adapter. Use matching files from a single release; combining refreshed copies duplicates loan-months.

For a downloaded 2010 historical sample in the current layout:

```powershell
.\.venv\Scripts\python.exe pipeline.py --orig "data/raw/sample_orig_2010.txt" --perf "data/raw/sample_perf_2010.txt" --dataset-kind historical --term 360 --output output/historical-2010 --workers 2 --partitions 32
```

Adjust the filenames to the files actually downloaded. `--term 360` requires matching origination data and keeps 30-year loans. Check missing joins and quarantine counts before interpreting results. Prefer scaling one vintage at a time, measure memory and disk usage, and then increase the workload.

## 7. Run the checks

```powershell
.\.venv\Scripts\python.exe -m unittest -v
```

Tests cover reporting gaps, duplicate months, event priority, missing-value sentinels, post-exit records, censoring, competing-risk arithmetic, and transitions across partitions.
