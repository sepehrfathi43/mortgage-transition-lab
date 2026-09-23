# Mortgage Transition Lab

When a mortgage falls a month behind, what happens next? Some borrowers catch up. Others miss another payment. Some loans leave the dataset because they are paid off, sold, or liquidated.

I'm building this project to follow those paths through monthly loan records. It starts with Freddie Mac's public release sample and a Dask pipeline, then moves toward default and payoff timing models on a larger historical cohort.

## What works now

- Read the **July 2026 Freddie Mac Standard layout**, validate record widths and status codes, and write Parquet.
- Keep each loan's history together across Dask partitions before calculating monthly transitions.
- Estimate one-month transition frequencies, including cures from delinquency back to current.
- Build a survival cohort and an Aalen-Johansen competing-risk baseline.
- Flag unmatched origination records, reject duplicate loan-months, and exclude loans with inconsistent exit timing.

Fannie Mae has a different layout and is not supported by this parser. DeepSurv training is a later stage; this repository does not yet contain a trained neural model or measured predictive accuracy.

## Run the first lesson

Use Python 3.12. From the project folder:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe download_sample.py
.\.venv\Scripts\python.exe pipeline.py --orig data/raw/origination_sample_file.txt --perf data/raw/performance_sample_file.txt --output output/pilot
.\.venv\Scripts\python.exe -m unittest -v
```

If Windows recognizes `py` instead of `python`, use `py -3.12` for the first command. [WALKTHROUGH.md](WALKTHROUGH.md) explains what each step does and how to inspect the results.

## What the sample tells us

The official Release 47 format sample contains 1,000 origination rows and 1,011 performance rows. The two files have no matching loan IDs. One loan also has an exit date that disagrees with its reporting period; the pipeline excludes that loan from analysis.

That makes this a useful engineering check, not a suitable training dataset. A successful sample run means the code handles these cases. It does not establish mortgage default rates or prove that the pipeline scales to billions of rows.

## A few choices that matter

**Default needs a definition.** The first model target is a serious-delinquency proxy: first 90+ days past due, REO, or a credit disposition. That is not the same thing as foreclosure. The transition table still allows a seriously delinquent loan to cure; the survival target stops at its first qualifying event.

**Payoff is a competing outcome.** The source combines voluntary prepayment and maturity in code 01. This project calls that outcome `PAID_OFF`. It doesn't silently treat every payoff as early refinancing. Sales and other removals form a separate competing outcome.

**Time starts when we can observe it.** The current survival cohort starts at a loan's first observed month, requires it to be current then, and follows it until an event or censoring. It measures time since that landmark, not since origination. Missing months or unknown statuses stop follow-up. The sample run includes all terms; use `--term 360` on matched historical files for a 30-year cohort.

**The output is a probability curve.** Even a good survival model cannot tell us the exact month an individual borrower will default. The baseline estimates how cumulative event probabilities change over observed follow-up.

## Next: a matched historical cohort

Download one vintage's origination and performance files from Freddie Mac Clarity, using the same release. Start with the official historical sample before full-quarter files. That sample is separate from the small format sample used here.

Then compare a nonparametric baseline, cause-specific Cox models, and DeepSurv. Hold out whole loans and later entry cohorts, restrict training outcomes to the training cutoff, and assess calibration as well as ranking. [MODELING.md](MODELING.md) records the target definitions, assumptions, and evaluation plan.

## Sources and data access

- [Freddie Mac dataset and access instructions](https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset)
- [July 2026 user guide](https://www.freddiemac.com/fmac-resources/research/pdf/general_user_guide_july_2026.pdf)
- [July 2026 file headers](https://www.freddiemac.com/fmac-resources/research/docs/file_headers_july_2026.zip)
- [Dask shuffle documentation](https://docs.dask.org/en/stable/dataframe-groupby.html)

Freddie Mac and Fannie Mae are government-sponsored enterprises. Their data is publicly available under access terms; it is not an unrestricted government public-domain dataset. Historical downloads require registration. Check the provider's terms before using or sharing results. Loan records and generated outputs stay out of this repository.
