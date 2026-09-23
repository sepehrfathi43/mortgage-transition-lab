# Modeling notes

## Scope of the first implementation

The transition estimator and survival baseline are descriptive. They use observed histories to summarize a cohort. There is no train/test split or predictive performance claim at this stage. No borrower credit decision is produced.

Transition states are CURRENT, D30, D60, D90_PLUS, REO, PAID_OFF, CREDIT_EXIT, OTHER_EXIT, and UNKNOWN. Terminal codes take precedence over delinquency on the same record. Unknown status is not current. Count only consecutive calendar months, and never count a transition after a terminal exit. REO is not merged into a generic 90+ category in the transition table.

The reported matrix is an empirical one-step Markov baseline. It doesn't prove the Markov assumption: calendar conditions, loan age, assistance plans, and prior delinquency can matter. Multi-step matrix powers and homogeneous lifetime forecasts are not implemented.

For survival, require CURRENT at the first observed month and at least one subsequent usable month. Use the first 90+ delinquency, REO, or credit disposition as event 1; payoff including maturity as event 2; other removal as event 3. Stop at the first event. Record censoring at the last continuous known month before a gap, unknown status, or file end. Exclude the entire loan if a reported exit month disagrees with its effective date rather than silently moving the event.

This is a first-observed-current landmark cohort. It does not prove that a borrower has never defaulted before observation, and it is not an origination-to-default cohort. Baseline covariates come from origination, while entry loan age records seasoning. Unknown numeric values remain missing. No loss amounts, recoveries, future balances, or future modifications become predictor inputs.

## Why competing risks

Payoff prevents a later default on the same loan. Treating payoff as ordinary censoring and presenting one minus a default Kaplan-Meier curve as actual default probability would answer the wrong question. The implemented Aalen-Johansen baseline keeps the event causes separate and sums their cumulative incidence with event-free survival to one. At tied monthly durations, events are counted before censoring. Independent censoring is an assumption; gaps and unknown statuses can violate it, so the reasons are reported.

## DeepSurv stage

DeepSurv extends the Cox proportional-hazards framework with a neural risk score. A default-only risk score does not itself supply payoff probabilities or an exact event month. For this project, the next comparison would use cause-specific Cox/DeepSurv models for each event and combine their hazards into cumulative incidence curves. Proportional hazards and baseline hazard estimation still need attention.

Before implementing training:

1. Acquire a matched historical cohort and agree on whether the target is serious delinquency, foreclosure/credit disposition, or another definition.
2. Use all observations needed to determine entry eligibility without exposing future outcomes to features. Censor training labels at the training calendar cutoff.
3. Split whole loans into chronological entry cohorts; keep all months for a loan in one split. Evaluate the same targets over comparable follow-up horizons. A retrospective data refresh is not a point-in-time production backtest.
4. Fit imputation and scaling only on training data. Examine state and missingness shifts across vintages, including the reporting change around October 2025.
5. Fit simple baselines first. Evaluate competing-risk calibration at fixed horizons, IPCW Brier scores where support permits, and ranking with a censoring-aware metric. Bootstrap by loan, not by loan-month.
6. Report how many loans remain at risk at each horizon. Do not claim a 360-month forecast when the cohort has no follow-up support that far out.

## Scaling evidence to collect

Record source release and archive hashes, input bytes, rows, unique loans, partition counts, worker count, CPU/RAM, elapsed time, peak memory, spill, and shuffle volume. The sample downloader hashes its archive; large local inputs currently have filenames, sizes, and modification times recorded, not content hashes. Preserve original archives separately.

The current pipeline accepts blocks and a distributed scheduler but is only validated on a small sample. Some loan-level processing uses Python loops within pandas partitions. Profile that step before claiming high throughput, and tune partitions so individual workers can hold them. Avoid collecting the full loan-month table into pandas. Only small aggregate count tables are collected by this implementation.

## References

- [Freddie Mac user guide](https://www.freddiemac.com/fmac-resources/research/pdf/general_user_guide_july_2026.pdf)
- [DeepSurv paper](https://doi.org/10.1186/s12874-018-0482-1)
- [Dask shuffling](https://docs.dask.org/en/stable/dataframe-groupby.html)
