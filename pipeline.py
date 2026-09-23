"""Dask pipeline for Freddie Mac Standard files in the July 2026 layout."""
import argparse
from datetime import datetime, timezone
import glob
import importlib.metadata
import json
from pathlib import Path
import platform
import time
import dask
import dask.bag as db
import dask.dataframe as dd
import pandas as pd
from analysis import add_transitions, build_survival, empty_survival, competing_risk_curve
from schema import ORIG, PERF, parse_line


def read_raw(pattern, kind, blocksize):
    fields = ORIG if kind == 'orig' else PERF
    meta = {name: 'str' for name in fields.values()}
    if kind == 'perf':
        meta.update(month='int64', state='str', exit_date_mismatch='bool')
    return db.read_text(pattern, blocksize=blocksize).map(parse_line, kind=kind).to_dataframe(meta=meta)


def file_manifest(pattern):
    return [{'name': Path(p).name, 'bytes': Path(p).stat().st_size,
             'modified_ns': Path(p).stat().st_mtime_ns} for p in sorted(glob.glob(pattern))]


def run(args):
    started = time.perf_counter()
    root = Path(args.output)
    if root.exists():
        raise ValueError('Output already exists. Choose a new --output directory to preserve previous runs.')
    orig_files, perf_files = file_manifest(args.orig), file_manifest(args.perf)
    if not orig_files or not perf_files:
        raise ValueError('No matching input files. Extract the archives and check the --orig / --perf paths.')
    root.mkdir(parents=True)
    report = {'status': 'running', 'dataset_kind': args.dataset_kind,
              'created_utc': datetime.now(timezone.utc).isoformat(), 'layout': 'Freddie Mac July 2026 Standard',
              'origination_files': orig_files, 'performance_files': perf_files,
              'python': platform.python_version(), 'platform': platform.platform(),
              'versions': {x: importlib.metadata.version(x) for x in ['dask', 'pandas', 'pyarrow']},
              'partitions': args.partitions, 'blocksize': args.blocksize, 'term_filter': args.term,
              'scheduler': args.scheduler or (f'local distributed, {args.workers} workers' if args.workers else 'local threads'),
              'definition': 'first 90+ delinquency / REO / credit exit after first-observed-current landmark'}
    report_file = root / 'run_report.json'
    try:
        orig = read_raw(args.orig, 'orig', args.blocksize)
        perf = read_raw(args.perf, 'perf', args.blocksize)
        orig.to_parquet(str(root / 'origination'), write_index=False)
        perf.to_parquet(str(root / 'performance'), write_index=False)
        orig = dd.read_parquet(str(root / 'origination'))
        perf = dd.read_parquet(str(root / 'performance'))
        n_orig, n_perf, dup_orig, mismatches = dask.compute(
            orig.shape[0], perf.shape[0], (orig.groupby('loan_id').size() > 1).sum(),
            perf.exit_date_mismatch.sum())
        report.update(origination_rows=int(n_orig), performance_rows=int(n_perf), exit_date_mismatches=int(mismatches))
        if dup_orig:
            raise ValueError('Duplicate origination IDs. Do not combine overlapping data releases.')
        bad_timing = perf.groupby('loan_id').exit_date_mismatch.max().rename('quarantined').reset_index()
        perf = perf.merge(bad_timing, on='loan_id', how='left')
        report['quarantined_loans'] = int(bad_timing.quarantined.sum().compute())
        report['quarantined_performance_rows'] = int(perf.quarantined.sum().compute())
        perf = perf[~perf.quarantined].drop(columns='quarantined')
        orig['origination_matched'] = True
        joined = perf.merge(orig, on='loan_id', how='left')
        joined['origination_matched'] = joined.origination_matched.fillna(False).astype(bool)
        if args.term:
            joined = joined[joined.original_term == str(args.term)]
        report['selected_performance_rows'] = int(joined.shape[0].compute())
        if not report['selected_performance_rows']:
            raise ValueError('No rows remain after timing checks and term filter. The release format sample has no matching origination IDs; run it without --term.')
        # Explicit shuffle keeps every loan history in a single partition before shifts.
        joined = joined.shuffle(on='loan_id', npartitions=args.partitions)
        transition_meta = joined._meta.copy()
        transition_meta['next_state'] = pd.Series(dtype='str')
        transition_meta['valid_transition'] = pd.Series(dtype='bool')
        enriched = joined.map_partitions(add_transitions, meta=transition_meta)
        enriched.to_parquet(str(root / 'timelines'), write_index=False)
        # Re-shuffle on read: a Parquet reader may split or combine physical files.
        timelines = dd.read_parquet(str(root / 'timelines')).shuffle(on='loan_id', npartitions=args.partitions)
        pair_counts = timelines[timelines.valid_transition].groupby(['state', 'next_state']).size().compute()
        transitions = pair_counts.rename('count').reset_index()
        transitions['probability'] = transitions['count'] / transitions.groupby('state')['count'].transform('sum')
        transitions.to_csv(root / 'transitions.csv', index=False)
        survival = timelines.map_partitions(build_survival, meta=empty_survival())
        survival.to_parquet(str(root / 'survival'), write_index=False)
        cohort = dd.read_parquet(str(root / 'survival'))
        usable = cohort[cohort.eligible]
        counts = usable.groupby(['duration', 'event']).size().compute().rename('n').reset_index()
        competing_risk_curve(counts).to_csv(root / 'competing_risk_curve.csv', index=False)
        loans, eligible, reasons, missing_orig = dask.compute(
            cohort.shape[0], cohort.eligible.sum(), cohort.groupby('reason').size(),
            (~cohort.origination_matched).sum())
        report.update(status='complete', performance_loans=int(loans), survival_eligible_loans=int(eligible),
                      loans_without_origination=int(missing_orig), valid_monthly_pairs=int(transitions['count'].sum()),
                      endpoint_counts={str(k): int(v) for k, v in reasons.items()},
                      elapsed_seconds=round(time.perf_counter() - started, 3),
                      interpretation='Engineering validation only' if args.dataset_kind == 'format-sample' else 'Descriptive cohort estimates; not validated predictions')
    except Exception as error:
        report.update(status='failed', error=str(error), elapsed_seconds=round(time.perf_counter() - started, 3))
        raise
    finally:
        report_file.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--orig', required=True, help='Origination txt file or glob; one release only')
    parser.add_argument('--perf', required=True, help='Performance txt file or glob; one release only')
    parser.add_argument('--output', default='output/pilot')
    parser.add_argument('--dataset-kind', choices=['format-sample', 'historical'], default='format-sample')
    parser.add_argument('--partitions', type=int, default=4)
    parser.add_argument('--term', type=int, default=0, help='Original term in months (360 for 30-year); 0 includes all available terms')
    parser.add_argument('--blocksize', default='32 MiB')
    parser.add_argument('--scheduler', help='Optional existing Dask scheduler address')
    parser.add_argument('--workers', type=int, default=0, help='Start this many local Dask worker processes; 0 uses threads')
    parser.add_argument('--memory-limit', default='1 GiB', help='Per-worker memory limit when using --workers')
    args = parser.parse_args()
    if args.partitions < 1 or args.term < 0 or args.workers < 0:
        parser.error('--partitions must be positive; --term and --workers must be nonnegative')
    if args.scheduler and args.workers:
        parser.error('Choose --scheduler or --workers, not both')
    if args.scheduler:
        from distributed import Client
        with Client(args.scheduler):
            run(args)
    elif args.workers:
        from distributed import Client, LocalCluster
        with LocalCluster(n_workers=args.workers, threads_per_worker=1, memory_limit=args.memory_limit,
                          dashboard_address=None, local_directory='data/dask-spill') as cluster:
            with Client(cluster):
                run(args)
    else:
        with dask.config.set(scheduler='threads'):
            run(args)


if __name__ == '__main__':
    main()
