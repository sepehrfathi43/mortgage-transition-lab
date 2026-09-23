"""Tiny invented cases test logic only; research inputs come from Freddie Mac."""
import unittest
import argparse
import contextlib
import io
import json
from pathlib import Path
import tempfile
import pandas as pd
import dask
import dask.dataframe as dd
from schema import state_for, month_number, parse_line
from analysis import add_transitions, build_survival, competing_risk_curve
from pipeline import run


def timeline(states, months=None, loan='test-loan'):
    months = months or list(range(24000, 24000 + len(states)))
    return pd.DataFrame({'loan_id': [loan] * len(states), 'month': months, 'state': states,
                         'loan_age': ['1'] * len(states), 'origination_matched': [True] * len(states),
                         'fico': ['720'] * len(states), 'ltv': ['80'] * len(states),
                         'dti': ['35'] * len(states), 'original_rate': ['4.5'] * len(states),
                         'original_term': ['360'] * len(states)})


class TimelineTests(unittest.TestCase):
    def test_states_and_missing_values(self):
        self.assertEqual(state_for('00', ''), 'CURRENT')
        self.assertEqual(state_for('01', ''), 'D30')
        self.assertEqual(state_for('02', ''), 'D60')
        self.assertEqual(state_for('99', ''), 'D90_PLUS')
        self.assertEqual(state_for('RA', ''), 'REO')
        self.assertEqual(state_for('XX', ''), 'UNKNOWN')

    def test_exit_precedence_and_other_removals(self):
        self.assertEqual(state_for('03', '01'), 'PAID_OFF')
        self.assertEqual(state_for('00', '09'), 'CREDIT_EXIT')
        self.assertEqual(state_for('00', '16'), 'OTHER_EXIT')
        with self.assertRaises(ValueError):
            state_for('00', '88')

    def test_month_rollover_and_bad_dates(self):
        self.assertEqual(month_number('202501') - month_number('202412'), 1)
        with self.assertRaises(ValueError):
            month_number('202513')

    def test_schema_rejects_wrong_width(self):
        with self.assertRaises(ValueError):
            parse_line('a|b|c', 'perf')

    def test_no_transition_over_gap(self):
        result = add_transitions(timeline(['CURRENT', 'D30'], [24000, 24002]))
        self.assertEqual(result.valid_transition.sum(), 0)

    def test_no_transitions_after_payoff(self):
        result = add_transitions(timeline(['CURRENT', 'PAID_OFF', 'CURRENT', 'D30']))
        self.assertEqual(result.valid_transition.tolist(), [True, False, False, False])

    def test_duplicate_month_fails(self):
        with self.assertRaises(ValueError):
            add_transitions(timeline(['CURRENT', 'D30'], [24000, 24000]))

    def test_first_default_stops_followup(self):
        result = build_survival(timeline(['CURRENT', 'D30', 'D90_PLUS', 'CURRENT', 'PAID_OFF'])).iloc[0]
        self.assertEqual((result.event, result.duration), (1, 2))

    def test_gap_censors_at_last_observed_month(self):
        result = build_survival(timeline(['CURRENT', 'D30', 'D90_PLUS'], [24000, 24001, 24003])).iloc[0]
        self.assertEqual((result.event, result.duration, result.reason), (0, 1, 'gap'))

    def test_unknown_does_not_become_current(self):
        result = build_survival(timeline(['CURRENT', 'D30', 'UNKNOWN', 'D90_PLUS'])).iloc[0]
        self.assertEqual((result.event, result.duration, result.reason), (0, 1, 'unknown_status'))

    def test_prevalent_delinquency_excluded(self):
        result = build_survival(timeline(['D30', 'CURRENT', 'D90_PLUS'])).iloc[0]
        self.assertFalse(result.eligible)

    def test_other_removal_is_separate_competing_event(self):
        result = build_survival(timeline(['CURRENT', 'OTHER_EXIT'])).iloc[0]
        self.assertEqual(result.event, 3)

    def test_missing_numeric_sentinels(self):
        data = timeline(['CURRENT', 'PAID_OFF'])
        data['fico'], data['ltv'], data['dti'] = '9999', '999', '999'
        result = build_survival(data).iloc[0]
        self.assertTrue(pd.isna(result.fico) and pd.isna(result.ltv) and pd.isna(result.dti))

    def test_competing_incidence_and_censor_ties(self):
        counts = pd.DataFrame({'duration': [1, 1, 2], 'event': [1, 0, 2], 'n': [1, 1, 1]})
        result = competing_risk_curve(counts)
        self.assertAlmostEqual(result.iloc[-1].default_cif, 1 / 3)
        self.assertAlmostEqual(result.iloc[-1].payoff_cif, 2 / 3)
        self.assertAlmostEqual(result.iloc[-1].survival, 0)
        self.assertTrue(((result[['default_cif', 'payoff_cif', 'other_cif']].sum(axis=1) + result.survival - 1).abs() < 1e-10).all())

    def test_shuffle_keeps_cross_partition_transitions(self):
        frame = pd.concat([timeline(['CURRENT', 'D30', 'CURRENT'], loan='a'),
                           timeline(['CURRENT', 'D60', 'D90_PLUS'], loan='b')], ignore_index=True)
        data = dd.from_pandas(frame.sample(frac=1, random_state=7), npartitions=4, sort=False)
        meta = add_transitions(frame.iloc[:0])
        with dask.config.set(scheduler='threads'):
            result = data.shuffle('loan_id', npartitions=3).map_partitions(add_transitions, meta=meta).compute()
        self.assertEqual(result.valid_transition.sum(), 4)

    def test_pipeline_quarantines_exit_dates_and_filters_term(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            originals, monthly = [], []
            for loan, term in [('good', '360'), ('bad-date', '360'), ('short-term', '180')]:
                row = [''] * 31
                for pos, value in {0: '720', 1: '200001', 3: '203001', 9: '35', 10: '100000',
                                   11: '80', 12: '4.5', 16: 'NY', 19: loan, 21: term}.items():
                    row[pos] = value
                originals.append('|'.join(row))
                for month in ('200001', '200002'):
                    row = [''] * 35
                    for pos, value in {0: loan, 1: month, 2: '100000', 3: '00', 4: '1'}.items():
                        row[pos] = value
                    if month == '200002':
                        row[8], row[9] = '01', '200003' if loan == 'bad-date' else month
                    monthly.append('|'.join(row))
            (root / 'orig.txt').write_text('\n'.join(originals) + '\n', encoding='utf-8')
            (root / 'perf.txt').write_text('\n'.join(monthly) + '\n', encoding='utf-8')
            args = argparse.Namespace(orig=str(root / 'orig.txt'), perf=str(root / 'perf.txt'),
                                      output=str(root / 'results'), dataset_kind='format-sample',
                                      partitions=3, blocksize='128 B', scheduler=None, workers=0, term=360)
            with dask.config.set(scheduler='threads'), contextlib.redirect_stdout(io.StringIO()):
                run(args)
            report = json.loads((root / 'results/run_report.json').read_text())
            self.assertEqual(report['quarantined_loans'], 1)
            self.assertEqual(report['performance_loans'], 1)
            self.assertEqual(report['valid_monthly_pairs'], 1)
            records = pd.read_parquet(root / 'results/survival')
            self.assertEqual(records.iloc[0].event, 2)
            self.assertEqual(records.iloc[0].fico, 720)


if __name__ == '__main__':
    unittest.main()
