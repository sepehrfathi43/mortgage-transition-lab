"""Loan-level timeline rules and a nonparametric competing-risk baseline."""
import pandas as pd
from schema import TERMINAL, DEFAULT_PROXY

SURVIVAL_TYPES = {
    'loan_id': 'str', 'entry_month': 'int64', 'duration': 'int64', 'event': 'int64',
    'reason': 'str', 'eligible': 'bool', 'entry_loan_age': 'str',
    'fico': 'float64', 'ltv': 'float64', 'dti': 'float64', 'original_rate': 'float64',
    'original_term': 'float64', 'origination_matched': 'bool'}


def empty_survival():
    return pd.DataFrame({k: pd.Series(dtype=v) for k, v in SURVIVAL_TYPES.items()})


def add_transitions(frame):
    frame = frame.sort_values(['loan_id', 'month']).copy()
    if frame.duplicated(['loan_id', 'month']).any():
        raise ValueError('Duplicate loan-month: use one consistent release, not overlapping refreshes')
    if frame.empty:
        frame['next_state'] = pd.Series(dtype='str')
        frame['valid_transition'] = pd.Series(dtype='bool')
        return frame
    groups = frame.groupby('loan_id', sort=False)
    frame['next_state'] = groups['state'].shift(-1).fillna('END')
    next_month = groups['month'].shift(-1)
    terminal = frame['state'].isin(TERMINAL)
    # Any row after the first termination is outside this loan's transition risk set.
    seen_terminal = terminal.groupby(frame['loan_id']).cumsum()
    frame['valid_transition'] = (
        (next_month - frame['month'] == 1) & (seen_terminal == 0)
        & (frame['state'] != 'UNKNOWN') & (~frame['next_state'].isin(['UNKNOWN', 'END']))
    )
    return frame


def numeric(value, low, high):
    try:
        number = float(value)
        return number if low <= number <= high else float('nan')
    except (ValueError, TypeError):
        return float('nan')


def build_survival(frame):
    """First-observed-current landmark; stop at the first gap, unknown, or event.

    Events: 0 censored, 1 first 90+ / REO / credit disposition proxy,
    2 voluntary payoff including maturity, 3 other removal.
    """
    rows = []
    for loan_id, loan in frame.groupby('loan_id', sort=False):
        loan = loan.sort_values('month')
        first = loan.iloc[0]
        row = {'loan_id': loan_id, 'entry_month': int(first['month']), 'duration': 0,
               'event': 0, 'reason': 'insufficient_followup', 'eligible': False,
               'entry_loan_age': first['loan_age'],
               'fico': numeric(first.get('fico'), 300, 850),
               'ltv': numeric(first.get('ltv'), 1, 998),
               'dti': numeric(first.get('dti'), 0, 65),
               'original_rate': numeric(first.get('original_rate'), 0, 30),
               'original_term': numeric(first.get('original_term'), 1, 600),
               'origination_matched': bool(first['origination_matched'])}
        if first['state'] != 'CURRENT':
            row['reason'] = 'not_current_at_entry'
            rows.append(row)
            continue
        previous = int(first['month'])
        for record in loan.iloc[1:].to_dict('records'):
            if int(record['month']) != previous + 1:
                row['reason'] = 'gap'
                break
            if record['state'] == 'UNKNOWN':
                row['reason'] = 'unknown_status'
                break
            row['duration'] = int(record['month']) - row['entry_month']
            row['reason'] = 'last_observation'
            row['eligible'] = True
            previous = int(record['month'])
            if record['state'] in DEFAULT_PROXY:
                row.update(event=1, reason='default_proxy')
                break
            if record['state'] == 'PAID_OFF':
                row.update(event=2, reason='payoff_or_maturity')
                break
            if record['state'] == 'OTHER_EXIT':
                row.update(event=3, reason='other_removal')
                break
        rows.append(row)
    return pd.DataFrame(rows).astype(SURVIVAL_TYPES) if rows else empty_survival()


def competing_risk_curve(counts):
    """Aalen-Johansen for three first-event causes, from duration/event counts.

    At tied times events occur before censoring. No learned covariates here.
    """
    if counts.empty:
        return pd.DataFrame(columns=['month', 'at_risk', 'survival', 'default_cif', 'payoff_cif', 'other_cif'])
    risk = int(counts['n'].sum())
    survival, incidence, result = 1.0, [0.0, 0.0, 0.0], []
    for duration, group in counts.groupby('duration', sort=True):
        deaths = [int(group.loc[group.event == cause, 'n'].sum()) for cause in (1, 2, 3)]
        for cause in range(3):
            incidence[cause] += survival * deaths[cause] / risk
        survival *= 1 - sum(deaths) / risk
        result.append({'month': int(duration), 'at_risk': risk, 'survival': survival,
                       'default_cif': incidence[0], 'payoff_cif': incidence[1], 'other_cif': incidence[2]})
        risk -= int(group['n'].sum())
    return pd.DataFrame(result)
