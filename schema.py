"""Selected positions from Freddie Mac's July 2026 Standard Dataset layout.

Positions here are zero-based. Earlier releases and Fannie Mae are unsupported.
"""
import re

ORIG = {0: 'fico', 1: 'first_payment', 3: 'maturity', 9: 'dti',
        10: 'original_upb', 11: 'ltv', 12: 'original_rate', 16: 'property_state',
        19: 'loan_id', 21: 'original_term'}
PERF = {0: 'loan_id', 1: 'period', 2: 'upb', 3: 'delinquency',
        4: 'loan_age', 8: 'zero_code', 9: 'zero_date'}
TERMINAL = {'PAID_OFF', 'CREDIT_EXIT', 'OTHER_EXIT'}
DEFAULT_PROXY = {'D90_PLUS', 'REO', 'CREDIT_EXIT'}


def month_number(value):
    if not re.fullmatch(r'\d{6}', value) or not 1 <= int(value[4:]) <= 12:
        raise ValueError(f'Invalid YYYYMM: {value!r}')
    if not 1900 <= int(value[:4]) <= 2100:
        raise ValueError(f'Year outside supported range: {value!r}')
    return int(value[:4]) * 12 + int(value[4:]) - 1


def state_for(delinquency, zero_code):
    if zero_code == '01':
        return 'PAID_OFF'  # Includes maturity; not necessarily early prepayment.
    if zero_code in {'02', '03', '09'}:
        return 'CREDIT_EXIT'
    if zero_code in {'15', '16', '96'}:
        return 'OTHER_EXIT'
    if zero_code:
        raise ValueError(f'Unrecognized zero balance code: {zero_code!r}')
    if delinquency == 'RA':
        return 'REO'
    if delinquency in {'', 'XX'}:
        return 'UNKNOWN'
    if not re.fullmatch(r'\d{1,2}', delinquency):
        raise ValueError(f'Unrecognized delinquency status: {delinquency!r}')
    value = int(delinquency)
    return ['CURRENT', 'D30', 'D60'][value] if value < 3 else 'D90_PLUS'


def parse_line(line, kind):
    values = line.rstrip('\r\n').split('|')
    expected, fields = (31, ORIG) if kind == 'orig' else (35, PERF)
    if len(values) != expected:
        raise ValueError(f'{kind}: expected {expected} fields, found {len(values)}; check release and file type')
    record = {name: values[position].strip() for position, name in fields.items()}
    if not record['loan_id']:
        raise ValueError('Missing loan identifier')
    if kind == 'perf':
        record['month'] = month_number(record['period'])
        record['state'] = state_for(record['delinquency'], record['zero_code'])
        if record['zero_date']:
            month_number(record['zero_date'])
        record['exit_date_mismatch'] = bool(record['zero_code']) and record['zero_date'] != record['period']
    else:
        month_number(record['first_payment'])
        month_number(record['maturity'])
    return record
