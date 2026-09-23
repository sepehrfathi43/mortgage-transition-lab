"""Fetch Freddie Mac's public Release 47 format sample, not the historical sample."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.request import Request, urlopen
from zipfile import ZipFile

URL = 'https://www.freddiemac.com/fmac-resources/research/docs/release-47-sample-files.zip'


def main():
    root = Path('data')
    (root / 'reference').mkdir(parents=True, exist_ok=True)
    (root / 'raw').mkdir(exist_ok=True)
    archive = root / 'reference/release-47-sample-files.zip'
    if not archive.exists():
        with urlopen(Request(URL, headers={'User-Agent': 'mortgage-transition-lab/0.1'}), timeout=60) as response:
            payload = response.read(10_000_001)
        if len(payload) > 10_000_000:
            raise ValueError('Sample exceeds expected size; check source before downloading')
        temporary = archive.with_suffix('.part')
        temporary.write_bytes(payload)
        temporary.replace(archive)
    with ZipFile(archive) as zipped:
        for name in ('origination_sample_file.txt', 'performance_sample_file.txt'):
            member = 'Sample Files/' + name
            if zipped.getinfo(member).file_size > 10_000_000:
                raise ValueError('Unexpected sample member size')
            (root / 'raw' / name).write_bytes(zipped.read(member))
    receipt = {'url': URL, 'checked_at_utc': datetime.now(timezone.utc).isoformat(),
               'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
               'purpose': 'Release 47 format validation only; not a representative research cohort'}
    (root / 'reference/sample_receipt.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    print('Official format sample saved in data/raw. See data/reference/sample_receipt.json.')


if __name__ == '__main__':
    main()
