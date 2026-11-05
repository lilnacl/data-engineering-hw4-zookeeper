import json
import os
import re
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
WORK = Path(os.getenv('HW_WORK', str(ROOT / 'work')))
BUCKET = 'hh-lake'
KEY = 'raw/it_vacancies_2024.csv'
GO_NAME = re.compile(r'(^|[^a-zа-яё0-9])(go|golang)([^a-zа-яё0-9]|$)', re.I)


def normalize_skills(value: str) -> tuple[str, ...]:
    skills = {s.strip().lower() for s in (value or '').split(';') if s.strip()}
    return tuple(sorted({'go' if s == 'golang' else s for s in skills}))


def is_variant(row: dict) -> bool:
    return row.get('role') == 'Программист, разработчик' and (
        bool(GO_NAME.search(row.get('name') or '')) or 'go' in normalize_skills(row.get('key_skills'))
    )


def credentials() -> dict:
    return json.loads((WORK / '.credentials.json').read_text())


def s3_client():
    c = credentials()
    return boto3.client('s3', endpoint_url=c['endpoint'], aws_access_key_id=c['access'],
                        aws_secret_access_key=c['secret'], region_name='us-east-1',
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                                      request_checksum_calculation='when_required',
                                      response_checksum_validation='when_required'))


def dump(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
