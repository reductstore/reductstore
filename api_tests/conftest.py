import json
import os
import random
import hashlib

import pytest
import requests


@pytest.fixture(name="base_url")
def _base_url() -> str:
    return f"http://{os.getenv('STORAGE_HOST', '127.0.0.1')}:8383"


@pytest.fixture(name='bucket_name')
def _gen_bucket_name() -> str:
    return f'bucket_{random.randint(0, 1000000)}'


def get_detail(resp) -> str:
    return json.loads(resp.content)["detail"]


@pytest.fixture(name='headers')
def _headers(base_url) -> dict:
    resp = requests.get(f'{base_url}/info')
    if resp.status_code == 200:
        # No JWT needed
        return {}
    elif resp.status_code == 401:
        hasher = hashlib.sha256(bytes(os.getenv("API_TOKEN"), 'utf-8'))
        resp = requests.post(f'{base_url}/auth/refresh', headers={'Authorization': f'Bearer {hasher.hexdigest()}'})
        if resp.status_code == 200:
            return {'Authorization': f'Bearer {resp.json()["access_token"]}'}

    raise RuntimeError(f'Failed to get access: {resp.content}')
