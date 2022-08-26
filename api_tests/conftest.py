import json
import os
import random

import pytest
import requests


@pytest.fixture(name="base_url")
def _base_url() -> str:
    return f"{os.getenv('STORAGE_URL', 'http://127.0.0.1:8383')}"


@pytest.fixture(name='bucket_name')
def _gen_bucket_name() -> str:
    return f'bucket_{random.randint(0, 1000000)}'


def get_detail(resp) -> str:
    return json.loads(resp.content)["detail"]


@pytest.fixture(name="session")
def _session(base_url):
    session = requests.session()
    session.verify = False
    session.trust_env = False
    session.headers['Authorization'] = f'Bearer {os.getenv("API_TOKEN")}'
    return session
