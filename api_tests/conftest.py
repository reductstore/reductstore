import json
import os
import random
import secrets
from typing import Callable

import pytest
import requests


@pytest.fixture(name="storage_url")
def _storage_url():
    return os.environ.get("STORAGE_URL", 'http://127.0.0.1:8383')


@pytest.fixture(name="base_url")
def _base_url(storage_url) -> str:
    return f"{storage_url}/api/v1"


@pytest.fixture(name='bucket_name')
def _gen_bucket_name() -> str:
    return f'bucket_{random.randint(0, 1000000)}'


def get_detail(resp) -> str:
    return json.loads(resp.content)["detail"]


def requires_env(key):
    env = os.environ.get(key)

    return pytest.mark.skipif(
        env is None or env == "",
        reason=f"Not suitable environment {key} for current test"
    )


def auth_headers(token):
    return {'Authorization': f'Bearer {token}'}


@pytest.fixture(name="session")
def _session(base_url):
    session = requests.session()
    session.verify = False
    session.trust_env = False
    session.headers['Authorization'] = f'Bearer {os.getenv("API_TOKEN")}'
    return session


@pytest.fixture(name="token_generator")
def _token_generator() -> Callable[[], str]:
    def _gen():
        return "token-" + secrets.token_urlsafe(32)

    return _gen


@pytest.fixture(name="token_name")
def _make_random_token(token_generator) -> str:
    return token_generator()


@pytest.fixture(name="token_without_permissions")
def _make_token_permissions(session, base_url, token_generator):
    permissions = {
        "full_access": False,
    }
    resp = session.post(f'{base_url}/tokens/{token_generator()}', json=permissions)
    assert resp.status_code == 200
    return json.loads(resp.content)["value"]


@pytest.fixture(name="token_read_bucket")
def _make_token_read_bucket(session, base_url, bucket_name, token_generator):
    permissions = {
        "full_access": False,
        "read": [bucket_name],
    }
    resp = session.post(f'{base_url}/tokens/{token_generator()}', json=permissions)
    assert resp.status_code == 200
    return json.loads(resp.content)["value"]


@pytest.fixture(name="token_write_bucket")
def _make_token_write_bucket(session, base_url, bucket_name, token_generator):
    permissions = {
        "full_access": False,
        "write": [bucket_name],
    }
    resp = session.post(f'{base_url}/tokens/{token_generator()}', json=permissions)
    assert resp.status_code == 200
    return json.loads(resp.content)["value"]
