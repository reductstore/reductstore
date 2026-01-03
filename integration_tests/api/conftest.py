import json
import os
import random
import secrets
from collections import namedtuple
from typing import Callable

import pytest
import requests


@pytest.fixture(name="storage_url")
def _storage_url():
    return os.environ.get("STORAGE_URL", "http://127.0.0.1:8383")


@pytest.fixture(name="base_url")
def _base_url(storage_url) -> str:
    return f"{storage_url}/api/v1"


@pytest.fixture(name="bucket_name")
def _gen_bucket_name() -> str:
    return f"bucket_{random.randint(0, 10000000)}"


@pytest.fixture(name="replication_name")
def _gen_replication_name() -> str:
    return f"replication_{random.randint(0, 10000000)}"


def requires_env(key):
    env = os.environ.get(key)

    return pytest.mark.skipif(
        env is None or env == "",
        reason=f"Not suitable environment {key} for current test",
    )


def requires_backend(*backends):
    backend = os.environ.get("STORAGE_BACKEND", "fs")
    return pytest.mark.skipif(
        backend not in backends,
        reason=f"Not suitable backend {backend} for current test",
    )


def auth_headers(token):
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(name="bucket")
def _make_bucket_and_return_name(base_url, session, bucket_name) -> str:
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200
    return bucket_name


@pytest.fixture(name="session")
def _session(base_url):
    session = requests.session()
    session.verify = False
    session.trust_env = False
    session.headers["Authorization"] = f'Bearer {os.getenv("API_TOKEN")}'
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
    name = token_generator()
    resp = session.post(f"{base_url}/tokens/{name}", json=permissions)
    assert resp.status_code == 200
    return namedtuple("Token", ["name", "value"])(
        name, json.loads(resp.content)["value"]
    )


@pytest.fixture(name="token_read_bucket")
def _make_token_read_bucket(session, base_url, bucket_name, token_generator):
    session.post(f"{base_url}/b/{bucket_name}")
    permissions = {
        "full_access": False,
        "read": [bucket_name],
    }
    name = token_generator()
    resp = session.post(f"{base_url}/tokens/{name}", json=permissions)
    assert resp.status_code == 200
    return namedtuple("Token", ["name", "value"])(
        name, json.loads(resp.content)["value"]
    )


@pytest.fixture(name="token_write_bucket")
def _make_token_write_bucket(session, base_url, bucket_name, token_generator):
    session.post(f"{base_url}/b/{bucket_name}")

    permissions = {
        "full_access": False,
        "write": [bucket_name],
    }
    name = token_generator()
    resp = session.post(f"{base_url}/tokens/{name}", json=permissions)
    assert resp.status_code == 200
    return namedtuple("Token", ["name", "value"])(
        name, json.loads(resp.content)["value"]
    )
