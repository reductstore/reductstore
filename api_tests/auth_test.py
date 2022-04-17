"""Test authorization"""
import os

import pytest
import requests

from conftest import get_detail


def requires_env(key):
    env = os.environ.get(key)

    return pytest.mark.skipif(
        env is None,
        reason=f"Not suitable environment {key} for current test"
    )


@requires_env("API_TOKEN")
def test__compare_api_token(base_url):
    """should compare token"""
    resp = requests.get(f'{base_url}/info', headers={'Authorization': 'Bearer ABCB0001'})
    assert resp.status_code == 401
    assert get_detail(resp) == "Invalid token"


@requires_env("API_TOKEN")
def test__bad_api_token(base_url):
    """should decode token"""
    resp = requests.get(f'{base_url}/info', headers={'Authorization': 'Bearer ITISNOTHEX'})
    assert resp.status_code == 401
    assert get_detail(resp) == "Invalid token"


@requires_env("API_TOKEN")
def test__empty_token(base_url):
    """should use Bearer token"""
    resp = requests.get(f'{base_url}/info', headers={'Authorization': ''})
    assert resp.status_code == 401
    assert get_detail(resp) == "No bearer token in response header"
