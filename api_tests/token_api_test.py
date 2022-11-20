import json

import pytest
import secrets

from conftest import get_detail


@pytest.fixture(name="token_name")
def _make_random_token() -> str:
    return "token-" + secrets.token_urlsafe(32)


def test_create_token(base_url, session, token_name):
    """Should create a token"""
    permissions = {
        "full_access": True,
        "read": ["bucket1", "bucket2"],
        "write": ["bucket2"],
    }

    resp = session.post(f'{base_url}/tokens/{token_name}', json=permissions)
    assert resp.status_code == 200
    assert resp.headers['content-type'] == "application/json"
    assert "token-" in json.loads(resp.content)["value"]


def test_create_token_exist(base_url, session, token_name):
    """Should return 409 if a token already exists"""
    permissions = {}

    resp = session.post(f'{base_url}/tokens/{token_name}', json=permissions)
    assert resp.status_code == 200
    resp = session.post(f'{base_url}/tokens/{token_name}')
    assert resp.status_code == 409
    assert get_detail(resp) == f"Token '{token_name}' already exists"


def test_list_tokens(base_url, session, token_name):
    """Should list all tokens"""
    permissions = {}

    resp = session.post(f'{base_url}/tokens/{token_name}', json=permissions)
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/tokens/list')
    assert resp.status_code == 200
    assert resp.headers['content-type'] == "application/json"
    assert token_name in [t["name"] for t in json.loads(resp.content)["tokens"]]


def test_get_token(base_url, session, token_name):
    """Should show a token name and permissions"""
    permissions = {
        "full_access": True,
        "read": ["bucket1", "bucket2"],
        "write": ["bucket2"],
    }

    resp = session.post(f'{base_url}/tokens/{token_name}', json=permissions)
    assert resp.status_code == 200
    created_at = json.loads(resp.content)["created_at"]

    resp = session.get(f'{base_url}/tokens/{token_name}')
    assert resp.status_code == 200
    assert resp.headers['content-type'] == "application/json"
    assert json.loads(resp.content) == {
        "name": token_name,
        "created_at": created_at,
        "value": "",
        "permissions": permissions,
    }


def test_get_token_not_found(base_url, session):
    """Should return 404 if a token does not exist"""
    resp = session.get(f'{base_url}/tokens/token-not-found')
    assert resp.status_code == 404
    assert get_detail(resp) == "Token 'token-not-found' doesn't exist"


def test_delete_token(base_url, session, token_name):
    """Should delete a token"""
    permissions = {}

    resp = session.post(f'{base_url}/tokens/{token_name}', json=permissions)
    assert resp.status_code == 200

    resp = session.delete(f'{base_url}/tokens/{token_name}')
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/tokens/list')
    assert resp.status_code == 200
    assert token_name not in [t["name"] for t in json.loads(resp.content)["tokens"]]


def test_delete_token_not_found(base_url, session):
    """Should return 404 if a token does not exist"""
    resp = session.delete(f'{base_url}/tokens/token-not-found')
    assert resp.status_code == 404
    assert get_detail(resp) == "Token 'token-not-found' doesn't exist"
