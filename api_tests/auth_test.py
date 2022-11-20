"""Test authorization"""
from conftest import get_detail, requires_env, auth_headers


@requires_env("API_TOKEN")
def test__compare_api_token(base_url, session):
    """should provide /alive without token"""
    resp = session.head(f'{base_url}/alive', headers={})
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__compare_api_token(base_url, session):
    """should compare token"""
    resp = session.get(f'{base_url}/info', headers=auth_headers('ABCB0001'))
    assert resp.status_code == 401
    assert get_detail(resp) == "Invalid token"


@requires_env("API_TOKEN")
def test__empty_token(base_url, session):
    """should use Bearer token"""
    resp = session.get(f'{base_url}/info', headers={'Authorization': ''})
    assert resp.status_code == 401
    assert get_detail(resp) == "No bearer token in request header"
