"""Test basic HTTP things"""
import requests


def test_allow_origin(base_url, headers):
    """Should allow origin in header"""
    headers['origin'] = 'http://test.com'
    resp = requests.get(f'{base_url}/info', headers=headers)

    assert resp.status_code == 200
    assert resp.headers['access-control-allow-origin'] == headers['origin']
