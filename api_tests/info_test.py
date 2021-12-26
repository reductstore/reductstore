import json
import requests


def test_ok(base_url):
    """Should provide information about the storage"""
    resp = requests.get(f'{base_url}/info/')

    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data['version'] == '0.1.0'
