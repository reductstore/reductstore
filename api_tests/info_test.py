import json
import requests


def test_get_info(base_url):
    """Should provide information about the storage"""
    resp = requests.get(f'{base_url}/info')

    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data['version'] == '0.2.0'
    assert data['bucket_count'] > 0


def test_get_wrong_path(base_url):
    """Should return 404"""

    resp = requests.get(f'{base_url}/')
    assert resp.status_code == 404

    resp = requests.get(f'{base_url}/NOTEXIST')
    assert resp.status_code == 404

    resp = requests.get(f'{base_url}/NOTEXIST/XXXX')
    assert resp.status_code == 404
