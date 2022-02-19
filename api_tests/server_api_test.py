import json

import requests


def test_get_info(base_url):
    """Should provide information about the storage"""
    resp = requests.get(f'{base_url}/info')

    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data['version'] == '0.2.0'
    assert int(data['bucket_count']) > 0
    assert int(data['uptime']) >= 0
    assert int(data['latest_record']) >= 0
    assert int(data['oldest_record']) >= 0


def test_get_list_of_buckets(base_url):
    """Should provide information about the storage"""
    resp = requests.get(f'{base_url}/list')

    assert resp.status_code == 200
    data = json.loads(resp.content)

    assert len(data["buckets"]) > 0
    assert "bucket_" in data["buckets"][0]["name"]
    assert int(data["buckets"][0]["size"]) >= 0
    assert int(data["buckets"][0]["entry_count"]) >= 0
    assert int(data["buckets"][0]["oldest_record"]) >= 0
    assert int(data["buckets"][0]["latest_record"]) >= 0


def test_get_wrong_path(base_url):
    """Should return 404"""

    resp = requests.get(f'{base_url}/')
    assert resp.status_code == 404

    resp = requests.get(f'{base_url}/NOTEXIST')
    assert resp.status_code == 404

    resp = requests.get(f'{base_url}/NOTEXIST/XXXX')
    assert resp.status_code == 404
