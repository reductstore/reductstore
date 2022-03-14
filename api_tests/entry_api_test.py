import json

import pytest
import requests

from conftest import get_detail


@pytest.fixture(name='bucket')
def _make_bucket_and_return_name(base_url, headers, bucket_name) -> str:
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200
    return bucket_name


@pytest.mark.parametrize("data,ts", [(b"some_data_1", 100), (b"some_data_2", 60), (b"some_data_3", 1000)])
def test_read_write_entries_ok(base_url, headers, bucket, data, ts):
    """Should write few entries and read them back"""
    resp = requests.post(f'{base_url}/b/{bucket}/entry?ts={ts}', headers=headers, data=data)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket}/entry?ts={ts}', headers=headers)
    assert resp.status_code == 200
    assert resp.content == data


def test_read_write_entries_big_blob_ok(base_url, headers, bucket):
    """Should write and read files more than max block size"""
    huge_data = b"xaz" * 1024 * 1024
    ts = 1000
    resp = requests.post(f'{base_url}/b/{bucket}/entry?ts={ts}', headers=headers, data=huge_data)
    assert resp.status_code == 200

    resp = requests.post(f'{base_url}/b/{bucket}/entry?ts={ts + 100}', headers=headers, data=huge_data)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket}/entry?ts={ts}', headers=headers)
    assert resp.status_code == 200
    assert resp.content == huge_data


def test_read_no_bucket(base_url, headers):
    """Should return 404 if no bucket found"""
    resp = requests.get(f'{base_url}/b/xxx/entry?ts=100', headers=headers)
    assert resp.status_code == 404
    assert 'xxx' in get_detail(resp)


def test_read_no_data(base_url, headers, bucket):
    """Should return 404 if no data for ts"""
    resp = requests.get(f'{base_url}/b/{bucket}/entry?ts=100', headers=headers)
    assert resp.status_code == 404


def test_read_bad_ts(base_url, headers, bucket):
    """Should return 400 if ts is bad"""
    resp = requests.get(f'{base_url}/b/{bucket}/entry?ts=XXXX', headers=headers)
    assert resp.status_code == 422
    assert 'XXX' in get_detail(resp)


def test_write_no_bucket(base_url, headers):
    """Should return 404 if no bucket found"""
    resp = requests.post(f'{base_url}/b/xxx/entry?ts=100', headers=headers)
    assert resp.status_code == 404
    assert 'xxx' in get_detail(resp)


def test_write_bad_ts(base_url, headers, bucket):
    """Should return 422 if ts is bad"""
    resp = requests.post(f'{base_url}/b/{bucket}/entry?ts=XXXX', headers=headers)
    assert resp.status_code == 422
    assert 'XXX' in get_detail(resp)


def test_list_entry_ok(base_url, headers, bucket):
    """Should return list with timestamps and sizes"""
    ts = 1000
    resp = requests.post(f'{base_url}/b/{bucket}/entry?ts={ts}', headers=headers, data="some_data")
    assert resp.status_code == 200

    resp = requests.post(f'{base_url}/b/{bucket}/entry?ts={ts + 100}', headers=headers, data="some_data")
    assert resp.status_code == 200

    resp = requests.post(f'{base_url}/b/{bucket}/entry?ts={ts + 200}', headers=headers, data="some_data")
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket}/entry/list?start={ts}&stop={ts + 200}', headers=headers)
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert "records" in data
    assert data["records"][0] == {'ts': '1000', 'size': '11'}
    assert data["records"][1] == {'ts': '1100', 'size': '11'}


def test_list_entry_no_data(base_url, headers, bucket):
    """Should return 404 if no data for request"""
    resp = requests.get(f'{base_url}/b/{bucket}/entry/list?start=100&stop=200', headers=headers)
    assert resp.status_code == 404


def test_latest_record(base_url, headers, bucket):
    """Should return the latest record"""
    ts = 1000
    resp = requests.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 404

    requests.post(f'{base_url}/b/{bucket}/entry?ts={ts}', headers=headers, data="some_data1")
    requests.post(f'{base_url}/b/{bucket}/entry?ts={ts+10}', headers=headers, data="some_data2")

    resp = requests.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 200
    assert resp.content == b"some_data2"
