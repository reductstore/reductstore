import pytest
import requests
from conftest import get_detail


@pytest.fixture(name='bucket')
def _make_bucket_and_return_name(base_url, bucket_name) -> str:
    resp = requests.post(f'{base_url}/{bucket_name}')
    assert resp.status_code == 200
    return bucket_name


@pytest.mark.parametrize("data,ts", [(b"some_data_1", 100), (b"some_data_2", 60), (b"some_data_3", 1000)])
def test_read_write_entries_ok(base_url, bucket, data, ts):
    """Should write few entries and read them back"""
    resp = requests.post(f'{base_url}/{bucket}/entry?ts={ts}', data=data)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/{bucket}/entry?ts={ts}')
    assert resp.status_code == 200
    assert resp.content == data


def test_read_write_entries_big_blob_ok(base_url, bucket):
    """Should write and read files more than max block size"""
    huge_data = b"xaz" * 1024 * 1024
    ts = 1000
    resp = requests.post(f'{base_url}/{bucket}/entry?ts={ts}', huge_data)
    assert resp.status_code == 200

    resp = requests.post(f'{base_url}/{bucket}/entry?ts={ts + 100}', huge_data)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/{bucket}/entry?ts={ts}')
    assert resp.status_code == 200
    assert resp.content == huge_data


def test_read_no_bucket(base_url):
    """Should return 404 if no bucket found"""
    resp = requests.get(f'{base_url}/xxx/entry?ts=100')
    assert resp.status_code == 404
    assert 'xxx' in get_detail(resp)


def test_read_no_data(base_url, bucket):
    """Should return 404 if no data for ts"""
    resp = requests.get(f'{base_url}/{bucket}/entry?ts=100', )
    assert resp.status_code == 404


def test_read_bad_ts(base_url, bucket):
    """Should return 400 if ts is bad"""
    resp = requests.get(f'{base_url}/{bucket}/entry?ts=XXXX', )
    assert resp.status_code == 400
    assert 'XXX' in get_detail(resp)


def test_write_no_bucket(base_url):
    """Should return 404 if no bucket found"""
    resp = requests.post(f'{base_url}/xxx/entry?ts=100')
    assert resp.status_code == 404
    assert 'xxx' in get_detail(resp)


def test_write_bad_ts(base_url, bucket):
    """Should return 400 if ts is bad"""
    resp = requests.post(f'{base_url}/{bucket}/entry?ts=XXXX', )
    assert resp.status_code == 400
    assert 'XXX' in get_detail(resp)
