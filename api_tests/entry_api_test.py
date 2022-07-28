import json

import numpy as np
import pytest

from conftest import get_detail


@pytest.fixture(name='bucket')
def _make_bucket_and_return_name(base_url, session, bucket_name) -> str:
    resp = session.post(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200
    return bucket_name


@pytest.mark.parametrize("data,ts", [(b"some_data_1", 100), (b"some_data_2", 60), (b"some_data_3", 1000)])
def test_read_write_entries_ok(base_url, session, bucket, data, ts):
    """Should write few entries and read them back"""
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data=data)
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/b/{bucket}/entry?ts={ts}')
    assert resp.status_code == 200
    assert resp.content == data


def test_read_write_entries_big_blob_ok(base_url, session, bucket):
    """Should write and read files more than max block size"""
    huge_data = b"xaz" * 1024 * 1024
    ts = 1000
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data=huge_data)
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 100}', data=huge_data)
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/b/{bucket}/entry?ts={ts}')
    assert resp.status_code == 200
    assert resp.content == huge_data

    assert resp.headers['Content-Type'] == "application/octet-stream"


def test_read_no_bucket(base_url, session):
    """Should return 404 if no bucket found"""
    resp = session.get(f'{base_url}/b/xxx/entry?ts=100')
    assert resp.status_code == 404
    assert 'xxx' in get_detail(resp)


def test_read_no_data(base_url, session, bucket):
    """Should return 404 if no data for ts"""
    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=100')
    assert resp.status_code == 404


def test_read_bad_ts(base_url, session, bucket):
    """Should return 400 if ts is bad"""
    session.post(f"{base_url}/b/{bucket}/entry?ts=100", data=b"somedata")
    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=XXXX')

    assert resp.status_code == 422
    assert 'XXX' in get_detail(resp)


def test_read_bad_no_entry(base_url, session, bucket):
    """Should return 400 if ts is bad"""
    resp = session.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 404
    assert 'entry' in get_detail(resp)


def test_write_no_bucket(base_url, session):
    """Should return 404 if no bucket found"""
    resp = session.post(f'{base_url}/b/xxx/entry?ts=100')
    assert resp.status_code == 404
    assert 'xxx' in get_detail(resp)


def test_write_bad_ts(base_url, session, bucket):
    """Should return 422 if ts is bad"""
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts=XXXX')
    assert resp.status_code == 422
    assert 'XXX' in get_detail(resp)


def test_list_entry_ok(base_url, session, bucket):
    """Should return list with timestamps and sizes"""
    ts = 1000
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data="some_data")
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 100}', data="some_data")
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 200}', data="some_data")
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/b/{bucket}/entry/list?start={ts}&stop={ts + 200}')
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert "records" in data
    assert data["records"][0] == {'ts': '1000', 'size': '9'}
    assert data["records"][1] == {'ts': '1100', 'size': '9'}


def test_list_entry_no_data(base_url, session, bucket):
    """Should return 404 if no data for request"""
    resp = session.get(f'{base_url}/b/{bucket}/entry/list?start=100&stop=200')
    assert resp.status_code == 404


def test_latest_record(base_url, session, bucket):
    """Should return the latest record"""
    ts = 1000
    resp = session.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 404

    session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data="some_data1")
    session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 10}', data="some_data2")

    resp = session.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 200
    assert resp.content == b"some_data2"


def test_read_write_big_blob(base_url, session, bucket):
    """Should read and write a big blob"""
    blob = np.random.bytes(2 ** 20).hex()
    ts = 1000

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data=blob)
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 200
    assert len(resp.text) == len(blob)
    assert resp.text == blob


def test_query_entry_ok(base_url, session, bucket):
    """Should return incrementing id with """
    ts = 1000
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data="some_data")
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 100}', data="some_data")
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 200}', data="some_data")
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/b/{bucket}/entry/q?start={ts}&stop={ts + 200}')
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    assert query_id >= 0

    last_id = query_id
    resp = session.get(f'{base_url}/b/{bucket}/entry/q?start={ts}')
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id

    last_id = query_id
    resp = session.get(f'{base_url}/b/{bucket}/entry/q?stop={ts+200}')

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id

    last_id = query_id
    resp = session.get(f'{base_url}/b/{bucket}/entry/q')

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id
