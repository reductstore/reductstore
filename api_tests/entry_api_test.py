import json

import numpy as np
import pytest

from conftest import requires_env, auth_headers


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
    assert resp.headers['x-reduct-time'] == str(ts)
    assert resp.headers['x-reduct-last'] == '1'


def test_read_no_bucket(base_url, session):
    """Should return 404 if no bucket found"""
    resp = session.get(f'{base_url}/b/xxx/entry?ts=100')
    assert resp.status_code == 404
    assert 'xxx' in resp.headers["-x-reduct-error"]


def test_read_no_data(base_url, session, bucket):
    """Should return 404 if no data for ts"""
    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=100')
    assert resp.status_code == 404


def test_read_bad_ts(base_url, session, bucket):
    """Should return 400 if ts is bad"""
    session.post(f"{base_url}/b/{bucket}/entry?ts=100", data=b"somedata")
    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=XXXX')

    assert resp.status_code == 422
    assert 'XXX' in resp.headers["-x-reduct-error"]


def test_read_bad_no_entry(base_url, session, bucket):
    """Should return 400 if ts is bad"""
    resp = session.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 404
    assert 'entry' in resp.headers["-x-reduct-error"]


@requires_env("API_TOKEN")
def test__read_with_read_token(base_url, session, bucket, token_without_permissions, token_read_bucket,
                               token_write_bucket):
    """Needs read permissions to read"""
    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(''))
    assert resp.status_code == 401

    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(token_without_permissions))
    assert resp.status_code == 403

    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(token_read_bucket))
    assert resp.status_code == 404  # no data

    resp = session.get(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(token_write_bucket))
    assert resp.status_code == 403


def test_write_no_bucket(base_url, session):
    """Should return 404 if no bucket found"""
    resp = session.post(f'{base_url}/b/xxx/entry?ts=100')
    assert resp.status_code == 404
    assert 'xxx' in resp.headers["-x-reduct-error"]


def test_write_bad_ts(base_url, session, bucket):
    """Should return 422 if ts is bad"""
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts=XXXX')
    assert resp.status_code == 422
    assert 'XXX' in resp.headers["-x-reduct-error"]


def test_get_record_ok(base_url, session, bucket):
    """Should return list with timestamps and sizes"""
    ts = 1000
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data="some_data")
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 100}', data="some_data")
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts + 200}', data="some_data")
    assert resp.status_code == 200


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
    assert resp.headers['x-reduct-time'] == '1010'
    assert resp.headers['x-reduct-last'] == '1'


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


def test_write_big_blob_with_error(base_url, session, bucket):
    """Should write a big blob with error and don't crush"""
    blob = np.random.bytes(2 ** 20).hex()
    ts = 1000

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data=blob)
    assert resp.status_code == 200

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data=blob)
    assert resp.status_code == 409

    resp = session.get(f'{base_url}/b/{bucket}/entry')
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__write_with_write_token(base_url, session, bucket, token_without_permissions, token_read_bucket,
                                 token_write_bucket):
    """Needs write permissions to write"""
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(''))
    assert resp.status_code == 401

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(token_without_permissions))
    assert resp.status_code == 403

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(token_read_bucket))
    assert resp.status_code == 403

    resp = session.post(f'{base_url}/b/{bucket}/entry?ts=1000', headers=auth_headers(token_write_bucket))
    assert resp.status_code == 200


def test_query_entry_ok(base_url, session, bucket):
    """Should return incrementing id with """
    ts = 1000
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data="some_data")
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
    resp = session.get(f'{base_url}/b/{bucket}/entry/q?stop={ts + 200}')

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id

    last_id = query_id
    resp = session.get(f'{base_url}/b/{bucket}/entry/q')

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id


def test_query_entry_next(base_url, session, bucket):
    """Should read next few records"""

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
    resp = session.get(f'{base_url}/b/{bucket}/entry?q={query_id}')

    assert resp.status_code == 200
    assert resp.content == b"some_data"
    assert resp.headers['x-reduct-time'] == '1000'
    assert resp.headers['x-reduct-last'] == '0'

    resp = session.get(f'{base_url}/b/{bucket}/entry?q={query_id}')

    assert resp.status_code == 200
    assert resp.content == b"some_data"
    assert resp.headers['x-reduct-time'] == '1100'
    assert resp.headers['x-reduct-last'] == '1'

    resp = session.get(f'{base_url}/b/{bucket}/entry?q={query_id}')
    assert resp.status_code == 404


def test_query_ttl(base_url, session, bucket):
    """Should keep TTL of query"""

    ts = 1000
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data="some_data")
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/b/{bucket}/entry/q?start={ts}&stop={ts + 200}&ttl=0')
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f'{base_url}/b/{bucket}/entry?q={query_id}')
    assert resp.status_code == 404


def test_query_entry_no_next(base_url, session, bucket):
    """Should return no content if there is no record for the query"""
    ts = 1000
    resp = session.post(f'{base_url}/b/{bucket}/entry?ts={ts}', data="some_data")
    assert resp.status_code == 200

    resp = session.get(f'{base_url}/b/{bucket}/entry/q?start={ts + 1}&stop={ts + 200}')
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f'{base_url}/b/{bucket}/entry?q={query_id}')
    assert resp.status_code == 204


@requires_env("API_TOKEN")
def test__query_with_read_token(base_url, session, bucket, token_without_permissions, token_read_bucket,
                                token_write_bucket):
    """Needs read permissions to query"""
    resp = session.get(f'{base_url}/b/{bucket}/entry/q', headers=auth_headers(''))
    assert resp.status_code == 401

    resp = session.get(f'{base_url}/b/{bucket}/entry/q', headers=auth_headers(token_without_permissions))
    assert resp.status_code == 403

    resp = session.get(f'{base_url}/b/{bucket}/entry/q', headers=auth_headers(token_read_bucket))
    assert resp.status_code == 404  # no data

    resp = session.get(f'{base_url}/b/{bucket}/entry/q', headers=auth_headers(token_write_bucket))
    assert resp.status_code == 403
