import json
from time import sleep

import numpy as np
import pytest

from ..conftest import requires_env, auth_headers


@pytest.mark.parametrize(
    "data,ts", [(b"some_data_1", 100), (b"some_data_2", 60), (b"some_data_3", 1000)]
)
def test_read_write_entries_ok(base_url, session, bucket, data, ts):
    """Should write few entries and read them back"""
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data=data)
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 200
    assert resp.content == data


def test_read_write_entries_ok_with_labels(base_url, session, bucket):
    """Should write few entries and read them back with labels"""
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        data="nomater",
        headers={"x-reduct-label-x": "0", "x-reduct-label-y": "1"},
    )
    assert resp.status_code == 200
    resp = session.get(f"{base_url}/b/{bucket}/entry?ts=1000")
    assert resp.status_code == 200
    assert resp.headers["x-reduct-label-x"] == "0"
    assert resp.headers["x-reduct-label-y"] == "1"


def test_read_write_entries_big_blob_ok(base_url, session, bucket):
    """Should write and read files more than max block size"""
    huge_data = b"xaz" * 1024 * 1024
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data=huge_data)
    assert resp.status_code == 200

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts + 100}", data=huge_data)
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 200
    assert resp.content == huge_data

    assert resp.headers["content-type"] == "application/octet-stream"
    assert resp.headers["x-reduct-time"] == str(ts)
    assert resp.headers["x-reduct-last"] == "1"


@requires_env("API_TOKEN")
def test__read_with_read_token(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs read permissions to read"""
    resp = session.get(f"{base_url}/b/{bucket}/entry?ts=1000", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        headers=auth_headers(token_without_permissions),
    )
    assert resp.status_code == 403

    resp = session.get(
        f"{base_url}/b/{bucket}/entry?ts=1000", headers=auth_headers(token_read_bucket)
    )
    assert resp.status_code == 404  # no data

    resp = session.get(
        f"{base_url}/b/{bucket}/entry?ts=1000", headers=auth_headers(token_write_bucket)
    )
    assert resp.status_code == 403


def test_get_record_ok(base_url, session, bucket):
    """Should return list with timestamps and sizes"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts + 100}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts + 200}", data="some_data")
    assert resp.status_code == 200


def test_latest_record(base_url, session, bucket):
    """Should return the latest record"""
    ts = 1000
    resp = session.get(f"{base_url}/b/{bucket}/entry")
    assert resp.status_code == 404

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data1")
    assert resp.status_code == 200
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts + 10}", data="some_data2")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry")
    assert resp.status_code == 200
    assert resp.content == b"some_data2"
    assert resp.headers["x-reduct-time"] == "1010"
    assert resp.headers["x-reduct-last"] == "1"


def test_read_write_big_blob(base_url, session, bucket):
    """Should read and write a big blob"""
    blob = np.random.bytes(2**20).hex()
    ts = 1000

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data=blob)
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry")
    assert resp.status_code == 200
    assert len(resp.text) == len(blob)
    assert resp.text == blob


def test_write_big_blob_with_error(base_url, session, bucket):
    """Should write a big blob with error and don't crush"""
    blob = np.random.bytes(2**20).hex()
    ts = 1000

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data=blob)
    assert resp.status_code == 200

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data=blob)
    assert resp.status_code == 409

    resp = session.get(f"{base_url}/b/{bucket}/entry")
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__write_with_write_token(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs write permissions to write"""
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000", headers=auth_headers("")
    )
    assert resp.status_code == 401

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        headers=auth_headers(token_without_permissions),
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000", headers=auth_headers(token_read_bucket)
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000", headers=auth_headers(token_write_bucket)
    )
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__head_entry_with_full_access_token(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_write_bucket,
    token_read_bucket,
):
    """Needs authenticated token. Should not return the body, only headers."""
    ts = 1000
    entry_name = "testentry"
    dummy_data = "dummy data"
    resp = session.post(
        f"{base_url}/b/{bucket}/{entry_name}?ts={ts}",
        data=dummy_data,
        headers=auth_headers(token_write_bucket),
    )
    assert resp.status_code == 200

    resp = session.head(
        f"{base_url}/b/{bucket}/{entry_name}", headers=auth_headers(token_read_bucket)
    )
    assert resp.status_code == 200
    assert len(resp.content) == 0
    assert resp.headers["Content-Length"] == str(len(dummy_data))


def test_write_with_content_type_header(base_url, session, bucket):
    """Should write data with content type header"""
    ts = 1000
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={1000}",
        data='{"data": "some data"}',
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry/q?start={ts}&stop={ts + 200}")
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b'{"data": "some data"}'
    assert resp.headers["content-type"] == "application/json"


@pytest.mark.parametrize(
    "method, expected_content", [("GET", b"some_data1some_data2"), ("HEAD", b"")]
)
def test_read_batched_records(base_url, session, bucket, method, expected_content):
    """Should read batched records and send metadata in headers"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data1")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts + 100}",
        data="some_data2",
        headers={"content-type": "text/plain", "x-reduct-label-x": "[a,b]"},
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry/q?start={ts}&stop={ts + 200}")
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.request(method, f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == expected_content
    assert resp.headers["content-type"] == "application/octet-stream"
    assert resp.headers["content-length"] == "20"
    assert resp.headers["x-reduct-time-1000"] == "10,application/octet-stream"
    assert resp.headers["x-reduct-time-1100"] == '10,text/plain,x="[a,b]"'
    assert resp.headers["x-reduct-last"] == "true"


def test_read_batched_max_header_size(base_url, session, bucket):
    """Should limit the header size in response"""
    for ts in range(0, 100):
        resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="")
        assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry/q")
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 200
    assert sum(header.startswith("x-reduct-time-") for header in resp.headers) == 86
    assert resp.headers["x-reduct-last"] == "false"

    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 200
    assert sum(header.startswith("x-reduct-time-") for header in resp.headers) == 14
    assert resp.headers["x-reduct-last"] == "true"

    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 404


def test_read_batched_query_required(base_url, session, bucket):
    """Should have query id in params"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data1")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry/batch")
    assert resp.status_code == 422
    assert (
        resp.headers["x-reduct-error"] == "'q' parameter is required for batched reads"
    )


def test_read_batched_continuous_query(base_url, session, bucket):
    """Should read batched records in continuous mode"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data1")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry/q?ttl=1&continuous=true")
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 200
    assert resp.headers["x-reduct-last"] == "true"

    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 204

    sleep(1.1)
    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 404


def test_write_batched_records(base_url, session, bucket):
    """Should write batched records"""
    ts = 1000
    resp = session.post(
        f"{base_url}/b/{bucket}/entry/batch",
        data=b"x" * 1000_000 + b"y" * 1000_000 + b"z" * 1000_000,
        headers={
            "x-reduct-time-1000": "1000000,",
            "x-reduct-time-1100": "1000000,",
            "x-reduct-time-1200": "1000000,",
        },
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry/q?start={1000}")
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"x" * 1000_000
    assert resp.headers["content-type"] == "application/octet-stream"
    assert resp.headers["x-reduct-time"] == "1000"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"y" * 1000_000
    assert resp.headers["content-type"] == "application/octet-stream"
    assert resp.headers["x-reduct-time"] == "1100"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"z" * 1000_000
    assert resp.headers["content-type"] == "application/octet-stream"
    assert resp.headers["x-reduct-time"] == "1200"
