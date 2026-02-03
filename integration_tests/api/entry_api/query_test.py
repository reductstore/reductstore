import json
from time import sleep

from ..conftest import requires_env, auth_headers


def test_query_entry_ok(base_url, session, bucket):
    """Should return incrementing id with"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "QUERY", "start": ts, "stop": ts + 200},
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    assert query_id >= 0

    last_id = query_id
    resp = session.get(f"{base_url}/b/{bucket}/entry/q?start={ts}")
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id

    last_id = query_id
    resp = session.get(f"{base_url}/b/{bucket}/entry/q?stop={ts + 200}")

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id

    last_id = query_id
    resp = session.get(f"{base_url}/b/{bucket}/entry/q")

    query_id = int(json.loads(resp.content)["id"])
    assert query_id > last_id


def test_query_entry_next(base_url, session, bucket):
    """Should read next few records"""

    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts + 100}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts + 200}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q?",
        json={"query_type": "QUERY", "start": ts, "stop": ts + 200},
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")

    assert resp.status_code == 200
    assert resp.content == b"some_data"
    assert resp.headers["x-reduct-time"] == "1000"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")

    assert resp.status_code == 200
    assert resp.content == b"some_data"
    assert resp.headers["x-reduct-time"] == "1100"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 204


def test_query_ttl(base_url, session, bucket):
    """Should keep TTL of query"""

    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "QUERY", "start": ts, "stop": ts + 200, "ttl": 0},
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 404


def _make_bucket_with_records(base_url, session, bucket_name):
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket_name}/entry?ts={ts}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket_name}/entry?ts={ts + 1000}", data="some_data"
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket_name}/entry?ts={ts + 2000}", data="some_data"
    )
    assert resp.status_code == 200


def test_query_limit(base_url, session, bucket):
    """Should limit number of records returned"""
    _make_bucket_with_records(base_url, session, bucket)

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q", json={"query_type": "QUERY", "limit": 1}
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 204


def test_query_each_n(base_url, session, bucket):
    """Should return each 2d record"""
    _make_bucket_with_records(base_url, session, bucket)

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q", json={"query_type": "QUERY", "each_n": 2}
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.headers["x-reduct-time"] == "1000"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.headers["x-reduct-time"] == "3000"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 204


def test_query_each_s(base_url, session, bucket):
    """Should return a record each 2ms"""
    _make_bucket_with_records(base_url, session, bucket)
    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q", json={"query_type": "QUERY", "each_s": 0.002}
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.headers["x-reduct-time"] == "1000"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.headers["x-reduct-time"] == "3000"

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 204


def test_query_with_include_and_exclude(base_url, session, bucket):
    """Should handle include and exclude labels"""
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        data="some_data1",
        headers={"x-reduct-label-foo": "bar"},
    )
    assert resp.status_code == 200
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=2000",
        data="some_data2",
        headers={"x-reduct-label-foo": "baz"},
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "QUERY", "exclude": {"foo": "bar"}},
    )
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"some_data2"

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "QUERY", "include": {"foo": "bar"}},
    )
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"some_data1"


def test_query_when(base_url, session, bucket):
    """Should handle include and exclude labels"""
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        data="some_data1",
        headers={"x-reduct-label-label1": "true"},
    )
    assert resp.status_code == 200
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=2000",
        data="some_data2",
        headers={"x-reduct-label-label1": "false"},
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "QUERY", "when": {"&label1": {"$and": True}}},
    )
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"some_data1"


def test_query_entry_no_next(base_url, session, bucket):
    """Should return no content if there is no record for the query"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "QUERY", "start": ts + 1, "stop": ts + 200, "ttl": 1},
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 204

    sleep(1.1)
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 404


def test_query_continuous(base_url, session, bucket):
    """Should  request next records continuously even if there is no data"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "QUERY", "start": ts + 1, "continuous": True},
    )
    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 204

    resp = session.get(f"{base_url}/b/{bucket}/entry?q={query_id}")
    assert resp.status_code == 204


@requires_env("API_TOKEN")
def test__query_with_read_token(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs read permissions to query"""
    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        headers=auth_headers(""),
        json={"query_type": "QUERY"},
    )
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/b/{bucket}/entry/q",
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        headers=auth_headers(token_read_bucket.value),
        json={"query_type": "QUERY"},
    )
    assert resp.status_code == 404  # no data

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        headers=auth_headers(token_write_bucket.value),
        json={"query_type": "QUERY"},
    )
    assert resp.status_code == 403


def test__head_entry_ok(base_url, session, bucket):
    """Should return only headers and nobody because request is of type HEAD."""
    ts = 1000
    entry_name = "testentry"
    dummy_data = "dummy data"
    resp = session.post(f"{base_url}/b/{bucket}/{entry_name}?ts={ts}", data=dummy_data)
    assert resp.status_code == 200

    resp = session.head(f"{base_url}/b/{bucket}/{entry_name}")
    assert resp.status_code == 200
    assert len(resp.content) == 0
    assert resp.headers["Content-Length"] == str(len(dummy_data))
