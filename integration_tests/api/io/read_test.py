import pytest
from urllib.parse import quote

from ..conftest import auth_headers, requires_env


@pytest.mark.parametrize("entry_root", ["x", "x/y", "x/y/z"])
def test_read_batched_records_v2(base_url, session, bucket, entry_root):
    entry_0 = f"{entry_root}-0"
    entry_1 = f"{entry_root}-1"
    headers = {
        "x-reduct-entries": f"{entry_0},{entry_1}",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-10": "2,text/plain",
        "x-reduct-1-0": "3,text/plain",
        "content-length": "5",
    }
    body = b"AA" + b"BBB"
    resp = session.post(f"{base_url}/io/{bucket}/write", data=body, headers=headers)
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json={
            "query_type": "QUERY",
            "entries": [entry_0, entry_1],
            "start": 0,
            "stop": 2000,
        },
    )
    assert resp.status_code == 200
    query_id = resp.json()["id"]

    resp = session.get(
        f"{base_url}/io/{bucket}/read", headers={"x-reduct-query-id": str(query_id)}
    )

    print(resp.headers)

    assert resp.status_code == 200
    assert resp.headers["x-reduct-entries"] == (
        f"{quote(entry_1, safe='')},{quote(entry_0, safe='')}"
    )
    assert resp.headers["x-reduct-start-ts"] == "1000"
    assert resp.headers["x-reduct-0-0"].startswith("3,text/plain")
    assert resp.headers["x-reduct-1-10"].startswith("2,text/plain")
    assert resp.headers["content-length"] == "5"
    assert resp.headers["x-reduct-last"] == "true"
    assert resp.content == b"BBBAA"


def test_read_batched_records_v2_stable_order_for_equal_timestamps(
    base_url, session, bucket
):
    entry_z = "z-entry"
    entry_a = "a-entry"
    headers = {
        "x-reduct-entries": f"{entry_z},{entry_a}",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-0": "2,text/plain",
        "x-reduct-1-0": "2,text/plain",
        "content-length": "4",
    }
    body = b"ZZ" + b"AA"
    resp = session.post(f"{base_url}/io/{bucket}/write", data=body, headers=headers)
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json={
            "query_type": "QUERY",
            "entries": [entry_z, entry_a],
            "start": 0,
            "stop": 2000,
        },
    )
    assert resp.status_code == 200
    query_id = resp.json()["id"]

    resp = session.get(
        f"{base_url}/io/{bucket}/read", headers={"x-reduct-query-id": str(query_id)}
    )

    assert resp.status_code == 200
    assert resp.headers["x-reduct-entries"] == (
        f"{quote(entry_a, safe='')},{quote(entry_z, safe='')}"
    )
    assert resp.headers["x-reduct-start-ts"] == "1000"
    assert resp.headers["x-reduct-0-0"].startswith("2,text/plain")
    assert resp.headers["x-reduct-1-0"].startswith("2,text/plain")
    assert resp.headers["x-reduct-last"] == "true"
    assert resp.content == b"AAZZ"


@requires_env("API_TOKEN")
@pytest.mark.parametrize("entry_path", ["x", "x/y", "x/y/z"])
def test_read_requires_bucket_read_permissions(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
    entry_path,
):
    headers = {
        "x-reduct-entries": entry_path,
        "x-reduct-start-ts": "1000",
        "x-reduct-0-0": "2,text/plain",
        "content-length": "2",
    }
    resp = session.post(f"{base_url}/io/{bucket}/write", data=b"AA", headers=headers)
    assert resp.status_code == 200

    query_body = {
        "entries": [entry_path],
        "start": 0,
        "stop": 2000,
        "query_type": "QUERY",
    }

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json=query_body,
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json=query_body,
        headers=auth_headers(token_write_bucket.value),
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json=query_body,
        headers=auth_headers(token_read_bucket.value),
    )
    assert resp.status_code == 200
    query_id = resp.json()["id"]

    resp = session.get(
        f"{base_url}/io/{bucket}/read",
        headers={
            "x-reduct-query-id": str(query_id),
            **auth_headers(token_read_bucket.value),
        },
    )
    assert resp.status_code == 200
