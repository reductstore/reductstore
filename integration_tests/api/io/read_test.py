from integration_tests.api.conftest import auth_headers, requires_env


def test_read_batched_records_v2(base_url, session, bucket):
    headers = {
        "x-reduct-entries": "entry-0,entry-1",
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
        json={"entries": ["entry-0", "entry-1"], "start": 0, "stop": 2000},
    )
    assert resp.status_code == 200
    query_id = resp.json()["id"]

    resp = session.get(
        f"{base_url}/io/{bucket}/read", headers={"x-reduct-query-id": str(query_id)}
    )
    assert resp.status_code == 200
    assert resp.headers["x-reduct-entries"] == "entry-1,entry-0"
    assert resp.headers["x-reduct-start-ts"] == "1000"
    assert resp.headers["x-reduct-0-0"].startswith("3,text/plain")
    assert resp.headers["x-reduct-1-10"].startswith("2,text/plain")
    assert resp.headers["content-length"] == "5"
    assert resp.headers["x-reduct-last"] == "true"
    assert resp.content == b"BBBAA"


@requires_env("API_TOKEN")
def test_read_requires_bucket_read_permissions(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    headers = {
        "x-reduct-entries": "entry-0",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-0": "2,text/plain",
        "content-length": "2",
    }
    resp = session.post(f"{base_url}/io/{bucket}/write", data=b"AA", headers=headers)
    assert resp.status_code == 200

    query_body = {"entries": ["entry-0"], "start": 0, "stop": 2000}

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
