from integration_tests.api.conftest import auth_headers, requires_env


def _write_records(base_url, session, bucket):
    records = [("entry-0", 1000, b"aaa"), ("entry-1", 1100, b"bbb")]
    for entry, ts, data in records:
        resp = session.post(f"{base_url}/b/{bucket}/{entry}?ts={ts}", data=data)
        assert resp.status_code == 200
    return records


def test_remove_records_across_entries(base_url, session, bucket):
    records = _write_records(base_url, session, bucket)

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json={
            "query_type": "REMOVE",
            "entries": ["entry-0", "entry-1"],
            "start": 0,
            "stop": 2000,
        },
    )

    assert resp.status_code == 200
    assert resp.json() == {"removed_records": len(records)}

    for entry, ts, _ in records:
        resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts={ts}")
        assert resp.status_code == 404


def test_remove_records_requires_filter(base_url, session, bucket):
    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json={"query_type": "REMOVE"},
    )

    assert resp.status_code == 422


@requires_env("API_TOKEN")
def test_remove_records_requires_bucket_write_permissions(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    records = _write_records(base_url, session, bucket)
    query_body = {
        "query_type": "REMOVE",
        "entries": ["entry-0", "entry-1"],
        "start": 0,
        "stop": 2000,
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
        headers=auth_headers(token_read_bucket.value),
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json=query_body,
        headers=auth_headers(token_write_bucket.value),
    )
    assert resp.status_code == 200
    assert resp.json() == {"removed_records": len(records)}

    for entry, ts, _ in records:
        resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts={ts}")
        assert resp.status_code == 404
