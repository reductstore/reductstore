from integration_tests.api.conftest import requires_env, auth_headers


def _write_records(base_url, session, bucket):
    resp = session.post(
        f"{base_url}/b/{bucket}/entry-0?ts=1000",
        data="0",
        headers={"content-type": "text/plain"},
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry-1?ts=1001",
        data="1",
        headers={"content-type": "text/plain"},
    )
    assert resp.status_code == 200


def test_remove_records_in_batch(base_url, session, bucket):
    _write_records(base_url, session, bucket)

    headers = {
        "x-reduct-entries": "entry-0,entry-1",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-0": "",
        "x-reduct-1-1": "",
        "x-reduct-1-2": "",
    }
    resp = session.delete(f"{base_url}/io/{bucket}/remove", headers=headers)
    assert resp.status_code == 200
    assert resp.headers["x-reduct-error-1-2"] == "404,No record with timestamp 1002"

    resp = session.get(f"{base_url}/b/{bucket}/entry-0?ts=1000")
    assert resp.status_code == 404
    resp = session.get(f"{base_url}/b/{bucket}/entry-1?ts=1001")
    assert resp.status_code == 404


@requires_env("API_TOKEN")
def test_remove_records_in_batch_with_permissions(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    _write_records(base_url, session, bucket)

    headers = {
        "x-reduct-entries": "entry-0,entry-1",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-0": "",
        "x-reduct-1-1": "",
    }

    resp = session.delete(
        f"{base_url}/io/{bucket}/remove",
        headers=headers | auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/io/{bucket}/remove",
        headers=headers | auth_headers(token_read_bucket.value),
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/io/{bucket}/remove",
        headers=headers | auth_headers(token_write_bucket.value),
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry-0?ts=1000")
    assert resp.status_code == 404
    resp = session.get(f"{base_url}/b/{bucket}/entry-1?ts=1001")
    assert resp.status_code == 404
