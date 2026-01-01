from ..conftest import requires_env, auth_headers


def _write_records(base_url, session, bucket):
    resp = session.post(
        f"{base_url}/b/{bucket}/entry-0?ts=1000",
        data="0",
        headers={
            "content-type": "text/plain",
            "x-reduct-label-a": "old",
            "x-reduct-label-b": "keep",
        },
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry-1?ts=1010",
        data="1",
        headers={"content-type": "text/plain", "x-reduct-label-a": "before"},
    )
    assert resp.status_code == 200


def test_update_labels_in_batch(base_url, session, bucket):
    _write_records(base_url, session, bucket)

    headers = {
        "x-reduct-entries": "entry-0,entry-1",
        "x-reduct-start-ts": "1000",
        "x-reduct-labels": "a,b",
        # entry-0: set a -> one, remove b, add c
        "x-reduct-0-0": "0,,0=one,1=,c=3",
        # entry-1: update a only
        "x-reduct-1-10": "0,,a=two",
    }

    resp = session.patch(f"{base_url}/io/{bucket}/update", headers=headers)
    assert resp.status_code == 200
    assert resp.headers == {}

    resp_entry_0 = session.get(f"{base_url}/b/{bucket}/entry-0?ts=1000")
    assert resp_entry_0.status_code == 200
    assert resp_entry_0.headers["x-reduct-label-a"] == "one"
    assert resp_entry_0.headers["x-reduct-label-c"] == "3"
    assert "x-reduct-label-b" not in resp_entry_0.headers

    resp_entry_1 = session.get(f"{base_url}/b/{bucket}/entry-1?ts=1010")
    assert resp_entry_1.status_code == 200
    assert resp_entry_1.headers["x-reduct-label-a"] == "two"


@requires_env("API_TOKEN")
def test_update_labels_in_batch_with_permissions(
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
        "x-reduct-labels": "a,b",
        "x-reduct-0-0": "0,,0=new,1=",
        "x-reduct-1-10": "0,,a=two",
    }

    resp = session.patch(
        f"{base_url}/io/{bucket}/update",
        headers=headers | auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.patch(
        f"{base_url}/io/{bucket}/update",
        headers=headers | auth_headers(token_read_bucket.value),
    )
    assert resp.status_code == 403

    resp = session.patch(
        f"{base_url}/io/{bucket}/update",
        headers=headers | auth_headers(token_write_bucket.value),
    )
    assert resp.status_code == 200

    resp_entry_0 = session.get(f"{base_url}/b/{bucket}/entry-0?ts=1000")
    assert resp_entry_0.headers["x-reduct-label-a"] == "new"
    assert "x-reduct-label-b" not in resp_entry_0.headers
