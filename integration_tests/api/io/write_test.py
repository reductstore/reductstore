from ..conftest import requires_env, auth_headers


def test_write_batched_orders_by_entry_then_time(base_url, session, bucket):
    """
    Should consume batched writes in entry-index order, even when timestamps differ.
    Body is concatenated in entry order; if the server sorted by time first, data would swap.
    """

    headers = {
        "content-length": "5",  # 2 bytes for entry-0 (delta 10) + 3 bytes for entry-1 (delta 0)
        "x-reduct-entries": "entry-0,entry-1",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-10": "2,text/plain",
        "x-reduct-1-0": "3,text/plain",
    }
    body = b"AA" + b"BBB"

    resp = session.post(f"{base_url}/io/{bucket}/write", data=body, headers=headers)
    assert resp.status_code == 200

    resp_entry_0 = session.get(f"{base_url}/b/{bucket}/entry-0?ts=1010")
    assert resp_entry_0.status_code == 200
    assert resp_entry_0.content == b"AA"
    assert resp_entry_0.headers["content-type"] == "text/plain"

    resp_entry_1 = session.get(f"{base_url}/b/{bucket}/entry-1?ts=1000")
    assert resp_entry_1.status_code == 200
    assert resp_entry_1.content == b"BBB"
    assert resp_entry_1.headers["content-type"] == "text/plain"


@requires_env("API_TOKEN")
def test_write_with_bucket_write_permissions(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs write permissions to write entry"""
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        data="some_data1",
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        data="some_data1",
        headers=auth_headers(token_read_bucket.value),
    )
    assert resp.status_code == 403

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1000",
        data="some_data1",
        headers=auth_headers(token_write_bucket.value),
    )
    assert resp.status_code == 200
