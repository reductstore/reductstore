import pytest
from ..conftest import requires_env, auth_headers


@pytest.fixture(name="write_data")
def _write_data(base_url, session, bucket):
    ts = 1000
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts}",
        data="some_data",
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts + 1}",
        data="some_data",
    )
    assert resp.status_code == 200

    return ts, ts + 1


def test_remove_record(base_url, session, bucket, write_data):
    """Should remove record."""
    ts1, _ = write_data

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry?ts={ts1}",
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts1}")
    assert resp.status_code == 404


def test_remove_records_in_batch(base_url, session, bucket, write_data):
    """Should remove record in batch"""
    ts1, ts2 = write_data

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/batch",
        headers={
            f"x-reduct-time-{ts1}": "",
            f"x-reduct-time-{ts2}": "",
            f"x-reduct-time-{ts2 + 1}": "",
        },
    )

    assert resp.status_code == 200
    assert (
        resp.headers[f"x-reduct-error-{ts2 + 1}"]
        == f"404,Record {ts2 + 1} not found in entry {bucket}/entry"
    )

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts1}")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts2}")
    assert resp.status_code == 404


def test_remove_records_query(base_url, session, bucket, write_data):
    """Should remove record within time range."""
    ts1, ts2 = write_data

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={"query_type": "REMOVE", "start": ts1, "stop": ts2 + 1},
    )

    assert resp.status_code == 200
    assert resp.json() == {"removed_records": 2}

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts1}")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts2}")
    assert resp.status_code == 404


@requires_env("API_TOKEN")
def test_remove_record_with_permission(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
    write_data,
):
    ts1, _ = write_data

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry?ts={ts1}",
        headers=auth_headers(token_without_permissions.value),
    )

    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry?ts={ts1}",
        headers=auth_headers(token_read_bucket.value),
    )

    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry?ts={ts1}",
        headers=auth_headers(token_write_bucket.value),
    )

    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test_remove_records_in_batch_with_permission(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
    write_data,
):
    ts1, _ = write_data

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/batch",
        headers=auth_headers(token_without_permissions.value),
    )

    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/batch",
        headers=auth_headers(token_read_bucket.value),
    )

    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/batch",
        headers=auth_headers(token_write_bucket.value),
    )

    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test_remove_records_query_with_permission(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
    write_data,
):
    ts1, ts2 = write_data

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/q?start={ts1}&stop={ts2 + 1}",
        headers=auth_headers(token_without_permissions.value),
    )

    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/q?start={ts1}&stop={ts2 + 1}",
        headers=auth_headers(token_read_bucket.value),
    )

    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/q?start={ts1}&stop={ts2 + 1}",
        headers=auth_headers(token_write_bucket.value),
    )

    assert resp.status_code == 200
