import json

from ..conftest import requires_env, auth_headers


def test_remove_entry(base_url, session, bucket):
    """Should remove entry"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data1")
    assert resp.status_code == 200

    resp = session.delete(f"{base_url}/b/{bucket}/entry")
    assert resp.status_code == 200

    # Deletion is async; allow cleanup to finish before asserting absence
    from time import sleep

    sleep(0.1)
    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 404


@requires_env("API_TOKEN")
def test_remove_with_bucket_write_permissions(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs write permissions to remove entry"""
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts=1000", data="some_data1")
    assert resp.status_code == 200

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry",
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry",
        headers=auth_headers(token_read_bucket.value),
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry",
        headers=auth_headers(token_write_bucket.value),
    )
    assert resp.status_code == 200


def test_rename_entry(base_url, session, bucket):
    """Should rename entry"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data1")
    assert resp.status_code == 200

    resp = session.put(
        f"{base_url}/b/{bucket}/entry/rename", json={"new_name": "new_name"}
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/b/{bucket}/new_name?ts={ts}")
    assert resp.status_code == 200
    assert resp.content == b"some_data1"


@requires_env("API_TOKEN")
def test_rename_with_bucket_write_permissions(
    base_url,
    session,
    bucket,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs to write permissions to rename entry"""
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts=1000", data="some_data1")
    assert resp.status_code == 200

    resp = session.put(
        f"{base_url}/b/{bucket}/entry/rename",
        json={"new_name": "new_name"},
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.put(
        f"{base_url}/b/{bucket}/entry/rename",
        json={"new_name": "new_name"},
        headers=auth_headers(token_read_bucket.value),
    )
    assert resp.status_code == 403

    resp = session.put(
        f"{base_url}/b/{bucket}/entry/rename",
        json={"new_name": "new_name"},
        headers=auth_headers(token_write_bucket.value),
    )
    assert resp.status_code == 200


def test_crud_single_record_for_deep_entry_path(base_url, session, bucket):
    entry = "factory-1/line-2/cell-3/sensor-4"
    renamed = "factory-1/line-2/cell-3/sensor-4-renamed"
    ts = 1000

    resp = session.post(f"{base_url}/b/{bucket}/{entry}?ts={ts}", data=b"raw-data")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts={ts}")
    assert resp.status_code == 200
    assert resp.content == b"raw-data"

    resp = session.put(
        f"{base_url}/b/{bucket}/{entry}/rename", json={"new_name": renamed}
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts={ts}")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/b/{bucket}/{renamed}?ts={ts}")
    assert resp.status_code == 200
    assert resp.content == b"raw-data"

    resp = session.delete(f"{base_url}/b/{bucket}/{renamed}?ts={ts}")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{renamed}?ts={ts}")
    assert resp.status_code == 404

    resp = session.post(
        f"{base_url}/b/{bucket}/{renamed}?ts={ts + 1}", data=b"new-record"
    )
    assert resp.status_code == 200

    resp = session.delete(f"{base_url}/b/{bucket}/{renamed}")
    assert resp.status_code == 200

    from time import sleep

    sleep(0.1)  # entry deletion is async
    resp = session.get(f"{base_url}/b/{bucket}/{renamed}?ts={ts + 1}")
    assert resp.status_code == 404


def test_batch_and_query_for_deep_entry_path(base_url, session, bucket):
    entry = "factory-9/line-8/cell-7/sensor-6"

    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}/batch",
        data=b"aaa" + b"bbb",
        headers={
            "x-reduct-time-1000": "3,",
            "x-reduct-time-1010": "3,",
        },
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{entry}/q?start=1000&stop=1020")
    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"aaa"

    resp = session.get(f"{base_url}/b/{bucket}/{entry}/batch?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b"bbb"

    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}/q",
        json={"query_type": "REMOVE", "start": 1000, "stop": 1020},
    )
    assert resp.status_code == 200
    assert resp.json() == {"removed_records": 2}

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts=1000")
    assert resp.status_code == 404
