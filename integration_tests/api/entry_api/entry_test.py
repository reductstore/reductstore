import json
from urllib.parse import quote

from ..conftest import requires_env, auth_headers


def _wait_parent_tree_removed(base_url, session, bucket, parent, timeout=3.0):
    from time import sleep, time

    deadline = time() + timeout
    while time() < deadline:
        resp = session.get(f"{base_url}/b/{bucket}")
        assert resp.status_code == 200
        names = [
            entry["name"]
            for entry in resp.json()["entries"]
            if entry["name"] == parent or entry["name"].startswith(f"{parent}/")
        ]
        if not names:
            return
        sleep(0.05)

    raise AssertionError(f"Entries under '{parent}' are still present")


def test_create_parent_entries_automatically(base_url, session, bucket):
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/a/b/c?ts={ts}", data=b"data-c")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}")
    assert resp.status_code == 200
    names = {entry["name"] for entry in resp.json()["entries"]}

    assert "a" in names
    assert "a/b" in names
    assert "a/b/c" in names


def test_rename_parent_entry_renames_children(base_url, session, bucket):
    resp = session.post(f"{base_url}/b/{bucket}/a/b/c?ts=1000", data=b"data-c")
    assert resp.status_code == 200
    resp = session.post(f"{base_url}/b/{bucket}/a/b/d?ts=1010", data=b"data-d")
    assert resp.status_code == 200

    resp = session.put(f"{base_url}/b/{bucket}/a/rename", json={"new_name": "renamed"})
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/a/b/c?ts=1000")
    assert resp.status_code == 404
    resp = session.get(f"{base_url}/b/{bucket}/a/b/d?ts=1010")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/b/{bucket}/renamed/b/c?ts=1000")
    assert resp.status_code == 200
    assert resp.content == b"data-c"
    resp = session.get(f"{base_url}/b/{bucket}/renamed/b/d?ts=1010")
    assert resp.status_code == 200
    assert resp.content == b"data-d"

    resp = session.get(f"{base_url}/b/{bucket}")
    assert resp.status_code == 200
    names = {entry["name"] for entry in resp.json()["entries"]}
    assert "a" not in names
    assert "a/b" not in names
    assert "a/b/c" not in names
    assert "a/b/d" not in names
    assert "renamed" in names
    assert "renamed/b" in names
    assert "renamed/b/c" in names
    assert "renamed/b/d" in names


def test_remove_parent_entry_removes_children(base_url, session, bucket):
    resp = session.post(f"{base_url}/b/{bucket}/a/b/c?ts=1000", data=b"data-c")
    assert resp.status_code == 200
    resp = session.post(f"{base_url}/b/{bucket}/a/b/d?ts=1010", data=b"data-d")
    assert resp.status_code == 200

    resp = session.delete(f"{base_url}/b/{bucket}/a")
    assert resp.status_code == 200

    _wait_parent_tree_removed(base_url, session, bucket, "a")

    resp = session.get(f"{base_url}/b/{bucket}/a/b/c?ts=1000")
    assert resp.status_code == 404
    resp = session.get(f"{base_url}/b/{bucket}/a/b/d?ts=1010")
    assert resp.status_code == 404


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


def test_meta_entry_visibility_and_bucket_count(base_url, session, bucket):
    entry = "a/x/y"
    meta_entry = f"{entry}/$meta"
    ts = 1000

    resp = session.post(f"{base_url}/b/{bucket}/{entry}?ts={ts}", data=b"data")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/{meta_entry}?ts={ts+1}",
        data=b'{"v":1}',
        headers={"x-reduct-label-key": "schema"},
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{meta_entry}?ts={ts+1}")
    assert resp.status_code == 200
    assert resp.content == b'{"v":1}'

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json={
            "query_type": "QUERY",
            "entries": [f"{entry}*"],
            "start": ts,
            "stop": ts + 2,
        },
    )
    assert resp.status_code == 200
    query_id = int(resp.json()["id"])

    resp = session.get(
        f"{base_url}/io/{bucket}/read", headers={"x-reduct-query-id": str(query_id)}
    )
    assert resp.status_code == 200
    assert resp.headers["x-reduct-entries"] == quote(entry, safe="")

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json={
            "query_type": "QUERY",
            "entries": [meta_entry],
            "start": ts,
            "stop": ts + 2,
        },
    )
    assert resp.status_code == 200
    meta_query_id = int(resp.json()["id"])

    resp = session.get(
        f"{base_url}/io/{bucket}/read",
        headers={"x-reduct-query-id": str(meta_query_id)},
    )
    assert resp.status_code == 200
    assert resp.headers["x-reduct-entries"] == quote(meta_entry, safe="$")

    resp = session.get(f"{base_url}/b/{bucket}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["info"]["entry_count"] == 3
    assert data["info"]["size"] >= len(b"data") + len(b'{"v":1}')
    names = {entry_info["name"] for entry_info in data["entries"]}
    assert names == {entry, "a", "a/x"}
    assert meta_entry not in names


def test_meta_entry_requires_key_label(base_url, session, bucket):
    entry = "a/x/y/$meta"
    resp = session.post(f"{base_url}/b/{bucket}/{entry}?ts=1000", data=b'{"v":1}')
    assert resp.status_code == 422


def test_meta_entry_upsert_by_key(base_url, session, bucket):
    entry = "a/x/y/$meta"

    headers = {"x-reduct-label-key": "schema"}
    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}?ts=1000", data=b'{"v":1}', headers=headers
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}?ts=1010", data=b'{"v":2}', headers=headers
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts=1000")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts=1010")
    assert resp.status_code == 200
    assert resp.content == b'{"v":2}'


def test_meta_entry_remove_marker_by_key(base_url, session, bucket):
    entry = "a/x/y/$meta"

    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}?ts=1000",
        data=b'{"v":1}',
        headers={"x-reduct-label-key": "$plugin"},
    )
    assert resp.status_code == 200

    resp = session.patch(
        f"{base_url}/io/{bucket}/update",
        headers={
            "x-reduct-entries": entry,
            "x-reduct-start-ts": "1000",
            "x-reduct-labels": "key,remove",
            "x-reduct-0-0": "0,,0=$plugin,1=true",
        },
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?ts=1000")
    assert resp.status_code == 200
    assert resp.headers["x-reduct-label-key"] == "$plugin"
    assert resp.headers["x-reduct-label-remove"] == "true"


def test_meta_entry_query_via_get_q_endpoint(base_url, session, bucket):
    entry = "a/x/y/$meta"
    ts = 2000

    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}?ts={ts}",
        data=b'{"schema":"v1"}',
        headers={"x-reduct-label-key": "$plugin-a"},
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/{entry}/q?start={ts}&stop={ts + 1}")
    assert resp.status_code == 200
    query_id = int(resp.json()["id"])

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b'{"schema":"v1"}'


def test_meta_entry_query_via_post_q_endpoint(base_url, session, bucket):
    entry = "a/x/y/$meta"
    ts = 2100

    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}?ts={ts}",
        data=b'{"schema":"v2"}',
        headers={"x-reduct-label-key": "$plugin-b"},
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/{entry}/q",
        json={"query_type": "QUERY", "start": ts, "stop": ts + 1},
    )
    assert resp.status_code == 200
    query_id = int(resp.json()["id"])

    resp = session.get(f"{base_url}/b/{bucket}/{entry}?q={query_id}")
    assert resp.status_code == 200
    assert resp.content == b'{"schema":"v2"}'
