from ..conftest import requires_env


def test_remove_entry(base_url, session, bucket):
    """Should remove entry"""
    ts = 1000
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts={ts}", data="some_data1")
    assert resp.status_code == 200

    resp = session.delete(f"{base_url}/b/{bucket}/entry")
    assert resp.status_code == 200

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
        headers={"Authorization": f"Bearer {token_without_permissions}"},
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry",
        headers={"Authorization": f"Bearer {token_read_bucket}"},
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry",
        headers={"Authorization": f"Bearer {token_write_bucket}"},
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
    """Needs write permissions to rename entry"""
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts=1000", data="some_data1")
    assert resp.status_code == 200

    resp = session.put(
        f"{base_url}/b/{bucket}/entry/rename",
        data={"new_name": "new_name"},
        headers={"Authorization": f"Bearer {token_without_permissions}"},
    )
    assert resp.status_code == 403

    resp = session.put(
        f"{base_url}/b/{bucket}/entry/rename",
        data={"new_name": "new_name"},
        headers={"Authorization": f"Bearer {token_read_bucket}"},
    )
    assert resp.status_code == 403

    resp = session.put(
        f"{base_url}/b/{bucket}/entry/rename",
        json={"new_name": "new_name"},
        headers={"Authorization": f"Bearer {token_write_bucket}"},
    )
    assert resp.status_code == 200
