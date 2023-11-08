import json

from conftest import requires_env, auth_headers


def test__create_bucket_ok(base_url, session, bucket_name):
    """Should create a bucket with default settings"""
    resp = session.post(f"{base_url}/b/{bucket_name}")

    assert resp.status_code == 200
    resp = session.get(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data["settings"] == {
        "max_block_records": 256,
        "max_block_size": 64000000,
        "quota_type": "NONE",
        "quota_size": 0,
    }
    assert data["info"]["name"] == bucket_name
    assert len(data["entries"]) == 0

    assert resp.headers["Content-Type"] == "application/json"


def test__create_bucket_quota_type_null(base_url, session, bucket_name):
    """Should create a bucket if quota_type is null"""
    resp = session.post(f"{base_url}/b/{bucket_name}", json={"quota_type": None})
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data["settings"]["quota_type"] == "NONE"


@requires_env("API_TOKEN")
def test__create_bucket_with_full_access_token(
    base_url, session, bucket_name, token_without_permissions
):
    """Needs full access token"""
    resp = session.post(f"{base_url}/b/{bucket_name}", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.post(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_without_permissions)
    )
    assert resp.status_code == 403


def test__create_bucket_bad_format(base_url, session, bucket_name):
    """Should not create a bucket if JSON data is invalid"""
    resp = session.post(f"{base_url}/b/{bucket_name}", data="xxx")
    assert resp.status_code == 422

    resp = session.post(f"{base_url}/b/{bucket_name}", json={"quota_type": "UNKNOWN"})
    assert resp.status_code == 422
    assert resp.headers["Content-Type"] == "application/json"


def test__create_bucket_custom(base_url, session, bucket_name):
    """Should create a bucket with some settings"""
    resp = session.post(f"{base_url}/b/{bucket_name}", json={"max_block_size": 500})
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data["settings"] == {
        "max_block_records": 256,
        "max_block_size": 500,
        "quota_type": "NONE",
        "quota_size": 0,
    }


@requires_env("API_TOKEN")
def test__get_bucket_with_authenticated_token(
    base_url, session, bucket_name, token_without_permissions
):
    """Needs an authenticated token"""
    session.post(f"{base_url}/b/{bucket_name}")

    resp = session.get(f"{base_url}/b/{bucket_name}", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_without_permissions)
    )
    assert resp.status_code == 200


def test__get_bucket_stats(base_url, session, bucket_name):
    """Should get stats from bucket"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket_name}/entry_1?ts=1000000", data="somedata"
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket_name}/entry_2?ts=2000000", data="anotherdata"
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data["entries"] == [
        {
            "block_count": 1,
            "latest_record": 1000000,
            "name": "entry_1",
            "oldest_record": 1000000,
            "record_count": 1,
            "size": 40,
        },
        {
            "block_count": 1,
            "latest_record": 2000000,
            "name": "entry_2",
            "oldest_record": 2000000,
            "record_count": 1,
            "size": 43,
        },
    ]
    assert data["info"] == dict(
        name=bucket_name,
        entry_count=2,
        size=83,
        latest_record=2000000,
        oldest_record=1000000,
        is_provisioned=False,
    )


def test__update_bucket_ok(base_url, session, bucket_name):
    """Should update setting of the bucket"""
    session.post(f"{base_url}/b/{bucket_name}")

    new_settings: dict = {"max_block_size": 1000, "quota_type": "FIFO"}
    resp = session.put(f"{base_url}/b/{bucket_name}", json=new_settings)
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200
    data = json.loads(resp.content)

    new_settings.update({"quota_size": 0, "max_block_records": 256})
    assert data["settings"] == new_settings


def test__update_bucket_bad_format(base_url, session, bucket_name):
    """Should not update setting if JSON format is bad"""
    session.post(f"{base_url}/b/{bucket_name}")

    resp = session.put(
        f"{base_url}/b/{bucket_name}",
        json={"max_block_size": 1000, "quota_type": "UNKNOWN", "quota_size": 500},
    )
    assert resp.status_code == 422

    resp = session.put(f"{base_url}/b/{bucket_name}", data="NOT_JSON")
    assert resp.status_code == 422


@requires_env("API_TOKEN")
def test__update_bucket_with_full_access_token(
    base_url,
    session,
    bucket_name,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs full access to change bucket"""

    resp = session.put(f"{base_url}/b/{bucket_name}", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.put(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_without_permissions)
    )
    assert resp.status_code == 403

    resp = session.put(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_read_bucket)
    )
    assert resp.status_code == 403

    resp = session.put(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_write_bucket)
    )
    assert resp.status_code == 403


def test__remove_bucket_ok(base_url, session, bucket_name):
    """Should remove a bucket"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    resp = session.delete(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 404


@requires_env("API_TOKEN")
def test__remove_bucket_with_full_access_token(
    base_url,
    session,
    bucket_name,
    token_without_permissions,
    token_read_bucket,
    token_write_bucket,
):
    """Needs full access to remove bucket"""
    resp = session.delete(f"{base_url}/b/{bucket_name}", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.delete(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_without_permissions)
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_read_bucket)
    )
    assert resp.status_code == 403

    resp = session.delete(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_write_bucket)
    )
    assert resp.status_code == 403


def test__head_bucket_ok(base_url, session, bucket_name):
    """Should check if bucket exist"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    resp = session.head(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200
    assert len(resp.content) == 0

    resp = session.head(f"{base_url}/b/{bucket_name}_________")
    assert resp.status_code == 404
    assert len(resp.content) == 0


@requires_env("API_TOKEN")
def test__head_bucket_with_full_access_token(
    base_url, session, bucket_name, token_without_permissions
):
    """Needs authenticated token"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    resp = session.head(f"{base_url}/b/{bucket_name}", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.head(
        f"{base_url}/b/{bucket_name}", headers=auth_headers(token_without_permissions)
    )
    assert resp.status_code == 200
