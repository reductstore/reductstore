import json
import re

from .conftest import requires_env, auth_headers


def test__get_info(base_url, session):
    """Should provide information about the storage"""
    resp = session.get(f"{base_url}/info")

    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data["version"] >= "1.11.0"
    assert int(data["bucket_count"]) > 0
    assert int(data["uptime"]) >= 0
    assert int(data["latest_record"]) >= 0
    assert int(data["oldest_record"]) >= 0

    assert data["defaults"]["bucket"] == {
        "max_block_records": 1024,
        "max_block_size": 64000000,
        "quota_size": 0,
        "quota_type": "NONE",
    }
    assert re.match(r"ReductStore 1\.\d+\.\d+", resp.headers["server"])
    assert resp.headers["Content-Type"] == "application/json"


@requires_env("API_TOKEN")
def test__authorized_info(base_url, session, token_without_permissions):
    """Needs authenticated token /info with token"""
    resp = session.get(f"{base_url}/info", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/info", headers=auth_headers(token_without_permissions.value)
    )
    assert resp.status_code == 200


def test__get_list_of_buckets(base_url, session):
    """Should provide information about the storage"""
    resp = session.get(f"{base_url}/list")

    assert resp.status_code == 200
    data = json.loads(resp.content)

    assert len(data["buckets"]) > 0
    assert any("bucket" in bucket["name"] for bucket in data["buckets"])

    for bucket in data["buckets"]:
        assert int(bucket["size"]) >= 0
        assert int(bucket["entry_count"]) >= 0
        assert int(bucket["oldest_record"]) >= 0
        assert int(bucket["latest_record"]) >= 0

    assert resp.headers["Content-Type"] == "application/json"


@requires_env("API_TOKEN")
def test__authorized_list(base_url, session, token_without_permissions):
    """Needs authenticated token /list with token"""
    resp = session.get(f"{base_url}/list", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/list", headers=auth_headers(token_without_permissions.value)
    )
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__audit_bucket_created_and_accessible(base_url, session, audit_bucket):
    """Should create the audit bucket lazily and expose it via normal bucket APIs"""
    resp = session.get(f"{base_url}/list")
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert any(bucket["name"] == audit_bucket for bucket in data["buckets"])

    resp = session.get(f"{base_url}/b/{audit_bucket}")
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data["info"]["name"] == audit_bucket
    assert data["info"]["entry_count"] >= 1
    assert int(data["info"]["latest_record"]) >= 0


@requires_env("API_TOKEN")
def test__audit_bucket_requires_exact_permission(
    base_url,
    session,
    audit_bucket,
    token_without_permissions,
    token_read_wildcard,
    token_read_audit,
):
    """Should allow $audit only for full-access or exact $audit permission"""
    resp = session.get(
        f"{base_url}/b/{audit_bucket}",
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403

    resp = session.get(
        f"{base_url}/b/{audit_bucket}",
        headers=auth_headers(token_read_wildcard.value),
    )
    assert resp.status_code == 403

    resp = session.get(
        f"{base_url}/b/{audit_bucket}",
        headers=auth_headers(token_read_audit.value),
    )
    assert resp.status_code == 200

    resp = session.get(
        f"{base_url}/list",
        headers=auth_headers(token_read_wildcard.value),
    )
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert all(bucket["name"] != audit_bucket for bucket in data["buckets"])

    resp = session.get(
        f"{base_url}/list",
        headers=auth_headers(token_read_audit.value),
    )
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert any(bucket["name"] == audit_bucket for bucket in data["buckets"])


def test__get_wrong_path(base_url, session):
    """Should return 404"""
    resp = session.get(f"{base_url}/NOTEXIST")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/NOTEXIST/XXXX")
    assert resp.status_code == 404


def test__head_alive(base_url, session):
    """Should provide HEAD /alive method"""
    resp = session.head(f"{base_url}/alive")
    assert resp.status_code == 200


def test__get_alive(base_url, session):
    """Should provide HEAD /alive method"""
    resp = session.get(f"{base_url}/alive")
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__anonymous_alive(base_url, session):
    """should access /alive without token"""
    resp = session.head(f"{base_url}/alive", headers={})
    assert resp.status_code == 200


def test__head_ready(base_url, session):
    """Should provide HEAD /ready method"""
    resp = session.head(f"{base_url}/ready")
    assert resp.status_code == 200


def test__get_ready(base_url, session):
    """Should provide GET /ready method"""
    resp = session.get(f"{base_url}/ready")
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__anonymous_ready(base_url, session):
    """should access /ready without token"""
    resp = session.head(f"{base_url}/ready", headers={})
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__me(
    base_url,
    session,
):
    """Should provide information about current token"""
    resp = session.get(f"{base_url}/me")

    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data["name"] == "init-token"
    assert data["permissions"] == {
        "full_access": True,
        "read": [],
        "write": [],
    }
    assert data["ip_allowlist"] == []

    resp = session.get(f"{base_url}/me", headers=auth_headers(""))
    assert resp.status_code == 401
