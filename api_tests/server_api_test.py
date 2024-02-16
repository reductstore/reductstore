import json
import re

from conftest import requires_env, auth_headers


def test__get_info(base_url, session):
    """Should provide information about the storage"""
    resp = session.get(f"{base_url}/info")

    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data["version"] >= "1.5.0"
    assert int(data["bucket_count"]) > 0
    assert int(data["uptime"]) >= 0
    assert int(data["latest_record"]) >= 0
    assert int(data["oldest_record"]) >= 0

    assert data["defaults"]["bucket"] == {
        "max_block_records": 256,
        "max_block_size": 64000000,
        "quota_size": 0,
        "quota_type": "NONE",
    }
    assert re.match(r"ReductStore 1\.\d+\.\d+", resp.headers["server"])
    assert resp.headers["Content-Type"] == "application/json"


@requires_env("LICENSE_PATH")
def test__get_lic_info(base_url, session):
    """Should provide information abount license"""
    resp = session.get(f"{base_url}/info")

    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data["license"] == {
        "device_number": 1,
        "disk_quota": 0,
        "expiry_date": "2035-01-01T00:00:00Z",
        "fingerprint": "8ab3caf4c8b29236930ff297d374aaaa94cd98c7ae54568102b20c0b12b4bc5c",
        "invoice": "xxxxxx",
        "licensee": "ReductStore,LLC",
        "plan": "UNLIMITED",
    }


@requires_env("API_TOKEN")
def test__authorized_info(base_url, session, token_without_permissions):
    """Needs authenticated token /info with token"""
    resp = session.get(f"{base_url}/info", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/info", headers=auth_headers(token_without_permissions)
    )
    assert resp.status_code == 200


def test__get_list_of_buckets(base_url, session):
    """Should provide information about the storage"""
    resp = session.get(f"{base_url}/list")

    assert resp.status_code == 200
    data = json.loads(resp.content)

    assert len(data["buckets"]) > 0
    assert "bucket" in data["buckets"][0]["name"]
    assert int(data["buckets"][0]["size"]) >= 0
    assert int(data["buckets"][0]["entry_count"]) >= 0
    assert int(data["buckets"][0]["oldest_record"]) >= 0
    assert int(data["buckets"][0]["latest_record"]) >= 0

    assert resp.headers["Content-Type"] == "application/json"


@requires_env("API_TOKEN")
def test__authorized_list(base_url, session, token_without_permissions):
    """Needs authenticated token /list with token"""
    resp = session.get(f"{base_url}/list", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/list", headers=auth_headers(token_without_permissions)
    )
    assert resp.status_code == 200


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


@requires_env("API_TOKEN")
def test__anonymous_alive(base_url, session):
    """should access /alive without token"""
    resp = session.head(f"{base_url}/alive", headers={})
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
    assert data["permissions"] == {"full_access": True, "read": [], "write": []}

    resp = session.get(f"{base_url}/me", headers=auth_headers(""))
    assert resp.status_code == 401
