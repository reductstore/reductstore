import json
from datetime import datetime, timedelta, timezone

from .conftest import auth_headers, requires_env


@requires_env("API_TOKEN")
def test__create_token(base_url, session, token_name, bucket_name):
    """Should create a token"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    permissions = {
        "full_access": True,
        "read": [bucket_name],
        "write": [bucket_name],
        "ip_allowlist": [],
    }

    resp = session.post(f"{base_url}/tokens/{token_name}", json=permissions)
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert "token-" in json.loads(resp.content)["value"]


@requires_env("API_TOKEN")
def test__create_v2_token(base_url, session, token_name, bucket_name):
    """Should create a token with v2 json format"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    permissions = {
        "full_access": True,
        "read": [bucket_name],
        "write": [bucket_name],
        "ip_allowlist": [],
    }
    expires_at = (datetime.now(timezone.utc) + timedelta(days=1)).replace(microsecond=0)
    payload = {
        "permissions": permissions,
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
    }

    resp = session.post(f"{base_url}/tokens/{token_name}", json=payload)
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    created_at = json.loads(resp.content)["created_at"]
    assert "token-" in json.loads(resp.content)["value"]

    resp = session.get(f"{base_url}/tokens/{token_name}")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert json.loads(resp.content) == {
        "name": token_name,
        "created_at": created_at,
        "value": "",
        "permissions": permissions,
        "is_provisioned": False,
        "expires_at": json.loads(resp.content)["expires_at"],
    }
    assert json.loads(resp.content)["expires_at"] is not None


@requires_env("API_TOKEN")
def test__creat_token_with_full_access(
    base_url, session, token_name, token_without_permissions
):
    """Needs full access to create a token"""
    permissions = {}

    resp = session.post(
        f"{base_url}/tokens/{token_name}", json=permissions, headers=auth_headers("")
    )
    assert resp.status_code == 401

    resp = session.post(
        f"{base_url}/tokens/{token_name}",
        json=permissions,
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403
    assert (
        resp.headers["x-reduct-error"]
        == f"Token '{token_without_permissions.name}' doesn't have full access"
    )


@requires_env("API_TOKEN")
def test__list_tokens(base_url, session, token_name):
    """Should list all tokens"""
    permissions = {}

    resp = session.post(f"{base_url}/tokens/{token_name}", json=permissions)
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/tokens")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert token_name in [t["name"] for t in json.loads(resp.content)["tokens"]]


@requires_env("API_TOKEN")
def test__list_token_with_full_access(base_url, session, token_without_permissions):
    """Needs full access to list tokens"""
    resp = session.get(f"{base_url}/tokens", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/tokens", headers=auth_headers(token_without_permissions.value)
    )
    assert resp.status_code == 403
    assert (
        resp.headers["x-reduct-error"]
        == f"Token '{token_without_permissions.name}' doesn't have full access"
    )


@requires_env("API_TOKEN")
def test__get_token(base_url, session, bucket_name, token_name):
    """Should show a token name and permissions"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    permissions = {
        "full_access": True,
        "read": [bucket_name],
        "write": [bucket_name],
        "ip_allowlist": [],
    }

    resp = session.post(f"{base_url}/tokens/{token_name}", json=permissions)
    assert resp.status_code == 200
    created_at = json.loads(resp.content)["created_at"]

    resp = session.get(f"{base_url}/tokens/{token_name}")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert json.loads(resp.content) == {
        "name": token_name,
        "created_at": created_at,
        "value": "",
        "permissions": permissions,
        "is_provisioned": False,
        "expires_at": None,
    }


@requires_env("API_TOKEN")
def test__token_ip_allowlist_enforced(base_url, session, token_name):
    """Should allow token usage only from configured client IP"""
    permissions = {
        "full_access": False,
        "read": ["*"],
        "write": [],
        "ip_allowlist": ["203.0.113.10"],
    }

    resp = session.post(f"{base_url}/tokens/{token_name}", json=permissions)
    assert resp.status_code == 200
    token_value = json.loads(resp.content)["value"]

    resp = session.get(
        f"{base_url}/me",
        headers={**auth_headers(token_value), "X-Forwarded-For": "203.0.113.10"},
    )
    assert resp.status_code == 200

    resp = session.get(
        f"{base_url}/me",
        headers={**auth_headers(token_value), "X-Forwarded-For": "203.0.113.11"},
    )
    assert resp.status_code == 401


@requires_env("API_TOKEN")
def test__get_token_with_full_access(base_url, session, token_without_permissions):
    """Needs full access to get a token"""
    resp = session.get(f"{base_url}/tokens/token-name", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.get(
        f"{base_url}/tokens/token-name",
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403
    assert (
        resp.headers["x-reduct-error"]
        == f"Token '{token_without_permissions.name}' doesn't have full access"
    )


@requires_env("API_TOKEN")
def test__rotate_token(base_url, session, token_name):
    """Should rotate a token and revoke the old value"""
    permissions = {"full_access": False, "read": ["*"], "write": []}

    resp = session.post(f"{base_url}/tokens/{token_name}", json=permissions)
    assert resp.status_code == 200
    old_value = json.loads(resp.content)["value"]

    resp = session.post(f"{base_url}/tokens/{token_name}/rotate")
    assert resp.status_code == 200
    new_value = json.loads(resp.content)["value"]
    assert new_value != old_value

    resp = session.get(f"{base_url}/me", headers=auth_headers(old_value))
    assert resp.status_code == 401

    resp = session.get(f"{base_url}/me", headers=auth_headers(new_value))
    assert resp.status_code == 200


@requires_env("API_TOKEN")
def test__rotate_token_with_full_access(base_url, session, token_without_permissions):
    """Needs full access to rotate a token"""
    resp = session.post(f"{base_url}/tokens/token-name/rotate", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.post(
        f"{base_url}/tokens/token-name/rotate",
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403
    assert (
        resp.headers["x-reduct-error"]
        == f"Token '{token_without_permissions.name}' doesn't have full access"
    )


@requires_env("API_TOKEN")
def test__delete_token(base_url, session, token_name):
    """Should delete a token"""
    permissions = {}

    resp = session.post(f"{base_url}/tokens/{token_name}", json=permissions)
    assert resp.status_code == 200

    resp = session.delete(f"{base_url}/tokens/{token_name}")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/tokens")
    assert resp.status_code == 200
    assert token_name not in [t["name"] for t in json.loads(resp.content)["tokens"]]


@requires_env("API_TOKEN")
def test__delete_token_with_full_access(base_url, session, token_without_permissions):
    """Needs full access to delete a token"""
    resp = session.delete(f"{base_url}/tokens/token-name", headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.delete(
        f"{base_url}/tokens/token-name",
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403
    assert (
        resp.headers["x-reduct-error"]
        == f"Token '{token_without_permissions.name}' doesn't have full access"
    )
