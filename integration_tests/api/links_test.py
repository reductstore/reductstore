import json

import requests

from .conftest import auth_headers, requires_env


def test__create_and_download_link(base_url, session, token_name, bucket_name):
    """Should create a token"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket_name}/test?ts=19999", data=b"Hello, World!"
    )
    assert resp.status_code == 200

    query_link = {
        "bucket": bucket_name,
        "entry": "test",
        "query": {
            "query_type": "QUERY",
        },
    }

    resp = session.post(f"{base_url}/links", json=query_link)
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    link = json.loads(resp.content)["link"]

    resp = requests.get(link)
    assert resp.status_code == 200
    assert resp.content == b"Hello, World!"


@requires_env("API_TOKEN")
def test__create_link_with_permissions(
    base_url, session, token_name, token_without_permissions, bucket_name
):
    """Needs full access to create a link"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    query_link = {
        "bucket": bucket_name,
        "entry": "test",
        "query": {
            "query_type": "QUERY",
        },
    }

    resp = session.post(f"{base_url}/links", json=query_link, headers=auth_headers(""))
    assert resp.status_code == 401

    resp = session.post(
        f"{base_url}/links",
        json=query_link,
        headers=auth_headers(token_without_permissions.value),
    )
    assert resp.status_code == 403
    assert (
        resp.headers["x-reduct-error"]
        == f"Token '{token_without_permissions.name}' doesn't have read access to bucket '{bucket_name}'"
    )
