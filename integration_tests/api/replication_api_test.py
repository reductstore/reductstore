"""Replication Tests"""

import pytest


@pytest.fixture(name="bucket")
def _make_bucket(base_url, session, bucket_name):
    """Create a bucket for tests"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200


@pytest.mark.usefixtures("bucket")
def test__create_replication_ok(base_url, session, bucket_name, replication_name):
    """Should create a replication"""

    resp = session.post(
        f"{base_url}/replications/{replication_name}",
        json={
            "src_bucket": bucket_name,
            "dst_bucket": "dst_bucket",
            "dst_host": "http://localhost:9000",
            "entries": ["entry1", "entry2"],
            "include": {"key1": "value1"},
            "exclude": {"key2": "value2"},
            "each_n": 10,
            "each_s": 0.5,
            "when": {"$eq": ["&key1", "value1"]},
        },
    )

    assert resp.status_code == 200

    resp = session.get(f"{base_url}/replications/{replication_name}")
    assert resp.status_code == 200
    assert resp.json() == {
        "diagnostics": {"hourly": {"errored": 0, "errors": {}, "ok": 0}},
        "info": {
            "is_active": True,
            "is_provisioned": False,
            "name": replication_name,
            "pending_records": 0,
        },
        "settings": {
            "src_bucket": bucket_name,
            "dst_bucket": "dst_bucket",
            "dst_host": "http://localhost:9000",
            "dst_token": None,
            "entries": ["entry1", "entry2"],
            "exclude": {"key2": "value2"},
            "include": {"key1": "value1"},
            "each_n": 10,
            "each_s": 0.5,
            "when": {"$eq": ["&key1", "value1"]},
        },
    }


@pytest.mark.usefixtures("bucket")
def test__create_replication_with_invalid_src_bucket(
    base_url, session, replication_name
):
    """Should not create a replication with invalid src bucket"""
    resp = session.post(
        f"{base_url}/replications/{replication_name}",
        json={
            "src_bucket": "invalid_bucket",
            "dst_bucket": "dst_bucket",
            "dst_host": "http://localhost:9000",
        },
    )

    assert resp.status_code == 404


@pytest.mark.usefixtures("bucket")
def test__update_replication_ok(base_url, session, bucket_name, replication_name):
    """Should update a replication"""

    resp = session.post(
        f"{base_url}/replications/{replication_name}",
        json={
            "src_bucket": bucket_name,
            "dst_bucket": bucket_name,
            "dst_host": "http://localhost:9000",
        },
    )

    assert resp.status_code == 200

    resp = session.put(
        f"{base_url}/replications/{replication_name}",
        json={
            "src_bucket": bucket_name,
            "dst_bucket": bucket_name,
            "dst_host": "http://localhost:9001",
        },
    )

    assert resp.status_code == 200

    resp = session.get(f"{base_url}/replications/{replication_name}")
    assert resp.status_code == 200

    assert resp.json() == {
        "diagnostics": {"hourly": {"errored": 0, "errors": {}, "ok": 0}},
        "info": {
            "is_active": True,
            "is_provisioned": False,
            "name": replication_name,
            "pending_records": 0,
        },
        "settings": {
            "dst_bucket": bucket_name,
            "dst_host": "http://localhost:9001",
            "dst_token": None,
            "entries": [],
            "exclude": {},
            "include": {},
            "each_n": None,
            "each_s": None,
            "src_bucket": bucket_name,
            "when": None,
        },
    }


@pytest.mark.usefixtures("bucket")
def test__remove_replication_ok(base_url, session, bucket_name, replication_name):
    """Should remove a replication"""

    resp = session.post(
        f"{base_url}/replications/{replication_name}",
        json={
            "src_bucket": bucket_name,
            "dst_bucket": bucket_name,
            "dst_host": "http://localhost:9000",
        },
    )
    assert resp.status_code == 200

    resp = session.delete(f"{base_url}/replications/{replication_name}")
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/replications")
    assert resp.status_code == 200
    assert replication_name not in [r["name"] for r in resp.json()["replications"]]

    resp = session.get(f"{base_url}/replications/{replication_name}")
    assert resp.status_code == 404
