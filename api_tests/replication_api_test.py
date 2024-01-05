"""Replication Tests"""


def test__create_replication_ok(base_url, session, bucket_name, replication_name):
    """Should create a replication"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/replications/{replication_name}",
        json={
            "src_bucket": bucket_name,
            "dst_bucket": bucket_name,
            "dst_host": "http://localhost:9000",
        },
    )

    assert resp.status_code == 200
    # todo: check if replication was created


def test__create_replication_with_invalid_src_bucket(
    base_url, session, replication_name
):
    """Should not create a replication with invalid src bucket"""
    resp = session.post(
        f"{base_url}/replications/{replication_name}",
        json={
            "src_bucket": "invalid_bucket",
            "dst_bucket": "invalid_bucket",
            "dst_host": "http://localhost:9000",
        },
    )

    assert resp.status_code == 404


def test__update_replication_ok(base_url, session, bucket_name, replication_name):
    """Should update a replication"""
    resp = session.post(f"{base_url}/b/{bucket_name}")
    assert resp.status_code == 200

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
    # todo: check if replication was updated
