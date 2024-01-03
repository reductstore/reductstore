"""Replication Tests"""


def test__create_bucket_ok(base_url, session, bucket_name, replication_name):
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
