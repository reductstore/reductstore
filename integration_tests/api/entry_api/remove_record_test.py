def test_remove_record(base_url, session, bucket):
    """Should remove record."""

    ts = 1000
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts}",
        data="some_data",
    )
    assert resp.status_code == 200

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry?ts={ts}",
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 404
