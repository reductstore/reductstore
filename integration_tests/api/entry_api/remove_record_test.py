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


def test_update_labels_in_batch(base_url, session, bucket):
    """Should remove record in batch"""

    ts = 1000
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts}",
        data="some_data",
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts + 1}",
        data="some_data",
    )
    assert resp.status_code == 200

    resp = session.delete(
        f"{base_url}/b/{bucket}/entry/batch",
        headers={
            f"x-reduct-time-{ts}": "",
            f"x-reduct-time-{ts + 1}": "",
            f"x-reduct-time-{ts + 2}": "",
        },
    )

    assert resp.status_code == 200
    assert (
        resp.headers[f"x-reduct-error-{ts+2}"] == f"404,No record with timestamp {ts+2}"
    )

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 404

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts+1}")
    assert resp.status_code == 404
