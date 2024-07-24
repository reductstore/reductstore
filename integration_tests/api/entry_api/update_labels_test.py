def test_update_labels(base_url, session, bucket):
    """Should update labels"""

    ts = 1000
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts}",
        headers={"x-reduct-label-x": "y", "x-reduct-label-a": "b"},
        data="some_data",
    )
    assert resp.status_code == 200

    resp = session.patch(
        f"{base_url}/b/{bucket}/entry?ts={ts}",
        headers={
            "x-reduct-label-x": "z",
            "x-reduct-label-a": "",
            "x-reduct-label-1": "2",
        },
    )
    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 200

    assert resp.headers["x-reduct-label-x"] == "z"
    assert "x-reduct-label-a" not in resp.headers
    assert resp.headers["x-reduct-label-1"] == "2"


def test_update_labels_in_batch(base_url, session, bucket):
    """Should update labels in batch"""

    ts = 1000
    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts}",
        headers={"x-reduct-label-x": "y", "x-reduct-label-a": "b"},
        data="some_data",
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts={ts + 1}",
        headers={"x-reduct-label-x": "y", "x-reduct-label-a": "b"},
        data="some_data",
    )
    assert resp.status_code == 200

    resp = session.patch(
        f"{base_url}/b/{bucket}/entry/batch",
        headers={
            f"x-reduct-time-{ts}": "0,,x=z,a=,1=2",
            f"x-reduct-time-{ts + 1}": "0,,x=z,a=,1=2",
        },
    )

    assert resp.status_code == 200

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts}")
    assert resp.status_code == 200

    assert resp.headers["x-reduct-label-x"] == "z"
    assert "x-reduct-label-a" not in resp.headers
    assert resp.headers["x-reduct-label-1"] == "2"

    resp = session.get(f"{base_url}/b/{bucket}/entry?ts={ts + 1}")
    assert resp.status_code == 200

    assert resp.headers["x-reduct-label-x"] == "z"
    assert "x-reduct-label-a" not in resp.headers
    assert resp.headers["x-reduct-label-1"] == "2"
