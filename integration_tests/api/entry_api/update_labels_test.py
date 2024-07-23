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
