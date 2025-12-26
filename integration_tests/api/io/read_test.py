def test_read_batched_records_v2(base_url, session, bucket):
    headers = {
        "x-reduct-entries": "entry-0,entry-1",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-10": "2,text/plain",
        "x-reduct-1-0": "3,text/plain",
        "content-length": "5",
    }
    body = b"AA" + b"BBB"
    resp = session.post(f"{base_url}/io/{bucket}/write", data=body, headers=headers)
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/io/{bucket}/q",
        json={"entries": ["entry-0", "entry-1"], "start": 0, "stop": 2000},
    )
    assert resp.status_code == 200
    query_id = resp.json()["id"]

    resp = session.get(f"{base_url}/io/{bucket}/read", params={"q": query_id})
    assert resp.status_code == 200
    assert resp.headers["x-reduct-entries"] == "entry-0,entry-1"
    assert resp.headers["x-reduct-start-ts"] == "1000"
    assert resp.headers["x-reduct-0-10"].startswith("2,text/plain")
    assert resp.headers["x-reduct-1-0"].startswith("3,text/plain")
    assert resp.headers["content-length"] == "5"
    assert resp.headers["x-reduct-last"] == "true"
    assert resp.content == b"AABBB"
