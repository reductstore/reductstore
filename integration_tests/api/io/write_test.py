def test_write_batched_orders_by_entry_then_time(base_url, session, bucket):
    """
    Should consume batched writes in entry-index order, even when timestamps differ.
    Body is concatenated in entry order; if the server sorted by time first, data would swap.
    """

    headers = {
        "content-length": "5",  # 2 bytes for entry-0 (delta 10) + 3 bytes for entry-1 (delta 0)
        "x-reduct-entries": "entry-0,entry-1",
        "x-reduct-start-ts": "1000",
        "x-reduct-0-10": "2,text/plain",
        "x-reduct-1-0": "3,text/plain",
    }
    body = b"AA" + b"BBB"

    resp = session.post(f"{base_url}/io/{bucket}/write", data=body, headers=headers)
    assert resp.status_code == 200

    resp_entry_0 = session.get(f"{base_url}/b/{bucket}/entry-0?ts=1010")
    assert resp_entry_0.status_code == 200
    assert resp_entry_0.content == b"AA"
    assert resp_entry_0.headers["content-type"] == "text/plain"

    resp_entry_1 = session.get(f"{base_url}/b/{bucket}/entry-1?ts=1000")
    assert resp_entry_1.status_code == 200
    assert resp_entry_1.content == b"BBB"
    assert resp_entry_1.headers["content-type"] == "text/plain"
