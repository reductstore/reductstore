import json

from .conftest import requires_env


@requires_env("LICENSE_PATH")
def test__select_ext(base_url, bucket, session):
    """Check if the select extension is available"""
    resp = session.post(f"{base_url}/b/{bucket}/entry?ts=1", data="1,2,3,4,5")
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={
            "query_type": "QUERY",
            "ext": {"select": {"columns": [{"index": 0, "as_label": "a"}]}},
        },
    )

    assert resp.status_code == 200

    query_id = int(json.loads(resp.content)["id"])
    assert query_id >= 0

    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 200

    assert resp.headers["x-reduct-time-1"] == "2,application/octet-stream,@a=1"
