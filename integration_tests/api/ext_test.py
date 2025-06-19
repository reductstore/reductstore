import json
from pathlib import Path

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

    assert resp.headers["x-reduct-time-1"] == "2,text/csv,@a=1"


@requires_env("LICENSE_PATH")
def test__ros_ext(base_url, bucket, session):
    data = b""
    with open(f"{Path(__file__).parent}/data/file.mcap", "rb") as f:
        data = f.read()

    resp = session.post(
        f"{base_url}/b/{bucket}/entry?ts=1",
        data=data,
        headers={"Content-Type": "application/mcap"},
    )
    assert resp.status_code == 200

    resp = session.post(
        f"{base_url}/b/{bucket}/entry/q",
        json={
            "query_type": "QUERY",
            "ext": {
                "ros": {  # name of the extension to use
                    "extract": {
                        "topic": "/test"  # Specify the topic to extract from the mcap file
                    },
                },
                "when": {"$limit": 1},
            },
        },
    )

    assert resp.status_code == 200
    query_id = int(json.loads(resp.content)["id"])
    assert query_id >= 0
    resp = session.get(f"{base_url}/b/{bucket}/entry/batch?q={query_id}")
    assert resp.status_code == 200

    assert (
        resp.headers["x-reduct-time-24"]
        == "16,application/json,encoding=cdr,schema=std_msgs/String,topic=/test"
    )
    assert resp.content == b'{"data":"hello"}'
