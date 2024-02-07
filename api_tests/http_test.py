"""Test basic HTTP things"""


def test_api_version(base_url, session):
    """Should get api version in header"""
    resp = session.get(f"{base_url}/info")

    assert resp.status_code == 200
    assert resp.headers["x-reduct-api"] == "1.9"
