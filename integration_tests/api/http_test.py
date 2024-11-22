"""Test basic HTTP things"""


def test_api_version(base_url, session):
    """Should get api version in header"""
    resp = session.get(f"{base_url}/info")

    assert resp.status_code == 200
    assert resp.headers["x-reduct-api"] == "1.13"


def test_cors_allows_first_allowed_origin(base_url, session):
    """Should allow requests from the first allowed origin"""
    headers = {"Origin": "https://first-allowed-origin.com"}
    resp = session.get(f"{base_url}/info", headers=headers)
    assert resp.status_code == 200
    assert (
        resp.headers["Access-Control-Allow-Origin"]
        == "https://first-allowed-origin.com"
    )


def test_cors_allows_second_allowed_origin(base_url, session):
    """Should allow requests from the second allowed origin"""
    headers = {"Origin": "https://second-allowed-origin.com"}
    resp = session.get(f"{base_url}/info", headers=headers)
    assert resp.status_code == 200
    assert (
        resp.headers["Access-Control-Allow-Origin"]
        == "https://second-allowed-origin.com"
    )


def test_cors_disallowed_origin(base_url, session):
    """Should not allow requests from a disallowed origin"""
    headers = {"Origin": "https://disallowed-origin.com"}
    resp = session.get(f"{base_url}/info", headers=headers)
    assert resp.status_code == 200
    assert "Access-Control-Allow-Origin" not in resp.headers


def test_cors_allowed_methods(base_url, session):
    """Should allow requests with any method"""
    headers = {"Origin": "https://first-allowed-origin.com"}
    resp = session.options(f"{base_url}/info", headers=headers)
    assert resp.status_code == 200
    assert resp.headers["Access-Control-Allow-Methods"] == "*"


def test_cors_allowed_headers(base_url, session):
    """Should allow requests with any headers"""
    headers = {"Origin": "https://first-allowed-origin.com"}
    resp = session.options(f"{base_url}/info", headers=headers)
    assert resp.status_code == 200
    assert resp.headers["Access-Control-Allow-Headers"] == "*"
