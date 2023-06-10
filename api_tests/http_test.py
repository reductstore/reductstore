"""Test basic HTTP things"""


# def test_allow_origin(base_url, session):
#     """Should allow origin in header"""
#     session.headers['origin'] = 'http://test.com'
#     resp = session.get(f'{base_url}/info')
#
#     assert resp.status_code == 200
#     assert resp.headers['access-control-allow-origin'] == session.headers['origin']

def test_allow_origin(base_url, session):
    """Should get api version in header"""
    # session.headers['origin'] = 'http://test.com'
    resp = session.get(f'{base_url}/info')

    version = resp.headers['ReductStore']
    tokens = version.split(".", 3)
    expected_version = tokens[0] + "." + tokens[1]

    assert resp.status_code == 200
    assert resp.headers['x-reduct-api'] == expected_version


