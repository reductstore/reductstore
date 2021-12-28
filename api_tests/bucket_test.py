import requests


def test_create_bucket(base_url, bucket_name):
    """Should provide information about the storage"""
    resp = requests.post(f'{base_url}/{bucket_name}')

    assert resp.status_code == 200


def test_create_twice_bucket(base_url, bucket_name):
    """Should provide information about the storage"""
    requests.post(f'{base_url}/{bucket_name}')
    resp = requests.post(f'{base_url}/{bucket_name}')

    assert resp.status_code == 409
