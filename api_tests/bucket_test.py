import requests
from conftest import get_detail


def test_create_bucket_ok(base_url, bucket_name):
    """Should create a bucket"""
    resp = requests.post(f'{base_url}/{bucket_name}')

    assert resp.status_code == 200


def test_create_twice_bucket(base_url, bucket_name):
    """Should not create a bucket twice with the same name"""
    requests.post(f'{base_url}/{bucket_name}')
    resp = requests.post(f'{base_url}/{bucket_name}')

    assert resp.status_code == 409
    assert "already exists" in get_detail(resp)


def test_get_bucket_ok(base_url, bucket_name):
    """Should provide information about bucket"""
    resp = requests.post(f'{base_url}/{bucket_name}')
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/{bucket_name}')
    assert resp.status_code == 200


def test_get_bucket_not_exist(base_url, bucket_name):
    """Should return error if the bucket is not found"""
    resp = requests.get(f'{base_url}/{bucket_name}')
    assert resp.status_code == 404
    assert "is not found" in get_detail(resp)


def test_remove_bucket_ok(base_url, bucket_name):
    """Should remove a bucket"""
    resp = requests.post(f'{base_url}/{bucket_name}')
    assert resp.status_code == 200

    resp = requests.delete(f'{base_url}/{bucket_name}')
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/{bucket_name}')
    assert resp.status_code == 404


def test_remove_bucket_not_exist(base_url, bucket_name):
    """Should return an error if  bucket doesn't exist"""
    resp = requests.delete(f'{base_url}/{bucket_name}')
    assert resp.status_code == 404
    assert "is not found" in get_detail(resp)
