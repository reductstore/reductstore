import json

import requests
from conftest import get_detail


def test_create_bucket_ok(base_url, bucket_name):
    """Should create a bucket with default settings"""
    resp = requests.post(f'{base_url}/b/{bucket_name}')

    assert resp.status_code == 200
    resp = requests.get(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data == {"max_block_size": str(1024 * 1024), "quota_type": "NONE", "quota_size": '0'}


def test_create_bucket_bad_format(base_url, bucket_name):
    """Should not create a bucket if JSON data is invalid """
    resp = requests.post(f'{base_url}/b/{bucket_name}', data="xxx")
    assert resp.status_code == 422

    resp = requests.post(f'{base_url}/b/{bucket_name}', json={"quota_type": "UNKNOWN"})
    assert resp.status_code == 422


def test_create_bucket_custom(base_url, bucket_name):
    """Should create a bucket with some settings"""
    resp = requests.post(f'{base_url}/b/{bucket_name}', json={"max_block_size": 500})
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data == {"max_block_size": '500', "quota_type": "NONE", "quota_size": '0'}


def test_create_twice_bucket(base_url, bucket_name):
    """Should not create a bucket twice with the same name"""
    requests.post(f'{base_url}/b/{bucket_name}')
    resp = requests.post(f'{base_url}/b/{bucket_name}')

    assert resp.status_code == 409
    assert "already exists" in get_detail(resp)


def test_get_bucket_not_exist(base_url, bucket_name):
    """Should return error if the bucket is not found"""
    resp = requests.get(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 404
    assert "is not found" in get_detail(resp)


def test_update_bucket_ok(base_url, bucket_name):
    """Should update setting of the bucket"""
    requests.post(f'{base_url}/b/{bucket_name}')

    new_settings = {"max_block_size": '1000', "quota_type": "FIFO", "quota_size": '500'}
    resp = requests.put(f'{base_url}/b/{bucket_name}',
                        json=new_settings)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data == new_settings


def test_update_bucket_bad_format(base_url, bucket_name):
    """Should not update setting if JSON format is bad"""
    requests.post(f'{base_url}/b/{bucket_name}')

    resp = requests.put(f'{base_url}/b/{bucket_name}',
                        json={"max_block_size": 1000, "quota_type": "UNKNOWN", "quota_size": 500})
    assert resp.status_code == 422

    resp = requests.put(f'{base_url}/b/{bucket_name}',
                        data="NOT_JSON")
    assert resp.status_code == 422


def test_update_bucket_not_found(base_url, bucket_name):
    """Should not update setting if no bucket is found"""
    resp = requests.put(f'{base_url}/b/{bucket_name}',
                        json={"max_block_size": 1000, "quota_type": "FIFO", "quota_size": 500})
    assert resp.status_code == 404


def test_remove_bucket_ok(base_url, bucket_name):
    """Should remove a bucket"""
    resp = requests.post(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200

    resp = requests.delete(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 404


def test_remove_bucket_not_exist(base_url, bucket_name):
    """Should return an error if  bucket doesn't exist"""
    resp = requests.delete(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 404
    assert "is not found" in get_detail(resp)


def test_head_bucket_ok(base_url, bucket_name):
    """Should check if bucket exist"""
    resp = requests.post(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200

    resp = requests.head(f'{base_url}/b/{bucket_name}')
    assert resp.status_code == 200
    assert len(resp.content) == 0

    resp = requests.head(f'{base_url}/b/{bucket_name}_________')
    assert resp.status_code == 404
    assert len(resp.content) == 0

