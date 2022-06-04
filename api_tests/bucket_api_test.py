import json

import requests

from conftest import get_detail


def test_create_bucket_ok(base_url, headers, bucket_name):
    """Should create a bucket with default settings"""
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers)

    assert resp.status_code == 200
    resp = requests.get(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data['settings'] == {"max_block_records": "1024", "max_block_size": str(64 * 1024 * 1024),
                                "quota_type": "NONE", "quota_size": '0'}
    assert data['info']['name'] == bucket_name
    assert len(data['entries']) == 0

    assert resp.headers['Content-Type'] == "application/json"


def test_create_bucket_bad_format(base_url, headers, bucket_name):
    """Should not create a bucket if JSON data is invalid """
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers, data="xxx")
    assert resp.status_code == 422

    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers, json={"quota_type": "UNKNOWN"})
    assert resp.status_code == 422
    assert resp.headers['Content-Type'] == "application/json"


def test_create_bucket_custom(base_url, headers, bucket_name):
    """Should create a bucket with some settings"""
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers, json={"max_block_size": 500})
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data['settings'] == {"max_block_records": "1024", "max_block_size": "500", "quota_type": "NONE",
                                "quota_size": "0"}


def test_create_twice_bucket(base_url, headers, bucket_name):
    """Should not create a bucket twice with the same name"""
    requests.post(f'{base_url}/b/{bucket_name}', headers=headers)
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers)

    assert resp.status_code == 409
    assert "already exists" in get_detail(resp)


def test_get_bucket_not_exist(base_url, headers, bucket_name):
    """Should return error if the bucket is not found"""
    resp = requests.get(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 404
    assert "is not found" in get_detail(resp)


def test_get_bucket_stats(base_url, headers, bucket_name):
    """Should get stats from bucket"""
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200

    resp = requests.post(f'{base_url}/b/{bucket_name}/entry_1?ts=1000000', headers=headers, data="somedata")
    assert resp.status_code == 200

    resp = requests.post(f'{base_url}/b/{bucket_name}/entry_2?ts=2000000', headers=headers, data="anotherdata")
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200

    data = json.loads(resp.content)
    assert data['entries'] == [{'block_count': '1',
                                'latest_record': '1000000',
                                'name': 'entry_1',
                                'oldest_record': '1000000',
                                'record_count': '1',
                                'size': '8'},
                               {'block_count': '1',
                                'latest_record': '2000000',
                                'name': 'entry_2',
                                'oldest_record': '2000000',
                                'record_count': '1',
                                'size': '11'}]
    assert data['info'] == dict(name=bucket_name, entry_count='2', size='19', latest_record='2000000',
                                oldest_record='1000000')


def test_update_bucket_ok(base_url, headers, bucket_name):
    """Should update setting of the bucket"""
    requests.post(f'{base_url}/b/{bucket_name}', headers=headers)

    new_settings = {"max_block_size": '1000', "quota_type": "FIFO", "quota_size": '500'}
    resp = requests.put(f'{base_url}/b/{bucket_name}', headers=headers,
                        json=new_settings)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data['settings'] == new_settings


def test_update_bucket_bad_format(base_url, headers, bucket_name):
    """Should not update setting if JSON format is bad"""
    requests.post(f'{base_url}/b/{bucket_name}', headers=headers)

    resp = requests.put(f'{base_url}/b/{bucket_name}', headers=headers,
                        json={"max_block_size": 1000, "quota_type": "UNKNOWN", "quota_size": 500})
    assert resp.status_code == 422

    resp = requests.put(f'{base_url}/b/{bucket_name}', headers=headers,
                        data="NOT_JSON")
    assert resp.status_code == 422


def test_update_bucket_not_found(base_url, headers, bucket_name):
    """Should not update setting if no bucket is found"""
    resp = requests.put(f'{base_url}/b/{bucket_name}', headers=headers,
                        json={"max_block_size": 1000, "quota_type": "FIFO", "quota_size": 500})
    assert resp.status_code == 404


def test_remove_bucket_ok(base_url, headers, bucket_name):
    """Should remove a bucket"""
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200

    resp = requests.delete(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 404


def test_remove_bucket_not_exist(base_url, headers, bucket_name):
    """Should return an error if  bucket doesn't exist"""
    resp = requests.delete(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 404
    assert "is not found" in get_detail(resp)


def test_head_bucket_ok(base_url, headers, bucket_name):
    """Should check if bucket exist"""
    resp = requests.post(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200

    resp = requests.head(f'{base_url}/b/{bucket_name}', headers=headers)
    assert resp.status_code == 200
    assert len(resp.content) == 0

    resp = requests.head(f'{base_url}/b/{bucket_name}_________', headers=headers)
    assert resp.status_code == 404
    assert len(resp.content) == 0
