import pytest
import requests
from conftest import get_detail


@pytest.mark.parametrize("data,ts", [(b"some_data_1", 100), (b"some_data_2", 60), (b"some_data_3", 1000)])
def test_read_write_entries_ok(base_url, bucket_name, data, ts):
    """Should write few entries and read them back"""
    resp = requests.post(f'{base_url}/{bucket_name}')
    assert resp.status_code == 200

    resp = requests.post(f'{base_url}/{bucket_name}/entry?ts={ts}', data=data)
    assert resp.status_code == 200

    resp = requests.get(f'{base_url}/{bucket_name}/entry?ts={ts}')
    assert resp.status_code == 200
    assert resp.content == data
