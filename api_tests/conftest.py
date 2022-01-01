import json

import pytest
import os
import random


@pytest.fixture(name="base_url")
def _base_url() -> str:
    return f"http://{os.getenv('STORAGE_HOST', '127.0.0.1')}:8383"


@pytest.fixture(name='bucket_name')
def _gen_bucket_name() -> str:
    return f'buket_{random.randint(0, 1000000)}'


def get_detail(resp) -> str:
    return json.loads(resp.content)["detail"]
