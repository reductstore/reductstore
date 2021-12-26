import pytest
import os


@pytest.fixture(name="base_url")
def _base_url() -> str:
    return f"http://{os.getenv('STORAGE_HOST', '127.0.0.1')}:8383"
