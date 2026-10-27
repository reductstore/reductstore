import os
from uuid import uuid4

import pytest


@pytest.fixture(autouse=True)
def routing_tests_enabled():
    if not os.environ.get("RS_ZENOH_ROUTING_TESTS"):
        pytest.skip("server is not configured with indexed Zenoh blocks")


@pytest.fixture
def run_id() -> str:
    return uuid4().hex
