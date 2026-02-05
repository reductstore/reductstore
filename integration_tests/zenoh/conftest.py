"""Pytest fixtures for Zenoh integration tests."""

import json
import os
import random

import pytest
import pytest_asyncio
import zenoh
from reduct import Client
from _pytest.fixtures import FixtureDef

# Compatibility for pytest-asyncio 0.23 with pytest 9.x.
if not hasattr(FixtureDef, "unittest"):
    FixtureDef.unittest = False


def _rand_name(prefix: str) -> str:
    return f"{prefix}_{random.randint(0, 1_000_000_000)}"


@pytest.fixture(scope="session")
def storage_url() -> str:
    return os.environ.get("STORAGE_URL", "http://127.0.0.1:8383")


@pytest.fixture(scope="session")
def zenoh_connect() -> str:
    return os.environ.get("ZENOH_CONNECT", "tcp/127.0.0.1:7447")


@pytest.fixture(scope="session")
def key_prefix() -> str:
    return os.environ.get("ZENOH_KEY_PREFIX", "reduct")


@pytest.fixture(scope="session")
def api_token() -> str:
    return os.environ.get("API_TOKEN") or os.environ.get("RS_API_TOKEN", "")


@pytest.fixture(scope="session")
def client(storage_url: str, api_token: str) -> Client:
    return Client(storage_url, api_token=api_token)


@pytest.fixture(scope="function")
def bucket_name() -> str:
    return _rand_name("zenoh_bucket")


@pytest.fixture(scope="function")
def entry_name() -> str:
    return _rand_name("entry")


@pytest.fixture(scope="function")
def zenoh_session(zenoh_connect: str):
    config = zenoh.Config()
    zenoh_mode = os.environ.get("ZENOH_MODE", "client")
    config.insert_json5("mode", json.dumps(zenoh_mode))
    config.insert_json5("connect/endpoints", json.dumps([zenoh_connect]))
    if zenoh_mode == "client":
        config.insert_json5("listen/endpoints", json.dumps({"peer": [], "router": []}))
    config.insert_json5("scouting/multicast/enabled", "false")
    if os.environ.get("ZENOH_SHM_ENABLED", "").lower() not in {"1", "true", "yes", "on"}:
        config.insert_json5("transport/shared_memory/enabled", "false")
    session = zenoh.open(config)
    yield session
    session.close()


@pytest_asyncio.fixture(scope="function")
async def bucket(client: Client, bucket_name: str):
    bucket = await client.create_bucket(bucket_name, exist_ok=True)
    try:
        yield bucket
    finally:
        await (await client.get_bucket(bucket_name)).remove()


@pytest.fixture(scope="session")
def serialize_labels():
    def _serialize(labels: dict) -> bytes:
        return json.dumps(labels).encode("utf-8")

    return _serialize
