import asyncio
import os
import subprocess
import time

import aiohttp
import pytest


def requires_env(key):
    env = os.environ.get(key)
    return pytest.mark.skipif(
        env is None or env == "",
        reason=f"Not suitable environment {key} for current test",
    )


def _ready_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/api/v1/ready"


async def _status(url: str, timeout_s: float = 5.0):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout_s) as response:
                return response.status
    except aiohttp.ClientError:
        return None


async def _wait_for_status(url, expected, timeout_s=30, step_s=0.5, label="status"):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status = await _status(url)
        if status == expected:
            return
        await asyncio.sleep(step_s)
    raise AssertionError(f"Timed out waiting for {label} (expected {expected})")


def _docker(*args):
    subprocess.run(["docker", *args], check=True, capture_output=True, text=True)


def _docker_try(*args):
    subprocess.run(["docker", *args], check=False, capture_output=True, text=True)


def _remove_lock_file():
    data_path = os.getenv("DATA_PATH")
    if data_path:
        lock_path = os.path.join(data_path, ".lock")
        try:
            os.remove(lock_path)
        except FileNotFoundError:
            pass


async def _reset_cluster(
    primary_container, secondary_container, primary_url, secondary_url
):
    _docker_try("stop", secondary_container)
    _docker_try("stop", primary_container)
    _remove_lock_file()
    _docker("start", primary_container)
    await _wait_for_status(primary_url, 200, label="primary ready")
    _docker("start", secondary_container)
    await _wait_for_status(secondary_url, 503, label="secondary waiting")


@requires_env("PRIMARY_STORAGE_URL")
@requires_env("SECONDARY_STORAGE_URL")
@requires_env("PRIMARY_CONTAINER")
@requires_env("SECONDARY_CONTAINER")
@pytest.mark.asyncio
async def test_secondary_promotes_after_primary_stop():
    primary_url = _ready_url(os.environ["PRIMARY_STORAGE_URL"])
    secondary_url = _ready_url(os.environ["SECONDARY_STORAGE_URL"])
    primary_container = os.environ["PRIMARY_CONTAINER"]
    secondary_container = os.environ["SECONDARY_CONTAINER"]

    await _reset_cluster(
        primary_container, secondary_container, primary_url, secondary_url
    )

    _docker("stop", primary_container)

    await _wait_for_status(
        secondary_url,
        200,
        timeout_s=40,
        label="secondary ready after primary stop",
    )


@requires_env("PRIMARY_STORAGE_URL")
@requires_env("SECONDARY_STORAGE_URL")
@requires_env("PRIMARY_CONTAINER")
@requires_env("SECONDARY_CONTAINER")
@pytest.mark.asyncio
async def test_secondary_promotes_after_primary_kill_and_ttl():
    primary_url = _ready_url(os.environ["PRIMARY_STORAGE_URL"])
    secondary_url = _ready_url(os.environ["SECONDARY_STORAGE_URL"])
    primary_container = os.environ["PRIMARY_CONTAINER"]
    secondary_container = os.environ["SECONDARY_CONTAINER"]

    await _reset_cluster(
        primary_container, secondary_container, primary_url, secondary_url
    )

    _docker("kill", "-s", "KILL", primary_container)

    ttl_s = int(os.getenv("LOCK_FILE_TTL_S", "30"))
    await _wait_for_status(
        secondary_url,
        200,
        timeout_s=ttl_s + 30,
        label="secondary ready after primary kill",
    )
