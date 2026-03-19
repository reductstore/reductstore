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


def _docker_output(*args):
    result = subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def _shared_container():
    return os.getenv("PRIMARY_CONTAINER") or os.getenv("SECONDARY_CONTAINER")


def _remove_lock_file():
    shared_container = _shared_container()
    if shared_container:
        _docker_try(
            "run",
            "--rm",
            "--volumes-from",
            shared_container,
            "--user",
            _container_user(shared_container),
            "busybox",
            "rm",
            "-f",
            "/data/.lock",
        )
        return
    data_path = os.getenv("DATA_PATH")
    if data_path:
        lock_path = os.path.join(data_path, ".lock")
        try:
            os.remove(lock_path)
        except FileNotFoundError:
            pass


def _write_lock_file(contents: str):
    shared_container = _shared_container()
    if shared_container:
        _docker(
            "run",
            "--rm",
            "--volumes-from",
            shared_container,
            "--user",
            _container_user(shared_container),
            "busybox",
            "sh",
            "-c",
            f"printf '%s' '{contents}' > /data/.lock",
        )
        return

    data_path = os.getenv("DATA_PATH")
    if data_path:
        with open(os.path.join(data_path, ".lock"), "w", encoding="utf-8") as lock_file:
            lock_file.write(contents)


def _read_lock_file():
    shared_container = _shared_container()
    if shared_container:
        return _docker_output(
            "run",
            "--rm",
            "--volumes-from",
            shared_container,
            "--user",
            _container_user(shared_container),
            "busybox",
            "cat",
            "/data/.lock",
        )

    data_path = os.getenv("DATA_PATH")
    if data_path:
        with open(os.path.join(data_path, ".lock"), encoding="utf-8") as lock_file:
            return lock_file.read()

    raise RuntimeError(
        "PRIMARY_CONTAINER, SECONDARY_CONTAINER, or DATA_PATH must be set"
    )


def _container_user(container_name: str) -> str:
    user = _docker_output("inspect", "-f", "{{.Config.User}}", container_name)
    return user or "10001:10001"


def _container_exit_code(container_name: str) -> int:
    return int(_docker_output("inspect", "-f", "{{.State.ExitCode}}", container_name))


def _container_logs(container_name: str) -> str:
    return _docker_output("logs", container_name)


async def _wait_for_container_exit(container_name: str, timeout_s: float = 15.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status = _docker_output("inspect", "-f", "{{.State.Status}}", container_name)
        if status == "exited":
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for container {container_name} to exit")


async def _wait_for_lock_contents_not(expected: str, timeout_s: float = 15.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if _read_lock_file() != expected:
            return
        await asyncio.sleep(0.5)
    raise AssertionError("Timed out waiting for lock file contents to change")


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
async def test_secondary_panics_if_lock_file_is_changed():
    secondary_container = os.environ["SECONDARY_CONTAINER"]
    primary_container = os.environ["PRIMARY_CONTAINER"]
    primary_url = _ready_url(os.environ["PRIMARY_STORAGE_URL"])
    secondary_url = _ready_url(os.environ["SECONDARY_STORAGE_URL"])
    tampered_lock = "tampered-lock"

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

    _write_lock_file(tampered_lock)

    await _wait_for_container_exit(secondary_container, timeout_s=10)
    assert _container_exit_code(secondary_container) != 0
    logs = _container_logs(secondary_container)
    assert "secondary cannot acquire it" in logs


@requires_env("PRIMARY_STORAGE_URL")
@requires_env("PRIMARY_CONTAINER")
@requires_env("SECONDARY_CONTAINER")
@pytest.mark.asyncio
async def test_primary_overwrites_changed_lock_file():
    primary_container = os.environ["PRIMARY_CONTAINER"]
    secondary_container = os.environ["SECONDARY_CONTAINER"]
    primary_url = _ready_url(os.environ["PRIMARY_STORAGE_URL"])
    secondary_url = _ready_url(os.environ["SECONDARY_STORAGE_URL"])
    tampered_lock = "tampered-lock"

    await _reset_cluster(
        primary_container, secondary_container, primary_url, secondary_url
    )

    _write_lock_file(tampered_lock)

    await _wait_for_lock_contents_not(tampered_lock, timeout_s=10)
    assert _read_lock_file() != tampered_lock
    await _wait_for_status(primary_url, 200, timeout_s=5, label="primary stays ready")
    await _wait_for_status(
        secondary_url, 503, timeout_s=5, label="secondary stays waiting"
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
