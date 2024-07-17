"""Test for WebConsole integration"""

import os

import pytest


@pytest.fixture(name="console_url")
def _console_url() -> str:
    return f"{os.getenv('STORAGE_URL', 'http://127.0.0.1:8383')}/ui"


def test__web_console(console_url, session):
    """should access web console without token"""
    resp = session.get(f"{console_url}", headers={"Authorization": ""})
    assert resp.status_code == 200
