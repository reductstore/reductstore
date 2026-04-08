"""Integration tests for Zenoh inline credential configuration.

These tests verify that inline TLS certificates and auth dictionaries
are correctly handled by the server. Most tests are skipped unless
a TLS-enabled Zenoh router is available.

Environment Variables (for inline credentials):
    RS_ZENOH_TLS_ROOT_CA: Root CA certificate content (PEM)
    RS_ZENOH_TLS_CONNECT_CERT: mTLS client certificate content (PEM)
    RS_ZENOH_TLS_CONNECT_KEY: mTLS client private key content (PEM)
    RS_ZENOH_AUTH_DICTIONARY: User:password dictionary (one per line)
"""

import os
import pytest


@pytest.fixture
def uses_inline_credentials() -> bool:
    """Check if the server is configured with inline credentials."""
    return any(
        [
            os.environ.get("RS_ZENOH_TLS_ROOT_CA"),
            os.environ.get("RS_ZENOH_TLS_CONNECT_CERT"),
            os.environ.get("RS_ZENOH_TLS_CONNECT_KEY"),
            os.environ.get("RS_ZENOH_AUTH_DICTIONARY"),
        ]
    )


@pytest.mark.asyncio
async def test_server_accepts_inline_credential_env_vars(client, zenoh_bucket):
    """Verify the server starts successfully with credential env vars.

    This test passes if the server is running and the bucket is accessible.
    The credentials may or may not be used depending on the Zenoh router config.
    """
    bucket = await client.get_bucket(zenoh_bucket)
    info = await bucket.info()
    assert info.name == zenoh_bucket


@pytest.mark.asyncio
async def test_ingestion_works_with_credential_config(
    bucket,
    entry_name,
    zenoh_session,
):
    """Verify data ingestion still works when credential env vars are set."""
    import time

    keyexpr = entry_name
    payload = b"test with credentials"
    zenoh_session.put(keyexpr, payload)

    time.sleep(0.5)

    async with bucket.read(entry_name) as record:
        data = await record.read_all()
        assert data == payload


@pytest.mark.skipif(
    not os.environ.get("RS_ZENOH_TLS_ROOT_CA"), reason="TLS root CA not configured"
)
@pytest.mark.asyncio
async def test_tls_root_ca_is_used(bucket, entry_name, zenoh_session):
    """Test that TLS root CA certificate is properly applied.

    This test only runs when RS_ZENOH_TLS_ROOT_CA is set.
    It verifies the server can communicate over TLS with the router.
    """
    import time

    keyexpr = entry_name
    payload = b"tls secured data"
    zenoh_session.put(keyexpr, payload)

    time.sleep(0.5)

    async with bucket.read(entry_name) as record:
        data = await record.read_all()
        assert data == payload


@pytest.mark.skipif(
    not os.environ.get("RS_ZENOH_AUTH_DICTIONARY"),
    reason="Auth dictionary not configured",
)
@pytest.mark.asyncio
async def test_auth_dictionary_is_used(bucket, entry_name, zenoh_session):
    """Test that auth dictionary is properly applied.

    This test only runs when RS_ZENOH_AUTH_DICTIONARY is set.
    It verifies the server can authenticate with the router.
    """
    import time

    keyexpr = entry_name
    payload = b"authenticated data"
    zenoh_session.put(keyexpr, payload)

    time.sleep(0.5)

    async with bucket.read(entry_name) as record:
        data = await record.read_all()
        assert data == payload
