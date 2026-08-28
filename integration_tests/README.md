# Run integration tests

Quick run (local):
1) Start the Zenoh router:

```bash
docker run --init -d --name zenoh-router -p 7447:7447/tcp eclipse/zenoh
```

2) Start ReductStore:
```bash
RS_DATA_PATH=~/data-test RS_DISABLE_AUTH=true RS_ZENOH_ENABLED=1 RS_ZENOH_CONFIG="mode=client;connect/endpoints=[tcp/127.0.0.1:7447]" RS_ZENOH_SUB_KEYEXPRS="**" RS_ZENOH_QUERY_KEYEXPRS="**" RS_CORS_ALLOW_ORIGIN="https://first-allowed-origin.com, https://second-allowed-origin.com" cargo run -p reductstore --features "fs-backend web-console zenoh-api"
```

3) Run tests:

```bash
pytest integration_tests/zenoh
```

Cleanup:
```bash
docker rm -f zenoh-router
rm -rf ~/data-test
```

## Zenoh Block Routing

`RS_ZENOH_SUB_KEYEXPRS` / `RS_ZENOH_QUERY_KEYEXPRS` above configure a single static block writing
everything to `RS_ZENOH_BUCKET` (default `zenoh`). The same paths can also be configured as
`RS_ZENOH_SUB_<ID>_*` / `RS_ZENOH_QUERY_<ID>_*` blocks, each with its own bucket and routing.
Setting any indexed variable makes the unindexed ones inactive.

`routing_test.py` exercises this and is skipped unless `RS_ZENOH_ROUTING_TESTS` is set.

1) Start ReductStore with indexed blocks:
```bash
RS_DATA_PATH=~/data-test RS_DISABLE_AUTH=true RS_ZENOH_ENABLED=1 \
  RS_ZENOH_CONFIG="mode=client;connect/endpoints=[tcp/127.0.0.1:7447]" \
  RS_ZENOH_SUB_0_KEYEXPRS='entry_$*,factory/**' RS_ZENOH_SUB_0_BUCKET=zenoh \
  RS_ZENOH_SUB_1_KEYEXPRS='rt_$*/**' RS_ZENOH_SUB_1_ROUTING=key-prefix \
  RS_ZENOH_SUB_1_BUCKET_ALLOWLIST='rt_site_*' RS_ZENOH_SUB_1_ALLOW_BUCKET_CREATION=true \
  RS_ZENOH_QUERY_0_KEYEXPRS='entry_$*,factory/**' RS_ZENOH_QUERY_0_BUCKET=zenoh \
  RS_ZENOH_QUERY_1_KEYEXPRS='rt_$*/**' RS_ZENOH_QUERY_1_ROUTING=key-prefix \
  RS_ZENOH_QUERY_1_BUCKET_ALLOWLIST='rt_site_*' \
  cargo run -p reductstore --features "fs-backend web-console zenoh-api"
```

2) Run tests:
```bash
RS_ZENOH_ROUTING_TESTS=1 pytest integration_tests/zenoh -v
```

Note that `_KEYEXPRS` takes Zenoh key expressions (`$*` matches within a chunk) while
`_BUCKET_ALLOWLIST` takes plain globs matched against bucket names.

## Zenoh Inline Credentials (TLS/Auth)

For cloud or container environments where file mounts are impractical, credentials can be provided inline via environment variables:

| Variable | Description |
|----------|-------------|
| `RS_ZENOH_TLS_ROOT_CA` | Root CA certificate content (PEM format) for validating the Zenoh router's TLS certificate |
| `RS_ZENOH_TLS_CONNECT_CERT` | mTLS client certificate content (PEM format) for client authentication |
| `RS_ZENOH_TLS_CONNECT_KEY` | mTLS client private key content (PEM format) for client authentication |
| `RS_ZENOH_AUTH_DICTIONARY` | User:password dictionary (one entry per line) for username/password authentication |

### Running Credential Tests

To test inline credentials, start ReductStore with the credential env vars, then run pytest with the same vars so the tests know to run.

#### Auth Dictionary Tests

1) Start Zenoh router and ReductStore with auth dictionary:
```bash
docker rm -f zenoh-router && docker run --init -d --name zenoh-router -p 7447:7447/tcp eclipse/zenoh

RS_DATA_PATH=~/data-test RS_DISABLE_AUTH=true RS_ZENOH_ENABLED=1 RS_ZENOH_CONFIG="mode=client;connect/endpoints=[tcp/127.0.0.1:7447]" RS_ZENOH_SUB_KEYEXPRS="**" RS_ZENOH_AUTH_DICTIONARY="testuser:testpassword" cargo run -p reductstore --features "fs-backend web-console zenoh-api"
```

2) Run tests:
```bash
RS_ZENOH_AUTH_DICTIONARY="testuser:testpassword" pytest integration_tests/zenoh/credentials_test.py -v
```

#### TLS Tests

TLS tests require a TLS-enabled Zenoh router with valid certificates. The test CA certificate in `misc/ca.crt` and the server certificate in `misc/certificate.crt` are valid until 2036.

To regenerate test certificates (if needed):
```bash
openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
  -keyout misc/privateKey.key -out misc/certificate.crt \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth"
```

Then start Zenoh router with TLS using the config from `misc/zenoh-tls.json5`:
```bash
docker rm -f zenoh-router && docker run --init -d --name zenoh-router -p 7447:7447/tcp \
  -v ${PWD}/misc:/certs:ro \
  -v ${PWD}/misc/zenoh-tls.json5:/zenoh.json5:ro \
  eclipse/zenoh -c /zenoh.json5
```

Start ReductStore and run tests:
```bash
RS_DATA_PATH=~/data-test RS_DISABLE_AUTH=true RS_ZENOH_ENABLED=1 RS_ZENOH_CONFIG="mode=client;connect/endpoints=[tls/127.0.0.1:7447]" RS_ZENOH_SUB_KEYEXPRS="**" RS_ZENOH_TLS_ROOT_CA="$(cat misc/ca.crt)" cargo run -p reductstore --features "fs-backend web-console zenoh-api"

RS_ZENOH_TLS_ROOT_CA="$(cat misc/ca.crt)" pytest integration_tests/zenoh/credentials_test.py -v
```
