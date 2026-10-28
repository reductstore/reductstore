# Zenoh Integration Tests

## Default

Covers the unindexed Zenoh subscriber and queryable write and read paths. Start ReductStore with `RS_ZENOH_SUB_KEYEXPRS='**'` and `RS_ZENOH_QUERY_KEYEXPRS='**'`. Run `pytest integration_tests/zenoh/default`.

## Routing

Covers indexed Zenoh block routing for subscriber and queryable paths. Start ReductStore after sourcing `integration_tests/zenoh/routing/env.sh`. Run `pytest integration_tests/zenoh/routing`; see [routing/README.md](routing/README.md) for the full routing matrix.

## Credentials

Covers inline Zenoh TLS and authentication credential configuration. Start ReductStore with the credential environment variables required by the credential scenario. Run `pytest integration_tests/zenoh/credentials`.
