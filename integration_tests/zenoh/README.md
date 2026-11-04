# Zenoh Integration Tests

## Default

Covers unindexed static Zenoh subscriber and queryable paths. Start ReductStore with `RS_ZENOH_SUB_KEYEXPRS='**'` and `RS_ZENOH_QUERY_KEYEXPRS='**'`. Run `pytest integration_tests/zenoh/default`.

## Routing

Covers indexed Zenoh block routing for subscriber and queryable paths. Unindexed blocks use the same routing settings without `<ID>_`; see [routing/README.md](routing/README.md). Start ReductStore after sourcing `integration_tests/zenoh/routing/env.sh`. Run `pytest integration_tests/zenoh/routing`.

## Credentials

Covers inline Zenoh TLS and authentication credential configuration. Start ReductStore with the credential environment variables required by the credential scenario. Run `pytest integration_tests/zenoh/credentials`.
