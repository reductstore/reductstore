# Run integration tests

Quick run (local):
1) Start the Zenoh router:
   `docker run --init -d --name zenoh-router -p 7447:7447/tcp -p 8000:8000/tcp eclipse/zenoh`
2) Start ReductStore:
   `RS_DATA_PATH=~/data-test RS_ZENOH_ENABLED=1 RS_ZENOH_CONFIG="mode=client;connect=tcp/127.0.0.1:7447" RS_ZENOH_SUB_KEYEXPRS="**" RS_ZENOH_QUERY_KEYEXPRS="**" RS_CORS_ALLOW_ORIGIN="https://first-allowed-origin.com, https://second-allowed-origin.com" cargo run -p reductstore --features "fs-backend web-console zenoh-api"`
3) Run tests:
   `pytest integration_tests/zenoh`

Cleanup:
`docker rm -f zenoh-router`
`rm -rf ~/data-test`
