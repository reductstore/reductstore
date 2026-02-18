# syntax=docker/dockerfile:1
FROM --platform=${BUILDPLATFORM} ubuntu:24.04 AS builder
ARG BUILDPLATFORM

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data && chown 10001:10001 /data

FROM ubuntu:24.04

# Binaries are prepared on GitHub runner.
COPY .image-build/usr/local/bin/reductstore /usr/local/bin/reductstore
COPY .image-build/usr/local/bin/reduct-cli /usr/local/bin/reduct-cli
COPY --chown=10001:10001 --from=builder /data /data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh


ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

EXPOSE 8383

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["reductstore"]
