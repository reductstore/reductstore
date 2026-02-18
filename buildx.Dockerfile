# syntax=docker/dockerfile:1
FROM --platform=${BUILDPLATFORM} ubuntu:22.04 AS builder
ARG BUILDPLATFORM

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 reduct \
    && useradd --uid 10001 --gid reduct --create-home --home-dir /home/reduct --shell /usr/sbin/nologin reduct

RUN mkdir -p /data && chown reduct:reduct /data

FROM ubuntu:22.04

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

# Binaries are prepared on GitHub runner.
COPY .image-build/usr/local/bin/reductstore /usr/local/bin/reductstore
COPY .image-build/usr/local/bin/reduct-cli /usr/local/bin/reduct-cli
COPY --chown=reduct:reduct --from=builder /data /data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

USER reduct


ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

EXPOSE 8383

CMD ["reductstore"]
