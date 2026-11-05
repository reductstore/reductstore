# syntax=docker/dockerfile:1
ARG BUILDPLATFORM
ARG FINAL_BASE_IMAGE=reduct/debian-base:trixie@sha256:8e69707a14a0eaa3af1cddc8c8d7ae1303fd08e1e9c1419c6b97d168f0a29d9a
FROM --platform=${BUILDPLATFORM} ${FINAL_BASE_IMAGE} AS builder
ARG BUILDPLATFORM

RUN groupadd --gid 10001 reduct \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin reduct

RUN mkdir -p /data && chown 10001:10001 /data

FROM ${FINAL_BASE_IMAGE}

# Binaries are prepared on GitHub runner.
COPY .image-build/usr/local/bin/reductstore /usr/local/bin/reductstore
COPY .image-build/usr/local/bin/reduct-cli /usr/local/bin/reduct-cli
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/shadow /etc/shadow
COPY --from=builder /etc/gshadow /etc/gshadow
COPY --chown=10001:10001 --from=builder /data /data
COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs

RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 8383
USER 10001:10001

VOLUME [ "/data" ]

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["reductstore"]
