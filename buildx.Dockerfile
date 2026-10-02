# syntax=docker/dockerfile:1
ARG BUILDPLATFORM
ARG BASE_IMAGE=reduct/debian-base:trixie@sha256:1e8eec385f969973a9e3387aca4e40e689186098b4d14770dae39cea60b8495d
FROM --platform=${BUILDPLATFORM} ${BASE_IMAGE} AS builder
ARG BUILDPLATFORM

RUN groupadd --gid 10001 reduct \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin reduct

RUN mkdir -p /data && chown 10001:10001 /data

FROM ${BASE_IMAGE}

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
