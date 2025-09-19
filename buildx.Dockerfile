# syntax=docker/dockerfile:1
ARG CARGO_TARGET=x86_64-unknown-linux-gnu

FROM --platform=${BUILDPLATFORM} ubuntu:22.04 AS  builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG CARGO_TARGET
ARG GCC_COMPILER=gcc-11
ARG RUST_VERSION
ARG BULD_PROFILE=release

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    curl \
    protobuf-compiler \
    clang \
    ca-certificates \
    ${GCC_COMPILER}

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
# Add .cargo/bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup default ${RUST_VERSION}
RUN rustup target add ${CARGO_TARGET}

WORKDIR /src

COPY reductstore reductstore
COPY reduct_base reduct_base
COPY reduct_macros reduct_macros
COPY .cargo /root/.cargo
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

ARG GIT_COMMIT=unspecified
ARG ARTIFACT_SAS_URL


RUN cargo install --force --locked bindgen-cli

# Use release directory for all builds
RUN CARGO_TARGET_DIR=target/${CARGO_TARGET}/release \
    GIT_COMMIT=${GIT_COMMIT} \
    ARTIFACT_SAS_URL=${ARTIFACT_SAS_URL} \
    cargo build --profile ${BULD_PROFILE} --target ${CARGO_TARGET} --package reductstore --all-features
RUN cargo install reduct-cli --profile ${BULD_PROFILE} --target ${CARGO_TARGET} --root /src/target/${CARGO_TARGET}/release

RUN mkdir /data

FROM ubuntu:22.04

ARG CARGO_TARGET
COPY --from=builder /src/target/${CARGO_TARGET}/release/reductstore /usr/local/bin/reductstore
COPY --from=builder /src/target/${CARGO_TARGET}/release/bin/reduct-cli /usr/local/bin/reduct-cli
COPY --from=builder /data /data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt


EXPOSE 8383

CMD ["reductstore"]
