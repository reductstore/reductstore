# syntax=docker/dockerfile:1
ARG CARGO_TARGET=x86_64-unknown-linux-gnu

FROM --platform=${BUILDPLATFORM} ubuntu:22.04 AS  builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG CARGO_TARGET
ARG GCC_COMPILER=gcc-11
ARG RUST_VERSION
ARG BUILD_PROFILE=release

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

RUN cargo install --force --locked bindgen-cli
RUN cargo install reduct-cli --target ${CARGO_TARGET} --root /build


COPY reductstore reductstore
COPY reduct_base reduct_base
COPY reduct_macros reduct_macros
COPY .cargo /root/.cargo
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

ARG GIT_COMMIT=unspecified

# Use release directory for all builds
RUN CARGO_TARGET_DIR=/build/ \
    GIT_COMMIT=${GIT_COMMIT} \
    cargo build --profile ${BUILD_PROFILE} --target ${CARGO_TARGET} --package reductstore --all-features

RUN mkdir /data

RUN mv /build/${CARGO_TARGET}/${BUILD_PROFILE}/reductstore /usr/local/bin/reductstore
RUN mv /build/bin/reduct-cli /usr/local/bin/reduct-cli

FROM ubuntu:22.04

ARG CARGO_TARGET
ARG BUILD_PROFILE
COPY --from=builder /usr/local/bin/reductstore /usr/local/bin/reductstore
COPY --from=builder /usr/local/bin/reduct-cli /usr/local/bin/reduct-cli
COPY --from=builder /data /data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt


EXPOSE 8383

CMD ["reductstore"]
