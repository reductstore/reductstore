# syntax=docker/dockerfile:1
ARG CARGO_TARGET=x86_64-unknown-linux-gnu

FROM --platform=$BUILDPLATFORM ubuntu:22.04 AS  builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG CARGO_TARGET
ARG GCC_COMPILER=gcc-11
ARG GIT_COMMIT=unspecified

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    protobuf-compiler \
    libssl-dev \
    pkg-config \
    ${GCC_COMPILER}

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Add .cargo/bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup target add ${CARGO_TARGET}

WORKDIR /src

COPY src src
COPY .cargo .cargo
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY build.rs build.rs

RUN GIT_COMMIT=${GIT_COMMIT} cargo build --release --target ${CARGO_TARGET}

RUN mkdir /data

FROM --platform=$TARGETPLATFORM ubuntu:22.04

ARG CARGO_TARGET
COPY --from=builder /src/target/${CARGO_TARGET}/release/reductstore /usr/local/bin/reductstore
COPY --from=builder /data /data

EXPOSE 8383

ENV PATH=/usr/local/bin/
CMD ["reductstore"]
