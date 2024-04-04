# syntax=docker/dockerfile:1
ARG CARGO_TARGET=x86_64-unknown-linux-gnu

FROM --platform=${BUILDPLATFORM} ubuntu:22.04 AS  builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG CARGO_TARGET
ARG GCC_COMPILER=gcc-11
ARG GIT_COMMIT=unspecified

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    curl \
    protobuf-compiler \
    ${GCC_COMPILER}

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Add .cargo/bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup target add ${CARGO_TARGET}

WORKDIR /src

COPY reductstore reductstore
COPY reduct_base reduct_base
COPY reduct_macros reduct_macros
COPY .cargo .cargo
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

RUN GIT_COMMIT=${GIT_COMMIT} cargo build --release --target ${CARGO_TARGET} --package reductstore
RUN cargo install reduct-cli --target ${CARGO_TARGET} --root /src/target/${CARGO_TARGET}/release

RUN mkdir /data

FROM ubuntu:22.04

ARG CARGO_TARGET
COPY --from=builder /src/target/${CARGO_TARGET}/release/reductstore /usr/local/bin/reductstore
COPY --from=builder /src/target/${CARGO_TARGET}/release/bin/reduct-cli /usr/local/bin/reduct-cli
COPY --from=builder /data /data

HEALTHCHECK --interval=5s --timeout=3s \
  CMD reduct-cli server alive http://localhost:8383 || exit 1

EXPOSE 8383

CMD ["reductstore"]
