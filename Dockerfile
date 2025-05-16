FROM ubuntu:22.04 AS  builder


ARG RUST_VERSION

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    curl \
    protobuf-compiler

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Add .cargo/bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup default ${RUST_VERSION}

WORKDIR /src

COPY reductstore reductstore
COPY reduct_base reduct_base
COPY reduct_macros reduct_macros
COPY .cargo .cargo
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

ARG GIT_COMMIT=unspecified
ARG ARTIFACT_SAS_URL
RUN GIT_COMMIT=${GIT_COMMIT} ARTIFACT_SAS_URL=${ARTIFACT_SAS_URL} cargo build --release --all-features

FROM ubuntu:22.04

COPY --from=builder /src/target/release/reductstore /usr/local/bin/reductstore

EXPOSE 8383

RUN mkdir /data

CMD ["reductstore"]
