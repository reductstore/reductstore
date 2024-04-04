FROM ubuntu:22.04 AS  builder

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    curl \
    protobuf-compiler

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Add .cargo/bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src

COPY reductstore reductstore
COPY reduct_base reduct_base
COPY reduct_macros reduct_macros
COPY .cargo .cargo
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

ARG GIT_COMMIT=unspecified
RUN GIT_COMMIT=${GIT_COMMIT} cargo build --release

FROM ubuntu:22.04

COPY --from=builder /src/target/release/reductstore /usr/local/bin/reductstore

EXPOSE 8383

RUN mkdir /data

CMD ["reductstore"]
