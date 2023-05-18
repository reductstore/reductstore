FROM ubuntu:22.04 AS  builder

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    protobuf-compiler \
    libssl-dev \
    pkg-config

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Add .cargo/bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src

COPY src src
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY build.rs build.rs

ARG GIT_COMMIT=unspecified
RUN GIT_COMMIT=${GIT_COMMIT} cargo build --release

FROM ubuntu:22.04

COPY --from=builder /src/target/release/reductstore /usr/local/bin/reductstore

EXPOSE 8383

RUN mkdir /data

ENV PATH=/usr/local/bin/
CMD ["reductstore"]
