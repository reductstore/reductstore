FROM reduct/ubuntu-build-image:main AS  builder

RUN apt-get install -y \
    build-essential \
    curl

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Add .cargo/bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY rust rust
COPY unit_tests unit_tests
COPY benchmarks benchmarks
COPY CMakeLists.txt .
COPY VERSION VERSION

WORKDIR /build

ARG BUILD_TYPE=Release
RUN cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DREDUCT_BUILD_TEST=ON -DREDUCT_BUILD_BENCHMARKS=ON /src
RUN make -j4


FROM ubuntu:22.04

RUN mkdir /data
COPY --from=builder /build/bin/ /usr/local/bin/
ENV PATH=/usr/local/bin/

CMD ["reductstore"]
