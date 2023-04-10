FROM reduct/ubuntu-build-image:main AS  builder

RUN apt-get update && apt-get install -y rustc cargo

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY rust rust
COPY CMakeLists.txt .
COPY VERSION VERSION

WORKDIR /build

RUN cmake -DCMAKE_BUILD_TYPE=Release -DREDUCT_BUILD_TEST=OFF -DREDUCT_BUILD_BENCHMARKS=OFF /src
RUN make -j4

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y curl libatomic1

COPY --from=builder /build/bin/reductstore /usr/local/bin/reductstore
RUN mkdir /data

EXPOSE 8383

CMD ["reductstore"]
