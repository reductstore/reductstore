FROM ghcr.io/reductstore/alpine-build-image:main AS builder

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY unit_tests unit_tests
COPY benchmarks benchmarks
COPY CMakeLists.txt .
COPY VERSION VERSION

WORKDIR /build

ARG BUILD_TYPE=Release
ARG WITH_TESTS=OFF
RUN cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DFULL_STATIC_BINARY=ON -DREDUCT_BUILD_TEST=${WITH_TESTS} \
    -DREDUCT_BUILD_BENCHMARKS=${WITH_TESTS} /src
RUN make -j4

RUN mkdir /data

FROM scratch

COPY --from=builder /tmp /tmp
COPY --from=builder /data /data
COPY --from=builder /build/bin/ /usr/local/bin/
ENV PATH=/usr/local/bin/
CMD ["reductstore"]
