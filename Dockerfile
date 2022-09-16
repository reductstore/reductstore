FROM ghcr.io/reduct-storage/alpine-build-image:main AS builder

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY unit_tests unit_tests
COPY benchmarks benchmarks
COPY CMakeLists.txt .
COPY web-console web-console

WORKDIR /build

ARG BUILD_TYPE=Release
RUN cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DFULL_STATIC_BINARY=ON -DWEB_CONSOLE_PATH=/src/web-console /src
RUN make -j4

RUN mkdir /data

FROM scratch

COPY --from=builder /tmp /tmp
COPY --from=builder /data /data
COPY --from=builder /build/bin/ /usr/local/bin/
ENV PATH=/usr/local/bin/
CMD ["reduct-storage"]
