FROM ghcr.io/reduct-storage/web-console:main AS web-console
FROM gcc:11.2 AS builder

RUN apt update && apt install -y cmake python3-pip zip

RUN pip3 install conan

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY CMakeLists.txt .

WORKDIR /build

COPY --from=web-console /app /web-console

RUN cmake -DCMAKE_BUILD_TYPE=Release -DREDUCT_BUILD_TEST=OFF -DWEB_CONSOLE_PATH=/web-console /src
RUN make -j4

FROM ubuntu:21.10

COPY --from=builder /build/bin/reduct-storage /usr/local/bin/reduct-storage
RUN mkdir /data
CMD ["reduct-storage"]
