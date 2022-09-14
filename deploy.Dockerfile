FROM ghcr.io/reduct-storage/web-console:v0.5.0 AS web-console
FROM ubuntu:22.04 AS builder

RUN apt update && apt install -y cmake python3-pip zip

RUN pip3 install conan==1.51.1

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY CMakeLists.txt .

WORKDIR /build

COPY --from=web-console /app /web-console

RUN cmake -DCMAKE_BUILD_TYPE=Release -DREDUCT_BUILD_TEST=OFF -DREDUCT_BUILD_BENCHMARKS=OFF -DWEB_CONSOLE_PATH=/web-console /src
RUN make -j4

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y curl libatomic1

COPY --from=builder /build/bin/reduct-storage /usr/local/bin/reduct-storage
RUN mkdir /data

EXPOSE 8383

CMD ["reduct-storage"]
