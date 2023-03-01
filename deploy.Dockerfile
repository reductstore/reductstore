FROM ubuntu:22.04 AS builder

RUN apt update && apt install -y cmake python3-pip zip

RUN pip3 install conan~=1.58.0

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY CMakeLists.txt .
COPY web-console web-console

WORKDIR /build

RUN cmake -DCMAKE_BUILD_TYPE=Release -DREDUCT_BUILD_TEST=OFF -DREDUCT_BUILD_BENCHMARKS=OFF -DWEB_CONSOLE_PATH=/src/web-console /src
RUN make -j4

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y curl libatomic1

COPY --from=builder /build/bin/reductstore /usr/local/bin/reductstore
RUN mkdir /data

EXPOSE 8383

CMD ["reductstore"]
