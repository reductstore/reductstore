FROM gcc:11.2 AS builder
RUN apt update && apt install -y cmake python3-pip

RUN pip3 install conan

WORKDIR /src

COPY conanfile.txt .
COPY src src
COPY CMakeLists.txt .

WORKDIR /build

RUN cmake -DCMAKE_BUILD_TYPE=Release -DREDUCT_BUILD_TEST=OFF /src
RUN make -j4

FROM ubuntu:21.10

COPY --from=builder /build/bin/reduct-storage /usr/local/bin/reduct-storage
RUN mkdir /var/reduct-storage/data -p
CMD ["reduct-storage"]