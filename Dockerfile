FROM gcc:11.2 AS builder
RUN apt update && apt install -y cmake python3-pip

RUN pip3 install conan

WORKDIR /src

COPY conanfile.txt .
RUN conan install . --build=missing

COPY src src
COPY unit_tests unit_tests
COPY CMakeLists.txt .

WORKDIR /build

ARG BUILD_TYPE=Release
RUN cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} /src
RUN make -j4

FROM ubuntu:20.04

COPY --from=builder /build/bin/ /usr/local/bin/
CMD ["reduct_storage"]