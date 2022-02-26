FROM gcc:11.2 AS builder
RUN apt-get update && apt-get install -y cmake python3-pip

RUN pip3 install conan

WORKDIR /src

COPY conanfile.txt .
#RUN conan install . --build=missing  TODO: cmake rebuiild the dependcies anyway

COPY src src
COPY unit_tests unit_tests
COPY CMakeLists.txt .

WORKDIR /build

ARG BUILD_TYPE=Release
RUN cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} /src
RUN make -j4

FROM ubuntu:21.10

RUN apt-get update && apt-get install -y libatomic1 #needed for raspianos

COPY --from=builder /build/bin/ /usr/local/bin/
RUN mkdir /data
CMD ["reduct-storage"]
