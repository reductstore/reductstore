FROM gcc:11.2

RUN apt update && apt install -y cmake python3-pip

RUN pip3 install conan

WORKDIR /src

ADD conanfile.txt .
RUN conan install . --build=missing

ADD . .

WORKDIR /build

ARG BUILD_TYPE=Release
RUN cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} /src
RUN make -j4

RUN cp bin/* /usr/local/bin
CMD ["reduct_storage"]