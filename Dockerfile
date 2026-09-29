FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential cmake git python3-dev zlib1g-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /ucsb
COPY . .
RUN cmake -DCMAKE_BUILD_TYPE=Release -DUCSB_BUILD_LMDB=ON -B ./build_release && \
    cmake --build ./build_release --parallel 4

ENTRYPOINT ["./build_release/build/bin/ucsb_bench"]
