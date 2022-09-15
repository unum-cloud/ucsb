FROM gcc:12 as builder

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install -y -V --fix-missing \
    build-essential \
    cmake \
    libexplain-dev

COPY . /usr/src/ucsb
WORKDIR /usr/src/ucsb
RUN cmake . && make -j16

FROM ubuntu:22.04
WORKDIR /root/
COPY --from=builder /usr/src/ucsb/build_release/bin/ucsb_bench ./
COPY ./run.py ./run.py
ENTRYPOINT ["python", "run.py"]
