FROM ubuntu

MAINTAINER unum@cloud.com

WORKDIR ./

RUN apt update
ENV DEBIAN_FRONTEND noninteractive

# Install tools
RUN apt install -y python3-pip
RUN pip3 install cmake
RUN apt install -y gcc-10
RUN apt install -y g++-10
RUN apt install -y libexplain-dev
RUN apt install -y libsnappy-dev
RUN apt install -y pkg-config
RUN apt install -y git

# Build WiredTiger (latest)
RUN git clone --depth 1 git@github.com:wiredtiger/wiredtiger.git
RUN mkdir ./wiredtiger/build
WORKDIR "./wiredtiger/build"
RUN cmake ..
RUN make install

WORKDIR /

# Build UCSB
RUN git clone --depth 1 git@github.com:unum-cloud/ucsb.git
WORKDIR "./ucsb/"
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 10
RUN update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 10
RUN bash -i build_release.sh
RUN rm -rf ./bench/results

ENTRYPOINT ["./build_release/build/bin/ucsb_bench"]
