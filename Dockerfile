FROM ubuntu

MAINTAINER unum@cloud.com

WORKDIR ./

RUN apt-get update
ENV DEBIAN_FRONTEND noninteractive

# Install tools
RUN apt install -y python3-pip
RUN pip3 install cmake
RUN pip3 install conan
RUN apt install -y gcc-10
RUN apt install -y g++-10
RUN apt-get install -y libexplain-dev
RUN apt-get install -y libsnappy-dev
RUN apt-get install -yq pkg-config
RUN apt-get install -y git

# Build WiredTiger (latest)
RUN git clone git://github.com/wiredtiger/wiredtiger.git
RUN mkdir ./wiredtiger/build
WORKDIR "./wiredtiger/build"
RUN cmake ../.
RUN make install

WORKDIR /

RUN mkdir "./ucsb"
WORKDIR "./ucsb/"
COPY "./build_release/bin/_ucsb_bench" "./build_release/bin/_ucsb_bench"

ENTRYPOINT ["./build_release/bin/_ucsb_bench"]
