FROM ubuntu

MAINTAINER unum@cloud.com

WORKDIR ./

RUN apt-get update

RUN apt install g++ --yes
RUN apt-get install cmake --yes
RUN apt-get install -y libexplain-dev
RUN apt-get install -y libsnappy-dev
RUN apt-get install -y git

# Install WiredTiger (latest)
RUN git clone git://github.com/wiredtiger/wiredtiger.git
RUN mkdir ./wiredtiger/build
WORKDIR "./wiredtiger/build"
RUN cmake ../.
RUN make install
RUN ldconfig -p

WORKDIR ./

COPY ./build_release/bin/ucsb_bench /bin/ucsb_bench

CMD ["/bin/ucsb_bench"]
