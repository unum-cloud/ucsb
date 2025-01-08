**Note that the instructions are here for DataStax C++ driver version 2.15.x (or lower).**
**We are using Cassandra 3.11 which uses the CQL v3.4.4 protocol, it needs DataStax 2.15.x or lower.**
**The DataStax Cassandra C++ Driver 2.16 only supports Cassandra 4.x and higher.**

To install C++ drivers for Cassandra (officially called DataStax Cassandra C++ Driver) on CentOS 8, follow these steps:

### Install Prerequisites

Ensure required libraries and tools are installed.
```
sudo yum install -y cmake make gcc-c++ openssl openssl-devel wget
```
### Install libuv - a DataStax C++ Driver Dependency

```
Note that the default way of installing libuv by using yum install will 
not work for some reason. 
The yum install step of the cassandra-cpp-driver (mentioned later) will 
fail citing unable to locate libuv.
libuv has to be obtained from the datastax website.
```

1. Get libuv 1.35 from the datastax repository. 


```
wget https://downloads.datastax.com/cpp-driver/centos/8/dependencies/libuv/v1.35.0/libuv-1.35.0-1.el8.x86_64.rpm
wget https://downloads.datastax.com/cpp-driver/centos/8/dependencies/libuv/v1.35.0/libuv-devel-1.35.0-1.el8.x86_64.rpm
```

2. Install the packages using yum. Use the yum package manager to install the downloaded RPMs. This approach ensures that any additional dependencies are resolved automatically:

```
sudo yum localinstall -y libuv-1.35.0-1.el8.x86_64.rpm libuv-devel-1.35.0-1.el8.x86_64.rpm
```
The localinstall command tells yum to install the packages from the local files you’ve downloaded.

3.	Verify the Installation:

**Check the Library Version:** Use `pkg-config` to verify the installed version, run:

```
pkg-config --modversion libuv
```
This command should output 1.35.0, indicating that the correct version is installed.

**List Installed Files:** To see the files installed by the libuv package:
```
rpm -ql libuv
```

**Note that the `libuv --version` command will not work.**

4. Lock libuv to a Specific Version

After installing libuv from the datastax repository, `sudo yum update` command will
not work, citing `Problem: cannot install both libuv-1:1.41.1-1.el8_4.x86_64 from appstream and libuv-1:1.35.0-1.el8.x86_64 from @System`.
So we need to exclude libuv from the update list.

Hence we lock libuv to a specific version, while allowing other updates.

```
sudo yum install -y yum-plugin-versionlock && sudo yum versionlock add libuv libuv-devel
```

Confirm that the packages are locked by running:

```
sudo yum versionlock list
```

Run the yum update again to verify if it runs successfully:

```
sudo yum update -y
```

### Download and Install the Cassandra C++ Driver


1.	Download the DataStax C++ Driver and its dependencies (e.g., cassandra-driver):

```
cd /usr/local/src
sudo wget https://downloads.datastax.com/cpp-driver//centos/8/cassandra/v2.15.3/cassandra-cpp-driver-2.15.3-1.el8.x86_64.rpm
sudo wget https://downloads.datastax.com/cpp-driver//centos/8/cassandra/v2.15.3/cassandra-cpp-driver-devel-2.15.3-1.el8.x86_64.rpm
```

2.	Install the RPM packages using yum:

```
sudo yum install -y cassandra-cpp-driver-2.15.3-1.el8.x86_64.rpm cassandra-cpp-driver-devel-2.15.3-1.el8.x86_64.rpm
```
4. Verify the Installation

To check if the driver installed correctly:

```
ldconfig -p | grep cassandra
```
You should see the libraries like `libcassandra.so` and related files listed.

5. Set Up Your C++ Project

Add the necessary flags for linking the Cassandra driver when compiling your C++ project.

### Sample scripts to check driver installation

There are 2 scripts, a basic one and a more comprehensive one:
- `check_cassandra_connection.cpp` only checks if a connection can be successfully made to a running cassandra instance.
- `check_cassandra_driver_full.cpp` makes a connection, creates a keyspace, creates a table, inserts data into the table and reads data from the table.

Example compilation:
```
g++ -o check_cassandra_connection check_cassandra_connection.cpp -lcassandra
```
Run by `./check_cassandra_connection`

```
g++ -o check_cassandra_driver_full check_cassandra_driver_full.cpp -lcassandra
```
Run by `./check_cassandra_driver_full`

Here `-lcassandra` links the Cassandra C++ driver.

Final Notes:
1.	Ensure Cassandra is running on your machine or a remote server before testing.
2.	Replace 127.0.0.1 with the Cassandra server’s IP address in the cpp files.
3.  The `cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4);` line in the cpp files specifies which 
    protocol version to use. Since the cassandra driver starts from the highest protocol version and falls back to 
    lower versions on error, you will see errors/warnings if you do not specify the exact protocol version from the outset.

