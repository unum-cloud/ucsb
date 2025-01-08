Below are a few common ways to upgrade CMake on CentOS 8 from version 3.26.5 to 3.28.4. You can either build from source (recommended if no official repo provides the newer version), or look for a prebuilt package (if available). The from-source approach is the most straightforward and guarantees you get the exact desired version.

1. Uninstall the Old Version (Optional but Recommended)

First, remove the old version of CMake that was installed from the CentOS 8 repositories (or any other source):

```
sudo dnf remove cmake
```
Note: If you want to keep the old version side by side, you can skip this step and install the new version somewhere else (e.g., /usr/local) and adjust your PATH accordingly.

2. Install Build Dependencies

To build CMake from source, you need development tools:
```
sudo dnf groupinstall "Development Tools"
```

Also ensure you have other necessary libraries (though most should be included in “Development Tools”)—e.g., openssl-devel if you want TLS support for CMake’s built-in file downloads:

```
sudo dnf install openssl-devel
```

3. Download and Unpack CMake 3.28.4

    1.	Go to the official CMake releases page and find version 3.28.4, or directly use wget:
```
wget https://github.com/Kitware/CMake/releases/download/v3.28.4/cmake-3.28.4.tar.gz
```

   2.	Unpack the tarball:
```
tar -zxvf cmake-3.28.4.tar.gz
cd cmake-3.28.4
```
4. Bootstrap and Compile

1.	Run the bootstrap script to configure the build. You can set --prefix=/usr/local or any other directory you prefer:
```
./bootstrap --prefix=/usr/local
```

2.	Compile using make. You can speed up compilation by using multiple cores with -j:
```
make -j$(nproc)
```

3.	Install:
```
sudo make install
```
5. Verify the Installation

Check the installed CMake version:
```
/usr/local/bin/cmake --version
```
If you uninstalled the old cmake and installed the new one under /usr/local, you may need to ensure /usr/local/bin is in your PATH. Typically on CentOS (and most Linux distributions), /usr/local/bin is already in PATH, but if not, you can update it in your shell config (e.g., ~/.bashrc):
```
export PATH="/usr/local/bin:$PATH"
```
Then verify again:
```
cmake --version
```
You should see:

cmake version 3.28.4

Alternative: Using a Different Install Path or “Alternatives” System

1.	Install to a custom directory: If you do not want to remove the system CMake but still want a newer version, just install to a different prefix, e.g.:
```
./bootstrap --prefix=/opt/cmake/3.28.4
make -j$(nproc)
sudo make install
```

Then you can add /opt/cmake/3.28.4/bin to your PATH whenever you need the newer version.

2.	Use the alternatives command: You can configure the system to treat multiple CMake binaries in parallel. For example:

```
sudo alternatives --install /usr/bin/cmake cmake /opt/cmake/3.28.4/bin/cmake 1
sudo alternatives --install /usr/bin/cmake cmake /usr/bin/cmake3.26.5        2
```

Then you can switch between them via:
```
sudo alternatives --config cmake
```
Summary

1.	Remove old CMake (optional): sudo dnf remove cmake
Install build dependencies:
```
sudo dnf groupinstall "Development Tools"
sudo dnf install openssl-devel
```

3.	Download & unpack CMake 3.28.4:
```
wget https://github.com/Kitware/CMake/releases/download/v3.28.4/cmake-3.28.4.tar.gz
tar -zxvf cmake-3.28.4.tar.gz
cd cmake-3.28.4
```

4.	Build & install:
```
./bootstrap --prefix=/usr/local
make -j$(nproc)
sudo make install
```

5.	Ensure it’s in PATH and verify:
```
cmake --version
```


That’s it—now you have CMake 3.28.4 running on your CentOS 8 system.