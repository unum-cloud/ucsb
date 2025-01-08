It appears that CentOS Stream 8 has changed the way software collections (SCL) are handled, and the centos-release-scl package is no longer available. Instead, you can use the AppStream repository or other updated sources to install the required development tools.

Here’s how you can resolve the issue:

Step 1: Use dnf to List Available Devtoolset Packages

Run the following command to search for available devtoolset packages:

sudo dnf list devtoolset*

If devtoolset-11 or a similar package is listed, proceed to install it. If it’s not listed, proceed with the steps below.

Step 2: Enable the CodeReady Builder Repository

For CentOS Stream 8, the equivalent of PowerTools is crb (CodeReady Builder).

sudo dnf config-manager --set-enabled crb

Step 3: Install GCC 11 or Later Directly

If devtoolset-11 is not available, install GCC directly from the AppStream or from a suitable repository.
1.	Search for available GCC versions:

sudo dnf module list gcc-toolset

This will list the available versions of gcc-toolset (e.g., gcc-toolset-10, gcc-toolset-11, etc.).

	2.	Enable the module for gcc-toolset-11 (or the latest version):

sudo dnf module enable gcc-toolset-11


	3.	Install the gcc-toolset package:

sudo dnf install gcc-toolset-11

Step 4: Enable the Installed GCC Toolset

Activate the toolset for your session:

scl enable gcc-toolset-11 bash

Step 5: Verify GCC Version

Check the version of GCC to ensure it matches the installed version:

gcc --version

Alternative: Manual Installation of GCC

If the above steps fail or the repository doesn’t have the required packages, you can build GCC from source. This method is more involved but ensures you get the version you need.
1.	Download GCC Source:

wget https://ftp.gnu.org/gnu/gcc/gcc-11.3.0/gcc-11.3.0.tar.gz


	2.	Extract the Archive:

tar -xvzf gcc-11.3.0.tar.gz
cd gcc-11.3.0


	3.	Install Required Dependencies:

sudo dnf groupinstall "Development Tools"
sudo dnf install gcc-c++ gmp-devel mpfr-devel libmpc-devel


	4.	Build and Install GCC:

./configure --disable-multilib
make -j$(nproc)
sudo make install


	5.	Verify Installation:

gcc --version

Summary

If centos-release-scl is unavailable:
•	Enable crb (CodeReady Builder) and install gcc-toolset.
•	Alternatively, install GCC manually from source.