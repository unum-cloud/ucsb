from conans import ConanFile, CMake
from conans.tools import SystemPackageTool
import os


class UCSBConan(ConanFile):
    # Default source:
    # https://conan.io/center/
    # Remotes to add:
    # conan remote add conan-center https://api.bintray.com/conan/conan/conan-center

    name = "UCSB"
    version = "0.0.1"
    url = "https://github.com/unum-cloud/ucsb.git"

    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake", "cmake_paths", "cmake_find_package"
    default_options = (
        # "arrow:with_cuda=True",
        # "arrow:with_csv=True",
        # "arrow:parquet=True",
        # "arrow:with_json=True",
        # "arrow:with_protobuf=True",
        # "arrow:with_jemalloc=True",
    )

    def configure(self):
        # To avoid linking problems - link to C++11 ABI
        # https://github.com/conan-io/conan/issues/2115#issuecomment-353020236
        # https://docs.conan.io/en/latest/howtos/manage_gcc_abi.html
        if self.settings.compiler.libcxx == "libstdc++":
            raise Exception("This package is only compatible with libstdc++11")

    def build(self):

        # It's recommended to use CMake.
        # https://docs.conan.io/en/latest/reference/conanfile/methods.html#build

        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def system_requirements(self):
        # If you nees some other package, missing in Conan-Center,
        # use this method to invoke the "System Package Tool".
        # https://docs.conan.io/en/latest/reference/conanfile/methods.html?#system-requirements
        # SystemPackageTool().install("libtorch")
        # SystemPackageTool().install("libcudnn8=8.1.1.*-1+cuda11.3")
        # {system_requirements}
        pass

    def requirements(self):
        # Your library dependencies go here.
        # https://docs.conan.io/en/latest/reference/conanfile/methods.html#requirements

        self.requires("fmt/8.1.1")
        self.requires("benchmark/1.6.0")
        self.requires("nlohmann_json/3.10.4")
        # self.requires("rocksdb/6.20.3")
        # self.requires("leveldb/1.23")
        self.requires("mongo-cxx-driver/3.6.6")
        self.requires("redis-plus-plus/1.3.3")
        self.requires("lmdb/0.9.29")
        self.requires("snappy/1.1.9")
        self.requires("lz4/1.9.3")
        self.requires("zstd/1.5.2")
        self.requires("zlib/1.2.11")
        self.requires("bzip2/1.0.8")
        self.requires("argparse/2.9")
        pass
