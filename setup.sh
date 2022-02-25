#!/bin/bash

conan profile new --detect default
conan profile update settings.compiler.libcxx=libstdc++11 default
conan install . --build=missing --install-folder="cmake" -s compiler.libcxx=libstdc++11
