#!/bin/bash
# Visualizing the dependencies graph.
# https://cmake.org/cmake/help/latest/module/CMakeGraphVizOptions.html#module:CMakeGraphVizOptions
# Debug vs Release:
# https://stackoverflow.com/a/7725055

cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DHAVE_GNU_POSIX_REGEX=OFF -DCMAKE_BUILD_TYPE=Release -B ./build_release &&
        make -j$(nproc) -C ./build_release &&
    # Congratulate the user!
    echo 'Congrats, UCSB is ready for use!'
