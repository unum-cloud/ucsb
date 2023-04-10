#!/bin/bash
# Visualizing the dependencies graph.
# https://cmake.org/cmake/help/latest/module/CMakeGraphVizOptions.html#module:CMakeGraphVizOptions
# Debug vs Release:
# https://stackoverflow.com/a/7725055

cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DCMAKE_BUILD_TYPE=Debug -B ./build_debug &&
    make -j16 -C ./build_debug --silent &&
    # Congratulate the user!
    echo 'Congrats, UCSB is ready for use!'
