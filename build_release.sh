#!/usr/bin/env bash
# Visualizing the dependencies graph.
# https://cmake.org/cmake/help/latest/module/CMakeGraphVizOptions.html#module:CMakeGraphVizOptions
# Debug vs Release:
# https://stackoverflow.com/a/7725055

cmake -DCMAKE_BUILD_TYPE=Release -B ./build_release &&
    make -j32 -C ./build_release --silent &&
    # Congratulate the user!
    echo 'Congrats, UCSB is ready for use!'
