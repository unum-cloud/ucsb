if [ $1 = "install" ]; then
    rm -rf wiredtiger &&
        git clone https://github.com/wiredtiger/wiredtiger.git &&
        cd wiredtiger && mkdir build && cd build &&
        cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-Wno-maybe-uninitialized" -DCMAKE_CXX_FLAGS="-Wno-unused-variable" .. && cmake --build . &&
        sudo make install &&
        echo "Successfully installed!"
elif [ $1 = "uninstall" ]; then
    sudo xargs rm <./wiredtiger/build/install_manifest.txt &&
        echo "Successfully removed!"
fi
