if [ $1 = "install" ]; then
	rm -rf wiredtiger &&
		git clone --recurse-submodules https://github.com/wiredtiger/wiredtiger.git wiredtiger &&
		cd wiredtiger &&
		mkdir -p build && cd build &&
		cmake ../. && make &&
		sudo make install &&
		echo "Successfully installed!"
elif [ $1 = "remove" ]; then
	sudo cd ./wiredtiger/build &&
		xargs rm <install_manifest.txt &&
		echo "Successfully removed!"
fi
