if [ $1 = "install" ]; then
	rm -rf wiredtiger &&
		git clone --recurse-submodules https://github.com/wiredtiger/wiredtiger.git wiredtiger &&
		cd wiredtiger &&
		mkdir -p build && cd build &&
		cmake ../. && make &&
		sudo make install &&
		echo "Success install!!"
elif [ $1 = "uninstall" ]; then
	sudo cd ./wiredtiger/build &&
		xargs rm <install_manifest.txt &&
		echo "Success removed!!"
fi
