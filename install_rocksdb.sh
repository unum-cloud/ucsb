VERSION=7.9.2

if [ $1 = "install" ]; then
	wget https://github.com/facebook/rocksdb/archive/refs/tags/v$VERSION.zip &&
		unzip v$VERSION.zip &&
		rm v$VERSION.zip &&
		cd rocksdb-$VERSION &&
		make static_lib &&
		sudo make install static_lib &&
		echo "Success Installed!!"
elif [ $1 = "uninstall" ]; then
	sudo rm -rf /usr/local/include/rocksdb \
		/usr/local/lib/librocksdb.a \
		/usr/local/lib/librocksdb.so.6.29.3 \
		/usr/local/lib/librocksdb.so.6.29 \
		/usr/local/lib/librocksdb.so.6 \
		/usr/local/lib/librocksdb.so \
		/usr/local/lib/pkgconfig/rocksdb.pc &&
		echo "Success Removed!!"
fi
