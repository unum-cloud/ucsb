if [ $1 = "install" ]; then 
	rm -rf LevelDB &&
	git clone --recurse-submodules https://github.com/google/leveldb.git LevelDB &&
	cd LevelDB &&
	mkdir -p build && cd build &&
	cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build . &&
	sudo make install &&
	echo "Success install!!"
elif [ $1 = "uninstall" ]; then
	sudo rm -rf /usr/local/lib/libleveldb.a \
		/usr/local/include/leveldb \
		/usr/local/lib/cmake/leveldb/ \
		/usr/local/include/gmock \
		/usr/local/lib/libgmock.a \
		/usr/local/lib/libgmock_main.a \
		/usr/local/lib/pkgconfig/gmock.pc \
		/usr/local/lib/pkgconfig/gmock_main.pc &&
	sudo rm -rf /usr/local/lib/cmake/GTest/GTestTargets.cmake /usr/local/lib/cmake/GTest/GTestTargets-release.cmake \
		/usr/local/lib/cmake/GTest/GTestConfigVersion.cmake \
		/usr/local/lib/cmake/GTest/GTestConfig.cmake \
		/usr/local/include/gtest \
		/usr/local/lib/libgtest.a \
		/usr/local/lib/libgtest_main.a \
		/usr/local/lib/pkgconfig/gtest.pc \
		/usr/local/lib/pkgconfig/gtest_main.pc \
		/usr/local/lib/libbenchmark.a \
		/usr/local/lib/libbenchmark_main.a \
		/usr/local/include/benchmark \
		/usr/local/lib/cmake/benchmark/benchmarkConfig.cmake \
		/usr/local/lib/cmake/benchmark/benchmarkConfigVersion.cmake \
		/usr/local/lib/pkgconfig/benchmark.pc \
		/usr/local/lib/cmake/benchmark/benchmarkTargets.cmake \
		/usr/local/lib/cmake/benchmark/benchmarkTargets-release.cmake &&
	echo "Success removed!!"
fi
