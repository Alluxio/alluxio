Alluxio C++ API

# A simple example to build the cpp project to dist dir
source ./alluxio-client-env.sh
./bootstrap  # if you are a maintainer
mkdir build
cd build
../configure --prefix=$(dirname $(pwd))/dist
make
make install

# Run the main function of FileSystemTest
cd ../dist/bin
./alluxiotest
