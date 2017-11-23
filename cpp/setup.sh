source ./alluxio-client-env.sh
./bootstrap  # if you are a maintainer
mkdir build
cd build
../configure --prefix=$(dirname $(pwd))/dist
make
make install
cd ../dist/bin
./alluxiotest
