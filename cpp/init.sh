rm -r build
rm -r bin
mkdir build
cd build
cmake ../
make
../bin/FileSystemTest
