# Alluxio C++ API

### environment variables configuration
- $JAVA_HOME must be set
- Java 8 is needed
-  Append $ALLUXIO_CLIENT_CLASSPATH to CLASSPATH in libexec/alluxio-config.sh.
```
. alluxio-config.sh
export CLASSPATH=$CLASSPATH:$ALLUXIO_CLIENT_CLASSPATH
```

### A simple example to build the cpp project to dist dir
- Use cmake to build library
```
cd $ALLUXIO_HOME/cpp
mkdir build
cd build
cmake ../
make
```
  You will get a static link library `libfilesystem.a`, a shared link library 
`libsharedfilesystem.so` in cpp/build/src and execuable files in
cpp/bin, you can link .a or .so file to your own cpp project.
 
- Build library by mvn
 ```
 mvn clean install
 ```
 the link librarys are in cpp/target/native/src.
 
- Calling alluxio cpp API
```
#include <FileSystem.h>
```

### Run the test case function in FileSystemTest

- You can run execuable files mapping to different test cases directly 
```
cd $ALLUXIO_HOME/cpp/bin
./FileSystemTest
./FileRWTest
```
- Or use mvn tools to test all cases
```
mvn test
```