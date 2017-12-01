# Alluxio C++ API

### environment variables configuration
- $JAVA_HOME must been set
- Add jvm path "$JAVA_HOME/jre/lib/i386/server" or "$JAVA_HOME/jre/lib/amd64/server"
or "$JAVA_HOME/jre/lib/server"(if your os platform is Darwin) to "LD_LIBRARY_PATH" 
depend on your OS platform version
- $ALLUXIO_CLIENT_CLASSPATH is needed by this project, being set
in libexec/alluxio-config.sh, you must add this path to $CLASSPATH
```
. alluxio-config.sh
export CLASSPATH=$CLASSPATH:ALLUXIO_CLIENT_CLASSPATH
```

### A simple example to build the cpp project to dist dir
```
cd $ALLUXIO_HOME/cpp
mkdir build
cd build
cmake ../
make
```
- You will get a static link library libfilesystem.a, a shared link library 
libsharedfilesystem.so in cpp/build/src and an execuable file FileSystemTest in
cpp/bin, you can link .a or .so file to your own cpp project. when calling 
alluxio cpp API, you need to add this
```
#include <FileSystem.h>
```

### Run the test case function in FileSystemTest
```
cd $ALLUXIO_HOME/cpp/bin
./FileSystemTest
```