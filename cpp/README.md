# Alluxio C++ API

### environment variables configuration
- $JAVA_HOME must be set
- Add path "$JAVA_HOME/jre/lib/i386/server(32bit)" or "$JAVA_HOME/jre/lib/amd64/server(64bit)"
or "$JAVA_HOME/jre/lib/server"(if your os platform is Darwin) to "LD_LIBRARY_PATH" 
depending on your OS platform version
-  Append $ALLUXIO_CLIENT_CLASSPATH to CLASSPATH in libexec/alluxio-config.sh.
```
. alluxio-config.sh
export CLASSPATH=$CLASSPATH:$ALLUXIO_CLIENT_CLASSPATH
```

### A simple example to build the cpp project to dist dir
```
cd $ALLUXIO_HOME/cpp
mkdir build
cd build
cmake ../
make
```
- You will get a static link library `libfilesystem.a`, a shared link library 
libsharedfilesystem.so in cpp/build/src and an execuable file FileSystemTest in
cpp/bin, you can link .a or .so file to your own cpp project. when calling 
alluxio cpp API, you need to add .h file to your file
```
#include <FileSystem.h>
```

### Run the test case function in FileSystemTest
```
cd $ALLUXIO_HOME/cpp/bin
./FileSystemTest
```