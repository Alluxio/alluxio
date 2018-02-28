# Alluxio C++ API

### Introduction
 The C++ client is implemented through JNI to native Java client. Generally the 
 performance of using java client directly is better than calling c++ client,   
 because JNI calling type needs to cross programming language barrier.
  
### Environment variables configuration
- $JAVA_HOME must be set
- Java 8 is needed.
- Add path "$JAVA_HOME/jre/lib/i386/server"(32bit) or "$JAVA_HOME/jre/lib/amd64/server"(64bit)or 
"$JAVA_HOME/jre/lib/server"(Darwin) to "LD_LIBRARY_PATH" depending on your OS platform version
- Append $ALLUXIO_CLIENT_CLASSPATH to CLASSPATH in libexec/alluxio-config.sh.
```
. alluxio-config.sh
export CLASSPATH=$CLASSPATH:$ALLUXIO_CLIENT_CLASSPATH
```

### Example
- Use cmake to build library
```
cd $ALLUXIO_HOME/cpp
mkdir build
cd build
cmake ../
make
```
  You will get a static library `liballuxio1.0.a`, a shared link library 
`liballuxio1.0.so`(linux) or `liballuxio1.0.dylib`(macOS) in cpp/build/src
 
- Build library of cpp module by mvn
 ```
 cd ${ALLUXIO_HOME}/cpp
 mvn clean install
 ```
 Both the static library and the shared library are generated at cpp/target/native/src
 
- Compile and run your application 
```
g++ test.cpp liballuxio.a  
-I${ALLUXIO_HOME}/cpp/include -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux 
-L${JRE_HOME}/lib/amd64/server -lpthread -ljvm -o test
# run the application
./test
```
 This is a simple example to link library to your program `test.cpp`. Notice 
 that JNI included paths are needed. Or you can use CMake or autotools to build 
 your project. The executable file `test` will be generated. 

- C++ example
```
#include <fileSystem.h>

using namespace alluxio;

int main(void){
  FileSystem* fileSystem = new FileSystem();
  AlluxioURI* uri = new AlluxioURI("/foo");
  FileOutStream* out;
  Status stus = fileSystem->CreateFile(*uri, &out);
  if (stus.ok) {
    out->Write("test write", 0, 10);
    out->Close();
  }
  delete uri;
  delete out;
  delete fileSystem;
  return 0;
}

```

### Test

- You can run executable files mapping to different test cases directly 
```
cd $ALLUXIO_HOME/cpp/build/test
./fileSystemTest
./fileRWTest
```
- Or use mvn tools to test all cases included in cpp module
```
cd ${ALLUXIO_HOME}/cpp
mvn test
```