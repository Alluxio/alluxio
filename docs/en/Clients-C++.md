---
layout: global
title: C++ Client
nickname: C++ Java
group: Clients
priority: 1
---

* Table of Contents
{:toc}

Alluxio has a C++ Client module in branch `cpp` for  interacting with Alluxio through JNI. The 
C++ client exposes an API similar to  the [native Java API](Clients-Java-Native.html).

# Environment variables configuration

$JAVA_HOME must be set. Java 8 is needed in this version.

Add path "$JAVA_HOME/jre/lib/i386/server"(32bit) or "$JAVA_HOME/jre/lib/amd64/server"(64bit)or 
"$JAVA_HOME/jre/lib/server"(Darwin) to "LD_LIBRARY_PATH" depending on your OS platform version.
 
Append $ALLUXIO_CLIENT_CLASSPATH to CLASSPATH in libexec/alluxio-config.sh.
```
. alluxio-config.sh
export CLASSPATH=$CLASSPATH:$ALLUXIO_CLIENT_CLASSPATH
```

# Install Cpp module

Use cmake to build library. After running below commands, You will get a static library `liballuxio1.0.a`, 
a shared link library `liballuxio1.0.so`(linux) or `liballuxio1.0.dylib` (macOS) in `cpp/build/src`.                   
```
cd $ALLUXIO_HOME/cpp
mkdir build
cd build
cmake ../
make
```
  
Build library of cpp module by mvn. Both the static library and the shared library are generated at 
`cpp/target/native/src`.
```
cd ${ALLUXIO_HOME}/cpp
mvn clean install
```
 
Compile and run your application. Here is a simple example to link library to your program `test.cpp`. 
Notice that JNI included paths are needed. Or you can use CMake or autotools to build your project. 
The executable file `test` will be generated. 
```
g++ test.cpp liballuxio.a  
-I${ALLUXIO_HOME}/cpp/include -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux 
-L${JRE_HOME}/lib/amd64/server -lpthread -ljvm -o test
# run the application
./test
```

# Example Usage
```
#include <fileSystem.h>

using namespace alluxio;

void info(Status stus) {
  printf("%s\n", stus.ToString());
}

int main(void){
  FileSystem* fileSystem = new FileSystem();
  
  FileOutStream* out;
  AlluxioURI* uri = new AlluxioURI("/foo");
  Status s = fileSystem->CreateFile(*uri, &out);
  if (s.ok()) {
    s = out->Write("Alluxio works with C++", 0, 22);
    if (s.ok()) {
      out->Close();
    } else {
      info(s);
    }
  } else {
    info(s);
  }
  delete out;
  
  FileInStream* in;
  s = fileSystem->OpenFile(*uri, &in);
  if (s.ok()) {
    char* inputBuffer = new char[100];
    size_t bytesRead = 0;
    s = in->Read(inputBuffer, 0, 22, &bytesRead);
    if (s.ok()) {
      printf("%s\n", inputBuffer);
      in->Close();
    } else {
      info(s);
    }
  } else {
    info(s);
  }
  delete in;
  delete inputBuffer;
  
  s = fileSystem->DeletePath(*uri);
  if(!s.ok()) {
    info(s);
  }
  
  delete uri;
  delete fileSystem;
  return 0;
}
```

 
