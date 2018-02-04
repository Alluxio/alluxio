/**
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

#include <assert.h>

#include "FileSystem.h"

using namespace alluxio;

void ReadFileTest(FileSystem* fileSystem, const char* path) {
  FileInStream* in;
  AlluxioURI* uri = new AlluxioURI(path);
  Status s = fileSystem->OpenFile(*uri, &in);
  delete uri;
  assert(s.ok());

  int bufferSize =100;
  char* inputBuffer = (char*)calloc(bufferSize, 1);
  size_t bytesRead = bufferSize;
  while (bytesRead == bufferSize) {
    Status res = in->Read(inputBuffer, 0, bufferSize, &bytesRead);
    if(! res.ok()) {
      in->Close();
      assert(res.ok());
      return;
    }
  }
  in->Close();
}

std::string randomString(int length) {
  std::string res = "";
  srand((unsigned)time(NULL));
  int temp = rand() % 3;
  for (int i = 0 ; i < length; i ++) {
    if (temp == 0) {
      res += ('A' + rand() % ('Z' - 'A' + 1));
    } else if (temp == 1) {
      res += ('a' + rand() % ('z' - 'a' + 1));
    } else {
      res += ('0' + rand() % ('9' - '0' + 1));
    }
  }
  return res;
}

void WriteFileTest(FileSystem* fileSystem, const char* path) {
  FileOutStream* out;
  AlluxioURI* uri = new AlluxioURI(path);
  Status s = fileSystem->CreateFile(*uri, &out);
  delete uri;
  assert(s.ok());
  std::string writeData;
  for(int i = 0; i < 1000; i ++) {
    writeData = randomString(100);
    s = out->Write(writeData.c_str(), 0, 100);
    if(!s.ok()) {
      assert(s.ok());
      return;
    }
  }
  out->Close();
}

jobject startLocalCluster(FileSystem** fileSystem) {
  JniHelper::Start();
  jobject miniCluster = JniHelper::CreateObjectMethod(
      "alluxio/master/LocalAlluxioCluster");
  JniHelper::CallVoidMethod(miniCluster,
                            "alluxio/master/AbstractLocalAlluxioCluster",
                            "initConfiguration");
  JniHelper::CallVoidMethod(miniCluster,
                            "alluxio/master/AbstractLocalAlluxioCluster",
                            "setupTest");
  JniHelper::CallVoidMethod(miniCluster,
                            "alluxio/master/LocalAlluxioCluster",
                            "startMasters");
  JniHelper::CallVoidMethod(miniCluster,
                            "alluxio/master/AbstractLocalAlluxioCluster",
                            "startWorkers");
  jobject jfileSystem = JniHelper::CallObjectMethod(miniCluster,
                                       "alluxio/master/LocalAlluxioCluster",
                                       "getClient",
                                       "alluxio/client/file/FileSystem");
  * fileSystem = new FileSystem(jfileSystem);
  return miniCluster;
}

// Tests fileSystem operations including reading and writing
int main(void) {
  FileSystem* fileSystem;
  jobject miniCluster = startLocalCluster(&fileSystem);
  // Tests read
  WriteFileTest(fileSystem, "/RW");
  // Tests write
  ReadFileTest(fileSystem, "/RW");

  JniHelper::CallVoidMethod(miniCluster,
                            "alluxio/master/AbstractLocalAlluxioCluster",
                            "stop");
  Status status = JniHelper::AlluxioExceptionCheck();
  assert(status.ok());

  JniHelper::DeleteObjectRef(miniCluster);

  return 0;
}
