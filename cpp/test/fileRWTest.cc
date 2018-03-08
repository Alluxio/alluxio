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

#include "fileSystem.h"
#include "localAlluxioCluster.h"

using ::alluxio::FileSystem;
using ::alluxio::LocalAlluxioCluster;

void ReadFileTest(FileSystem* fileSystem, const char* path,
                  std::string write_data) {
  FileInStream* in;
  AlluxioURI* uri = new AlluxioURI(path);
  Status s = fileSystem->OpenFile(*uri, &in);
  delete uri;
  assert(s.ok());

  int bufferSize = 100;
  char* inputBuffer =  reinterpret_cast<char*>(calloc(bufferSize, 1));
  size_t bytesRead = bufferSize;
  int pos = 0;
  while (bytesRead == bufferSize) {
    memset(inputBuffer, 0, sizeof(inputBuffer));
    Status res = in->Read(inputBuffer, 0, bufferSize, &bytesRead);
    if (bytesRead > 0) {
      if (!res.ok()) {
        in->Close();
        assert(res.ok());
        return;
      }
      assert(std::string(inputBuffer) == write_data.substr(pos, bufferSize));
      pos += bytesRead;
    }
  }
  assert(pos == write_data.length());
  in->Close();
}

std::string randomString(int length) {
  std::string res = "";
  srand((unsigned)time(NULL));
  unsigned int base;
  int temp = rand_r(&base) % 3;
  for (int i = 0 ; i < length; i ++) {
    if (temp == 0) {
      res += ('A' + rand_r(&base) % ('Z' - 'A' + 1));
    } else if (temp == 1) {
      res += ('a' + rand_r(&base) % ('z' - 'a' + 1));
    } else {
      res += ('0' + rand_r(&base) % ('9' - '0' + 1));
    }
  }
  return res;
}

void WriteFileTest(FileSystem* fileSystem, const char* path, std::string* res) {
  FileOutStream* out;
  AlluxioURI* uri = new AlluxioURI(path);
  Status s = fileSystem->CreateFile(*uri, &out);
  delete uri;
  assert(s.ok());
  std::string writeData;
  for (int i = 0; i < 10; i ++) {
    writeData = randomString(100);
    s = out->Write(writeData.c_str(), 0, 100);
    (*res) += writeData;
    if (!s.ok()) {
      assert(s.ok());
      return;
    }
  }
  out->Close();
}

// Tests fileSystem operations including reading and writing
int main(void) {
  FileSystem* fileSystem;
  LocalAlluxioCluster* miniCluster = new LocalAlluxioCluster();
  miniCluster->start();
  miniCluster->getClient(&fileSystem);
  std::string res;
  // Tests read
  WriteFileTest(fileSystem, "/RW", &res);
  // Tests write
  ReadFileTest(fileSystem, "/RW", res);
  delete miniCluster;

  Status status = JniHelper::AlluxioExceptionCheck();
  assert(status.ok());
  return 0;
}
