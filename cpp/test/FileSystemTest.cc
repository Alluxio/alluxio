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

void CreateDirectoryTest(FileSystem* fileSystem, const char* dirPath) {
  AlluxioURI* uri = new AlluxioURI(dirPath);
  Status status = fileSystem->CreateDirectory(*uri);
  delete uri;
  assert(status.ok());
}

void CreateFileTest(FileSystem* fileSystem, const char* path) {
  FileOutStream* outStream;
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->CreateFile(*uri, &outStream);
  delete uri;
  assert(status.ok());
}

void DeletePathTest(FileSystem* fileSystem, const char* path) {
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->DeletePath(*uri);
  delete uri;
  assert(status.ok());
}

void ExistTest(FileSystem* fileSystem, const char* path) {
  bool* res;
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->Exists(*uri, res);
  delete uri;
  assert(status.ok());
}

void OpenFileTest(FileSystem* fileSystem, const char* path) {
  FileInStream* inStream;
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->OpenFile(*uri,  &inStream);
  assert(status.ok());
}

void FreeTest(FileSystem* fileSystem, const char* path) {
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->Free(*uri);
  delete uri;
  assert(status.ok());
}

void GetStatusTest(FileSystem* fileSystem, const char* path) {
  URIStatus* result;
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->GetStatus(*uri, &result);
  delete uri;
  assert(status.ok());
}

void ListStatusTest(FileSystem* fileSystem, const char* path) {
  std::vector<URIStatus> result;
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->ListStatus(*uri, &result);
  delete uri;
  assert(status.ok());
}

void MountTest(FileSystem* fileSystem, const char* srcPath,
               const char* dirPath) {
  AlluxioURI* src = new AlluxioURI(srcPath);
  AlluxioURI* dir = new AlluxioURI(dirPath);
  Status status = fileSystem->Mount(*src, *dir);
  delete src;
  delete dir;
  assert(status.ok());
}

void UnmountTest(FileSystem* fileSystem, const char* path) {
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->Unmount(*uri);
  delete uri;
  assert(status.ok());
}

void GetMountTableTest(FileSystem* fileSystem) {
  std::map<std::string, MountPointInfo> result;
  Status status = fileSystem->GetMountTable(&result);
  assert(status.ok());
  std::map<std::string, MountPointInfo>::iterator it;
  for (it = result.begin(); it != result.end(); it ++) {
	std::string key = (std::string)it->first;
	MountPointInfo value = (MountPointInfo)it->second;
  }
}

void RenameTest(FileSystem* fileSystem, const char* srcPath,
                const char* dirPath) {
  AlluxioURI* src = new AlluxioURI(srcPath);
  AlluxioURI* dir = new AlluxioURI(dirPath);
  Status status = fileSystem->Rename(*src, *dir);
  delete src;
  delete dir;
  assert(status.ok());
}

void SetAttributeTest(FileSystem* fileSystem, const char* path)  {
  AlluxioURI* uri = new AlluxioURI(path);
  Status status = fileSystem->SetAttribute(*uri);
  delete uri;
  assert(status.ok());
}

void startMultiProcessCluster(FileSystem** filesystem,
                              JniHelper::LocalRefMapType& localRefs) {
  JniHelper::Start();
  jobject builder = JniHelper::CallStaticObjectMethod(
                        "alluxio/multi/process/MultiProcessCluster",
                        "newBuilder",
                        "alluxio/multi/process/MultiProcessCluster$Builder");
  localRefs[JniHelper::GetEnv()].push_back(builder);
  jobject miniCluster = JniHelper::CallObjectMethod(builder,
                            "alluxio/multi/process/MultiProcessCluster$Builder",
                            "build",
                            "alluxio/multi/process/MultiProcessCluster");
  localRefs[JniHelper::GetEnv()].push_back(miniCluster);
  JniHelper::CallVoidMethod(miniCluster,
                            "alluxio/multi/process/MultiProcessCluster",
                            "start");
  jobject jfileSystem = JniHelper::CallObjectMethod(miniCluster,
                            "alluxio/multi/process/MultiProcessCluster",
                            "getFileSystemClient",
                            "alluxio/client/file/FileSystem");
  * filesystem = new FileSystem(jfileSystem);

}

// Tests fileSystem operations without reading and writing
int main(void) {
  FileSystem* fileSystem;
  JniHelper::LocalRefMapType localRefs;

  startMultiProcessCluster(&fileSystem, localRefs);

  // Tests create directory
  CreateDirectoryTest(fileSystem, "/foo");

  CreateDirectoryTest(fileSystem, "/foo0");

  // Tests create file
  CreateFileTest(fileSystem, "/foo/foo1");

  // Test file Exist
  ExistTest(fileSystem, "/foo/foo1");

  // Tests get status
  GetStatusTest(fileSystem, "/foo/foo1");

  // Tests rename
  RenameTest(fileSystem, "/foo/foo1", "/foo/foo2");

  // Tests open file
  OpenFileTest(fileSystem, "/foo/foo2");

  // Tests list status
  CreateFileTest(fileSystem, "/foo/foo1");
  ListStatusTest(fileSystem, "/foo");

  // Tests mount
  MountTest(fileSystem, "/1", "/usr");
  MountTest(fileSystem, "/2", "/share");
  GetMountTableTest(fileSystem);
  UnmountTest(fileSystem, "/1");
  UnmountTest(fileSystem, "/2");
  GetMountTableTest(fileSystem);

  // Tests delete
  DeletePathTest(fileSystem, "/foo/foo1");
  DeletePathTest(fileSystem, "/foo/foo2");
  DeletePathTest(fileSystem, "/foo");
  DeletePathTest(fileSystem, "/foo0");

  JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);

  return 0;
}
