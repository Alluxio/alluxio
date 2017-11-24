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

#ifndef FILESYSTEM_H
#define FILESYSTEM_H

#include <list>
#include <map>
#include <vector>

#include "JNIHelper.h"
#include "Options.h"
#include "FileOutStream.h"
#include "FileInStream.h"
#include "Wire.h"

using namespace alluxio;

namespace alluxio {

// Basic file system interface supporting metadata operations and data
// operations
// Example:
//   FileSystem* fileSystem = new FileSystem();
//   fileSystem->CreateDirectory("/foo");
//   delete fileSystem;
class FileSystem {

 public:
  // Constructor of FileSystem
  FileSystem();
  // Creates a directory with default option
  Status CreateDirectory(const std::string& path);
  // Creates a directory
  Status CreateDirectory(const std::string& path,
                         const CreateDirectoryOptions& options);
  // Creates a file with default option. Sets a ptr to an allocated
  // FileOutStream to outStream when Status::OK, return status code otherwise
  // without setting outStream
  Status CreateFile(const std::string& path, FileOutStream** outStream);
  // Creates a file.
  Status CreateFile(const std::string& path, const CreateFileOptions& options,
                    FileOutStream** outStream);
  // Deletes a file or a directory with default option
  Status DeletePath(const std::string& path);
  // Deletes a file or a directory
  Status DeletePath(const std::string& path, const DeleteOptions& options);
  // Checks whether a path exists in Alluxio space with default option. The
  // result ptr will be set when Java API return a primitive types
  Status Exists(const std::string& path, bool* result);
  // Checks whether a path exists in Alluxio space
  Status Exists(const std::string& path,const ExistsOptions& options,
                bool* result);
  // Evicts any data under the given path from Alluxio space, but does not
  // delete the data from the UFS. The metadata will still be present in
  // Alluxio space after this operation
  Status Free(const std::string& path);
  // Frees space
  Status Free(const std::string& path, const FreeOptions& options);
  // Gets the URIStatus object that represents the metadata of an Alluxio path
  // with default option. The result ptr will be set when Java API return a
  // primitive types of URIStatus
  Status GetStatus(const std::string& path, URIStatus** result);
  // Gets the URIStatus object that represents the metadata of an Alluxio path
  Status GetStatus(const std::string& path, const GetStatusOptions& options,
                   URIStatus** result);
  // Convenience method for ListStatus with default options. The result ptr
  // will be set when Java API return a primitive types of List of URIStatus
  Status ListStatus(const std::string& path, std::vector<URIStatus>* result);
  // If the path is a directory, returns the URIStatus of all the direct
  // entries in it. Otherwise returns a list with a single URIStatus element
  // for the file
  Status ListStatus(const std::string& path, const ListStatusOptions& options,
                    std::vector<URIStatus>* result);
  // Mounts a UFS subtree to the given Alluxio path with default option
  Status Mount(const std::string& alluxioPath, const std::string& ufsPath);
  // Mounts a UFS subtree to the given Alluxio path
  Status Mount(const std::string& alluxioPath, const std::string& ufsPath,
               const MountOptions& options);
  // Lists all mount points and their corresponding under storage addresses
  Status GetMountTable(std::map<std::string, MountPointInfo>* result);
  // Opens a file for reading with default option. Sets a ptr to an allocated
  // FileInStream to inStream when Status::OK, return status code otherwise
  // without setting inStream
  Status OpenFile(const std::string& path, FileInStream** inStream);
  // Opens a file for reading
  Status OpenFile(const std::string& path, const OpenFileOptions& options,
                  FileInStream** inStream);
  // Renames an existing Alluxio path to another Alluxio path in Alluxio with
  // default option
  Status Rename(const std::string& src, const std::string& dst);
  // Renames an existing Alluxio path to another Alluxio path in Alluxio
  Status Rename(const std::string& src, const std::string& dst,
                const RenameOptions& options);
  // Sets any number of a path's attributes with default option
  Status SetAttribute(const std::string& path);
  // Sets any number of a path's attributes
  Status SetAttribute(const std::string& path,
                      const SetAttributeOptions& options);
  // Unmounts a UFS subtree identified by the given Alluxio path with default
  // option
  Status Unmount(const std::string& path);
  // Unmounts a UFS subtree identified by the given Alluxio path
  Status Unmount(const std::string& path, const UnmountOptions& options);
  // closes FileSystem, releases JNI resource
  void closeFileSystem();
  ~FileSystem();

 private:
  jobject filesystem;
  JniHelper::LocalRefMapType localRefs;
  jobject createAlluxioURI(const std::string& path);
  Status callJniBydefaultOption(const std::string& path,
                                const std::string& methodName);
  Status callJniBydefaultOption(const std::string& src, const std::string& dst,
                                const std::string& methodName);
  Status callJniByOption(const std::string& path, const std::string& methodName,
                         const jobject option);
  Status callJniByOption(const std::string& src, const std::string&dst,
                         const std::string& methodName, const jobject option);
};

} // namespace alluxio

#endif // FILESYSTEM_H
