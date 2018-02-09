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

#ifndef CPP_INCLUDE_FILESYSTEM_H_
#define CPP_INCLUDE_FILESYSTEM_H_

#include "alluxioURI.h"
#include "fileOutStream.h"
#include "fileInStream.h"
#include "jniHelper.h"
#include <options.h>
#include <wire.h>

#include <list>
#include <vector>
#include <map>
#include <string>

using ::alluxio::AlluxioURI;
using ::alluxio::MountPointInfo;
using ::alluxio::URIStatus;
using ::alluxio::FileInStream;
using ::alluxio::FileOutStream;

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
  explicit FileSystem(jobject localObj);
  // Creates a directory with default option
  Status CreateDirectory(const AlluxioURI& path);
  // Creates a directory
  Status CreateDirectory(const AlluxioURI& path,
                         const CreateDirectoryOptions& options);
  // Creates a file with default option. Sets a ptr to an allocated
  // FileOutStream to outStream when Status::OK, otherwise returns status code
  // without setting outStream
  Status CreateFile(const AlluxioURI& path, FileOutStream** outStream);
  // Creates a file.
  Status CreateFile(const AlluxioURI& path, const CreateFileOptions& options,
                    FileOutStream** outStream);
  // Deletes a file or a directory with the default option
  Status DeletePath(const AlluxioURI& path);
  // Deletes a file or a directory
  Status DeletePath(const AlluxioURI& path, const DeleteOptions& options);
  // Checks whether a path exists in Alluxio space with the default option. The
  // result pointer will be set when Java API returns a primitive types
  Status Exists(const AlluxioURI& path, bool* result);
  // Checks whether a path exists in Alluxio space
  Status Exists(const AlluxioURI& path, const ExistsOptions& options,
                bool* result);
  // Evicts any data under the given path from Alluxio space, but does not
  // delete the data from the UFS. The metadata will still be present in
  // Alluxio space after this operation
  Status Free(const AlluxioURI& path);
  // Frees space
  Status Free(const AlluxioURI& path, const FreeOptions& options);
  // Gets the URIStatus object that represents the metadata of an Alluxio path
  // with the default option. The result pointer will be set when Java API
  // returns a primitive type of URIStatus
  Status GetStatus(const AlluxioURI& path, URIStatus** result);
  // Gets the URIStatus object that represents the metadata of an Alluxio path
  Status GetStatus(const AlluxioURI& path, const GetStatusOptions& options,
                   URIStatus** result);
  // Convenience method for ListStatus with the default options. The result
  // pointer will be set when Java API returns a primitive types of List of
  // URIStatus
  Status ListStatus(const AlluxioURI& path, std::vector<URIStatus>* result);
  // If the path is a directory, returns the URIStatus of all the direct
  // entries in it. Otherwise returns a list with a single URIStatus element
  // for the file
  Status ListStatus(const AlluxioURI& path, const ListStatusOptions& options,
                    std::vector<URIStatus>* result);
  // Mounts a UFS subtree to the given Alluxio path with the default option
  Status Mount(const AlluxioURI& alluxioPath, const AlluxioURI& ufsPath);
  // Mounts a UFS subtree to the given Alluxio path
  Status Mount(const AlluxioURI& alluxioPath, const AlluxioURI& ufsPath,
               const MountOptions& options);
  // Lists all mount points and their corresponding under storage addresses
  Status GetMountTable(std::map<std::string, MountPointInfo>* result);
  // Opens a file for reading with the default option. Sets a ptr to an
  // allocated FileInStream to inStream when Status::OK, returns status code
  // otherwise without setting inStream
  Status OpenFile(const AlluxioURI& path, FileInStream** inStream);
  // Opens a file for reading
  Status OpenFile(const AlluxioURI& path, const OpenFileOptions& options,
                  FileInStream** inStream);
  // Renames an existing Alluxio path to another Alluxio path in Alluxio with
  // the default option
  Status Rename(const AlluxioURI& src, const AlluxioURI& dst);
  // Renames an existing Alluxio path to another Alluxio path in Alluxio
  Status Rename(const AlluxioURI& src, const AlluxioURI& dst,
                const RenameOptions& options);
  // Sets any number of a path's attributes with the default option
  Status SetAttribute(const AlluxioURI& path);
  // Sets any number of a path's attributes
  Status SetAttribute(const AlluxioURI& path,
                      const SetAttributeOptions& options);
  // Unmounts a UFS subtree identified by the given Alluxio path with the
  // default option
  Status Unmount(const AlluxioURI& path);
  // Unmounts a UFS subtree identified by the given Alluxio path
  Status Unmount(const AlluxioURI& path, const UnmountOptions& options);
  // closes FileSystem, releases JNI resource
  void closeFileSystem();
  ~FileSystem();

 private:
  jobject filesystem;
  JniHelper::LocalRefMapType localRefs;
  jobject createAlluxioURI(const std::string& path);
  Status callJniBydefaultOption(const AlluxioURI& path,
                                const std::string& methodName);
  Status callJniBydefaultOption(const AlluxioURI& src, const AlluxioURI& dst,
                                const std::string& methodName);
  Status callJniByOption(const AlluxioURI& path, const std::string& methodName,
                         const jobject option);
  Status callJniByOption(const AlluxioURI& src, const AlluxioURI& dst,
                         const std::string& methodName, const jobject option);
};

}  // namespace alluxio

#endif  // CPP_INCLUDE_FILESYSTEM_H_
