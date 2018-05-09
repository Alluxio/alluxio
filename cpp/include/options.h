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
#ifndef CPP_INCLUDE_OPTIONS_H_
#define CPP_INCLUDE_OPTIONS_H_

#include <wire.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <string>
#include <map>

using ::jnihelper::JniHelper;
using ::alluxio::JniObjectBase;
using ::alluxio::WriteType;

namespace alluxio {

class CreateDirectoryOptions : public JniObjectBase {
 public:
  static CreateDirectoryOptions* getDefaultOptions() {
    jobject createDirectoryOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/CreateDirectoryOptions", "defaults",
        "alluxio/client/file/options/CreateDirectoryOptions");
    return new CreateDirectoryOptions(createDirectoryOpt);
  }
  Mode* getMode();
  WriteType* getWriteType();
  bool isAllowExists();
  int64_t getTtl();
  TtlAction* getTtlAction();
  bool isRecursive();
  CreateDirectoryOptions* setAllowExists(bool allowExists);
  CreateDirectoryOptions* setMode(Mode* mode);
  CreateDirectoryOptions* setRecursive(bool recursive);
  CreateDirectoryOptions* setTtl(int64_t ttl);
  CreateDirectoryOptions* setTtlAction(TtlAction* ttlAction);
  CreateDirectoryOptions* setWriteType(WriteType* writeType);

 private:
  explicit CreateDirectoryOptions(jobject createDirectoryOptions) :
    JniObjectBase(createDirectoryOptions) {}
};

class CreateFileOptions : public JniObjectBase {
 public:
  static CreateFileOptions* getDefaultOptions() {
    jobject createFileOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/CreateFileOptions", "defaults",
        "alluxio/client/file/options/CreateFileOptions");
    return new CreateFileOptions(createFileOpt);
  }
  int64_t getBlockSizeBytes();
  // FileWriteLocationPolicy* getLocationPolicy();
  TtlAction* getTtlAction();
  Mode* getMode();
  int64_t getTtl();
  int getWriteTier();
  WriteType* getWriteType();
  std::string getLocationPolicyClass();
  bool isRecursive();
  CreateFileOptions* setBlockSizeBytes(int64_t blockSizeBytes);
// * setLocationPolicy(FileWriteLocationPolicy* locationPolicy);
  CreateFileOptions* setLocationPolicyClass(std::string className);
  CreateFileOptions* setMode(Mode* mode);
  CreateFileOptions* setRecursive(bool recursive);
  CreateFileOptions* setTtl(int64_t ttl);
  CreateFileOptions* setTtlAction(TtlAction* ttlAction);
  CreateFileOptions* setWriteTier(int writeTier);
  CreateFileOptions* setWriteType(WriteType* writeType);

 private:
  explicit CreateFileOptions(jobject createFileOptions) :
    JniObjectBase(createFileOptions) {}
};

class DeleteOptions : public JniObjectBase {
 public:
  static DeleteOptions* getDefaultOptions() {
    jobject deleteOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/DeleteOptions", "defaults",
        "alluxio/client/file/options/DeleteOptions");
    return new DeleteOptions(deleteOpt);
  }
  bool isRecursive();
  bool isAlluxioOnly();
  bool isUnchecked();
  DeleteOptions* setRecursive(bool recursive);
  DeleteOptions* setAlluxioOnly(bool alluxioOnly);
  DeleteOptions* setUnchecked(bool unchecked);

 private:
  explicit DeleteOptions(jobject deleteOptions) :
    JniObjectBase(deleteOptions) {}
};

class ExistsOptions : public JniObjectBase {
 public :
  static ExistsOptions* getDefaultOptions() {
    jobject existsOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/ExistsOptions", "defaults",
        "alluxio/client/file/options/ExistsOptions");
    return new ExistsOptions(existsOpt);
  }
  LoadMetadataType* getLoadMetadataType();
  ExistsOptions* setLoadMetadataType(LoadMetadataType* loadMetadataType);

 private:
  explicit ExistsOptions(jobject existsOptions) :
    JniObjectBase(existsOptions) {}
};

class FreeOptions : public JniObjectBase {
 public:
  static FreeOptions* getDefaultOptions() {
    jobject freeOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/FreeOptions", "defaults",
        "alluxio/client/file/options/FreeOptions");
    return new FreeOptions(freeOpt);
  }
  bool isForced();
  bool isRecursive();
  FreeOptions* setForced(bool forced);
  FreeOptions* setRecursive(bool recursive);

 private:
  explicit FreeOptions(jobject existsOptions) :
    JniObjectBase(existsOptions) {}
};

class ListStatusOptions : public JniObjectBase {
 public:
  static ListStatusOptions* getDefaultOptions() {
    jobject ListStatusOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/ListStatusOptions", "defaults",
        "alluxio/client/file/options/ListStatusOptions");
    return new ListStatusOptions(ListStatusOpt);
  }
  LoadMetadataType* getLoadMetadataType();
  ListStatusOptions* setLoadMetadataType(LoadMetadataType* loadMetadataType);

 private:
  explicit ListStatusOptions(jobject ListStatusOptions) :
    JniObjectBase(ListStatusOptions) {}
};

class MountOptions : public JniObjectBase {
 public:
  static MountOptions* getDefaultOptions() {
    jobject mountOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/MountOptions", "defaults",
        "alluxio/client/file/options/MountOptions");
    return new MountOptions(mountOpt);
  }
  bool isReadOnly();
  std::map<std::string, std::string> getProperties();
  bool isShared();
  MountOptions* setReadOnly(bool readOnly);
  MountOptions* setProperties(std::map<std::string, std::string> properties);
  MountOptions* setShared(bool shared);

 private:
  explicit MountOptions(jobject mountOptions) :
    JniObjectBase(mountOptions) {}
};

class OpenFileOptions : public JniObjectBase {
 public:
  static OpenFileOptions* getDefaultOptions() {
    jobject openFileOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/OpenFileOptions", "defaults",
        "alluxio/client/file/options/OpenFileOptions");
    return new OpenFileOptions(openFileOpt);
  }
  // FileWriteLocationPolicy getLocationPolicy();
  // FileWriteLocationPolicy getCacheLocationPolicy();
  std::string getLocationPolicyClass();
  std::string getCacheLocationPolicyClass();
  ReadType* getReadType();
  int getMaxUfsReadConcurrency();
  // BlockLocationPolicy getUfsReadLocationPolicy();
  std::string getUfsReadLocationPolicyClass();
  // OpenFileOptions* setLocationPolicy(FileWriteLocationPolicy locationPolicy);
// * setCacheLocationPolicy(FileWriteLocationPolicy locationPolicy);
  // OpenFileOptions* setUfsReadLocationPolicy(BlockLocationPolicy policy);
  OpenFileOptions* setLocationPolicyClass(std::string className);
  OpenFileOptions* setCacheLocationPolicyClass(std::string className);
  OpenFileOptions* setUfsReadLocationPolicyClass(std::string className);
  OpenFileOptions* setReadType(ReadType* readType);
  OpenFileOptions* setMaxUfsReadConcurrency(int maxUfsReadConcurrency);

 private:
  explicit OpenFileOptions(jobject openFileOptions) :
    JniObjectBase(openFileOptions) {}
};

class RenameOptions : public JniObjectBase {
 public:
  static RenameOptions* getDefaultOptions() {
    jobject renameOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/RenameOptions", "defaults",
        "alluxio/client/file/options/RenameOptions");
    return new RenameOptions(renameOpt);
  }

 private:
  explicit RenameOptions(jobject renameOptions) :
    JniObjectBase(renameOptions) {}
};

class SetAttributeOptions : public JniObjectBase {
 public:
  static SetAttributeOptions* getDefaultOptions() {
    jobject setAttributeOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/SetAttributeOptions", "defaults",
        "alluxio/client/file/options/SetAttributeOptions");
    return new SetAttributeOptions(setAttributeOpt);
  }
  bool getPinned();
  int64_t getTtl();
  TtlAction* getTtlAction();
  bool getPersisted();
  std::string getOwner();
  std::string getGroup();
  Mode* getMode();
  bool isRecursive();
  SetAttributeOptions* setPinned(bool pinned);
  SetAttributeOptions* setTtl(int64_t ttl);
  SetAttributeOptions* setTtlAction(TtlAction* ttlAction);
  SetAttributeOptions* setPersisted(bool persisted);
  Status setOwner(std::string owner);
  Status setGroup(std::string group);
  SetAttributeOptions* setMode(Mode* mode);
  SetAttributeOptions* setRecursive(bool recursive);

 private:
  explicit SetAttributeOptions(jobject setAttributeOptions) :
    JniObjectBase(setAttributeOptions) {}
};

class UnmountOptions : public JniObjectBase {
 public:
  static UnmountOptions* getDefaultOptions() {
    jobject unmountOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/UnmountOptions", "defaults",
        "alluxio/client/file/options/UnmountOptions");
    return new UnmountOptions(unmountOpt);
  }

 private:
  explicit UnmountOptions(jobject unmountOptions) :
    JniObjectBase(unmountOptions) {}
};

class GetStatusOptions : public JniObjectBase {
 public:
  static GetStatusOptions* getDefaultOptions() {
    jobject getStatusOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/GetStatusOptions", "defaults",
        "alluxio/client/file/options/GetStatusOptions");
    return new GetStatusOptions(getStatusOpt);
  }
  LoadMetadataType* getLoadMetadataType();
  GetStatusOptions* setLoadMetadataType(LoadMetadataType* loadMetadataType);

 private:
  explicit GetStatusOptions(jobject getStatusOptions) :
    JniObjectBase(getStatusOptions) {}
};

}  // namespace alluxio

#endif  // CPP_INCLUDE_OPTIONS_H_
