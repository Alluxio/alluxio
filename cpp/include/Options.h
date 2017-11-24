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

#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>

#include "JNIHelper.h"

using namespace jnihelper;

namespace alluxio {

// The base options class of filesystem options
class JniObjectBase {

 public:
  JniObjectBase(jobject localObj) {
    options = localObj;
  }

  ~JniObjectBase() {
    JniHelper::DeleteObjectRef(options);
  }

  const jobject& getOptions() const {
    return options;
  }

 protected:
  jobject options;

  template<typename T>
  JniObjectBase& SetMemberValue(const std::string& className,
                                const std::string& methodName, T t) {
    jobject result = JniHelper::CallObjectMethod(options, className, methodName,
    											 className, t);
    options = JniHelper::GetEnv()->NewGlobalRef(result);
    JniHelper::DeleteObjectRef(result);
    return *this;
  }
};

class CreateDirectoryOptions : public JniObjectBase {

 public:
  static CreateDirectoryOptions* getDefaultOptions() {
    jobject createDirectoryOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/file/options/CreateDirectoryOptions", "defaults",
        "alluxio/client/file/options/CreateDirectoryOptions");
    return new CreateDirectoryOptions(createDirectoryOpt);
  }

 private:
  CreateDirectoryOptions(jobject createDirectoryOptions) :
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

 private:
  CreateFileOptions(jobject createFileOptions) :
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

 private:
  DeleteOptions(jobject deleteOptions) :
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

 private:
  ExistsOptions(jobject existsOptions) :
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

 private:
  FreeOptions(jobject existsOptions) :
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

 private:
  ListStatusOptions(jobject ListStatusOptions) :
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

 private:
  MountOptions(jobject mountOptions) :
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

 private:
  OpenFileOptions(jobject openFileOptions) :
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
  RenameOptions(jobject renameOptions ) :
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

 private:
  SetAttributeOptions(jobject setAttributeOptions) :
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
  UnmountOptions(jobject unmountOptions) :
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

 private:
  GetStatusOptions(jobject getStatusOptions) :
    JniObjectBase(getStatusOptions) {}
};

} // namespace alluxio
