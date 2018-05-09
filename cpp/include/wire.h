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

#ifndef CPP_INCLUDE_WIRE_H_
#define CPP_INCLUDE_WIRE_H_

#include "jniHelper.h"
#include <string>

using ::jnihelper::JniHelper;

namespace alluxio {
// The base options class of filesystem options
class JniObjectBase {
 public:
  explicit JniObjectBase(jobject localObj) {
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
  JniObjectBase* SetMemberValue(const std::string& className,
                                const std::string& methodName, T t) {
    jobject result = JniHelper::CallObjectMethod(options, className, methodName,
                                                 className, t);
    JniHelper::DeleteObjectRef(result);
    return this;
  }
};

class ReadType {
 public:
  explicit ReadType(int mValue) {
    value = mValue;
    rType = RType(value);
  }

  int getValue() { return value; }

  jobject tojReadType() {
    JNIEnv* env = JniHelper::GetEnv();
    jclass clRType = env->FindClass("alluxio/client/ReadType");
    jfieldID id = NULL;
    switch (value) {
    case 1:
      id = env->GetStaticFieldID(clRType, "NO_CACHE",
                                 "Lalluxio/client/ReadType;");
      break;
    case 2:
      id = env->GetStaticFieldID(clRType, "CACHE",
                                 "Lalluxio/client/ReadType;");
      break;
    case 3:
      id = env->GetStaticFieldID(clRType, "CACHE_PROMOTE",
                                 "Lalluxio/client/ReadType;");
      break;
    }
    return env->GetStaticObjectField(clRType, id);
  }

 private:
  enum RType {
    NO_CACHE = 1,
    CACHE = 2,
    CACHE_PROMOTE = 3
  } rType;
  int value;
};

class LoadMetadataType {
 public:
  explicit LoadMetadataType(int mValue) {
    value = mValue;
    type = LMdType(value);
  }

  int getValue() { return value; }

  jobject tojLoadMetadataType() {
    JNIEnv* env = JniHelper::GetEnv();
    jclass clLMdType = env->FindClass("alluxio/wire/LoadMetadataType");
    jfieldID id = NULL;
    switch (value) {
    case 0:
      id = env->GetStaticFieldID(clLMdType, "Never",
                                 "Lalluxio/wire/LoadMetadataType;");
      break;
    case 1:
      id = env->GetStaticFieldID(clLMdType, "Once",
                                 "Lalluxio/wire/LoadMetadataType;");
      break;
    case 2:
      id = env->GetStaticFieldID(clLMdType, "Always",
                                 "Lalluxio/wire/LoadMetadataType;");
      break;
    }
    return env->GetStaticObjectField(clLMdType, id);
  }

 private:
  enum LMdType {
    Never = 0,
    Once = 1,
    Always = 2
  } type;
  int value;
};

class Bits {
 public:
  explicit Bits(std::string str) {
    bitStr = str;
    if (str.compare("---") == 0) {
      _bits = _Bits::NONE;
    }
    if (str.compare("--x") == 0) {
      _bits = _Bits::EXECUTE;
    }
    if (str.compare("-w-") == 0) {
      _bits = _Bits::WRITE;
    }
    if (str.compare("-wx") == 0) {
      _bits = _Bits::WRITE_EXECUTE;
    }
    if (str.compare("r--") == 0) {
      _bits = _Bits::READ;
    }
    if (str.compare("r-x") == 0) {
      _bits = _Bits::READ_EXECUTE;
    }
    if (str.compare("rw-") == 0) {
      _bits = _Bits::READ_WRITE;
    }
    if (str.compare("rwx") == 0) {
      _bits = _Bits::ALL;
    }
  }
  jobject tojBits() {
    JNIEnv* env = JniHelper::GetEnv();
    jclass clBits = env->FindClass("alluxio/security/authorization/Mode$Bits");
    jfieldID id = NULL;
    if (bitStr.compare("---") == 0) {
      id = env->GetStaticFieldID(clBits, "NONE",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    if (bitStr.compare("--x") == 0) {
      id = env->GetStaticFieldID(clBits, "EXECUTE",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    if (bitStr.compare("-w-") == 0) {
      id = env->GetStaticFieldID(clBits, "WRITE",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    if (bitStr.compare("-wx") == 0) {
      id = env->GetStaticFieldID(clBits, "WRITE_EXECUTE",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    if (bitStr.compare("r--") == 0) {
      id = env->GetStaticFieldID(clBits, "READ",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    if (bitStr.compare("r-x") == 0) {
      id = env->GetStaticFieldID(clBits, "READ_EXECUTE",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    if (bitStr.compare("rw-") == 0) {
      id = env->GetStaticFieldID(clBits, "READ_WRITE",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    if (bitStr.compare("rwx") == 0) {
      id = env->GetStaticFieldID(clBits, "ALL",
                                 "Lalluxio/security/authorization/Mode$Bits;");
    }
    return env->GetStaticObjectField(clBits, id);
  }
  std::string toString() { return bitStr; }

 private:
  enum _Bits {
    NONE,
    EXECUTE,
    WRITE,
    WRITE_EXECUTE,
    READ,
    READ_EXECUTE,
    READ_WRITE,
    ALL} _bits;
  std::string bitStr;
};

class Mode : public JniObjectBase {
 public:
  explicit Mode(jobject mode) :
    JniObjectBase(mode) {}
  jobject getOpt() { return options; }
  static Mode* getDefaultMode() {
      jobject mode = JniHelper::CallStaticObjectMethod(
          "alluxio/security/authorization/Mode", "defaults",
          "alluxio/security/authorization/Mode");
      return new Mode(mode);
  }
  static Mode* createNoAccess() {
      jobject mode = JniHelper::CallStaticObjectMethod(
          "alluxio/security/authorization/Mode", "createNoAccess",
          "alluxio/security/authorization/Mode");
      return new Mode(mode);
  }
  static Mode* createFullAccess() {
      jobject mode = JniHelper::CallStaticObjectMethod(
          "alluxio/security/authorization/Mode", "createFullAccess",
          "alluxio/security/authorization/Mode");
      return new Mode(mode);
  }
  Bits* getOwnerBits();
  void setOwnerBits(Bits* bits);
  Bits* getGroupBits();
  void setGroupBits(Bits* bits);
  Bits* getOtherBits();
  void setOtherBits(Bits* bits);
};

class WriteType {
 public:
  explicit WriteType(int mValue) {
    value = mValue;
    wType = WType(value);
  }
  int getValue() { return value; }
  bool isAsync() { return value == 5; }
  bool isCache() {
    return (value == 1) || (value == 2) || (value == 3) || (value == 5);
  }
  bool isThrough() {
    return (value == 3) || (value == 4);
  }
  jobject tojWriteType() {
    JNIEnv* env = JniHelper::GetEnv();
    jclass clWriteType = env->FindClass("alluxio/client/WriteType");
    jfieldID id = NULL;
    switch (getValue()) {
    case 1:
      id = env->GetStaticFieldID(clWriteType, "MUST_CACHE",
                                 "Lalluxio/client/WriteType;");
      break;
    case 2:
      id = env->GetStaticFieldID(clWriteType, "TRY_CACHE",
                                 "Lalluxio/client/WriteType;");
      break;
    case 3:
      id = env->GetStaticFieldID(clWriteType, "CACHE_THROUGH",
                                 "Lalluxio/client/WriteType;");
      break;
    case 4:
      id = env->GetStaticFieldID(clWriteType, "THROUGH",
                                 "Lalluxio/client/WriteType;");
      break;
    case 5:
      id = env->GetStaticFieldID(clWriteType, "ASYNC_THROUGH",
                                 "Lalluxio/client/WriteType;");
      break;
    case 6:
      id = env->GetStaticFieldID(clWriteType, "NONE",
                                 "Lalluxio/client/WriteType;");
      break;
    }
    return env->GetStaticObjectField(clWriteType, id);
  }

 private:
  enum WType {
    MUST_CACHE = 1,
    TRY_CACHE = 2,
    CACHE_THROUGH = 3,
    THROUTH = 4,
    ASYNC_THROUTH = 5,
    NONE = 6
  } wType;
  int value;
};

class TtlAction {
 public:
  explicit TtlAction(std::string str) {
    ttlAct = TtlAct::DELETE;
    if (str.compare("FREE") == 0) { ttlAct = TtlAct::FREE; }
  }
  jobject tojTtlAction() {
    JNIEnv* env = JniHelper::GetEnv();
    jclass clTtlAction = env->FindClass("alluxio/wire/TtlAction");
    jfieldID id = env->GetStaticFieldID(clTtlAction, "DELETE",
                                        "Lalluxio/wire/TtlAction;");
    if (ttlAct == TtlAct::FREE) {
      id = env->GetStaticFieldID(clTtlAction, "FREE",
                                 "Lalluxio/wire/TtlAction;");
    }
    return env->GetStaticObjectField(clTtlAction, id);
  }
  bool isFree() {
    if (ttlAct == TtlAct::FREE) { return true; }
    return false;
  }

 private:
  enum TtlAct {
    DELETE,
    FREE
  } ttlAct;
};

class MountPointInfo {
 public:
  explicit MountPointInfo(jobject mountPointInfo) {
    jMountPointInfo = mountPointInfo;
  }

  MountPointInfo(const MountPointInfo& s) {
    jMountPointInfo = JniHelper::GetEnv()->NewGlobalRef(s.jMountPointInfo);
  }

  void operator = (const MountPointInfo& s) {
    jMountPointInfo = JniHelper::GetEnv()->NewGlobalRef(s.jMountPointInfo);
  }

  ~MountPointInfo() {
    JniHelper::DeleteObjectRef(jMountPointInfo);
  }

  std::string ToString() {
    try {
      return JniHelper::CallStringMethod(jMountPointInfo,
                                         "alluxio/wire/MountPointInfo",
                                         "toString");
    } catch (std::string e ) {
      return "";
    }
  }

 private:
  jobject jMountPointInfo;
};

class URIStatus {
 public:
  explicit URIStatus(jobject URIStatus) {
    jURIStatus = URIStatus;
  }

  explicit URIStatus(const URIStatus& s) {
    jURIStatus = JniHelper::GetEnv()->NewGlobalRef(s.jURIStatus);
  }

  void operator = (const URIStatus& s) {
    jURIStatus = JniHelper::GetEnv()->NewGlobalRef(s.jURIStatus);
  }

  ~URIStatus() {
    JniHelper::DeleteObjectRef(jURIStatus);
  }

  std::string ToString() {
    try {
      return JniHelper::CallStringMethod(jURIStatus,
                                         "alluxio/client/file/URIStatus",
                                         "toString");
    } catch (std::string e ) {
      return "";
    }
  }

 private:
  jobject jURIStatus;
};

}  // namespace alluxio

#endif  // CPP_INCLUDE_WIRE_H_
