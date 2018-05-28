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
#include <vector>
#include <map>

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

class WorkerNetAddress {
 public:
  WorkerNetAddress() {}

  WorkerNetAddress(std::string host, int rpcPort,
      int dataPort, int webPort, std::string domainSocketPath) {
    mHost = host;
    mRpcPort = rpcPort;
    mDataPort = dataPort;
    mWebPort = webPort;
    mDomainSocketPath = domainSocketPath;
  }

  explicit WorkerNetAddress(jobject jNetAddress) {
    mHost = JniHelper::CallStringMethod(jNetAddress,
        "alluxio/wire/WorkerNetAddress", "getHost");
    mRpcPort = JniHelper::CallIntMethod(jNetAddress,
        "alluxio/wire/WorkerNetAddress", "getRpcPort");
    mDataPort = JniHelper::CallIntMethod(jNetAddress,
        "alluxio/wire/WorkerNetAddress", "getDataPort");
    mWebPort = JniHelper::CallIntMethod(jNetAddress,
        "alluxio/wire/WorkerNetAddress", "getWebPort");
    mDomainSocketPath = JniHelper::CallStringMethod(jNetAddress,
        "alluxio/wire/WorkerNetAddress", "getDomainSocketPath");
  }

  bool operator==(WorkerNetAddress address) {
    return (mHost.compare(address.getHost()) == 0) &&
        (mRpcPort == address.getRpcPort()) &&
        (mDataPort == address.getDataPort()) &&
        (mWebPort == address.getWebPort()) &&
        (mDomainSocketPath.compare(address.getDomainSocketPath()) == 0);
  }

  std::string getHost();
  int getRpcPort();
  int getDataPort();
  int getWebPort();
  std::string getDomainSocketPath();
  WorkerNetAddress* setHost(std::string host);
  WorkerNetAddress* setRpcPort(int rpcPort);
  WorkerNetAddress* setDataPort(int dataPort);
  WorkerNetAddress* setWebPort(int webPort);
  WorkerNetAddress* setDomainSocketPath(std::string domainSocketPath);

 private:
  std::string mHost = "";
  int mRpcPort;
  int mDataPort;
  int mWebPort;
  std::string mDomainSocketPath = "";
};

class BlockWorkerInfo {
 public:
  BlockWorkerInfo(WorkerNetAddress* netAddress,
      int64_t capacityBytes, int64_t usedBytes) {
    mNetAddress = netAddress;
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
  }

  explicit BlockWorkerInfo(jobject jBlockWorkerInfo) {
    jobject jNetAddress = JniHelper::CallObjectMethod(jBlockWorkerInfo,
        "alluxio/client/block/BlockWorkerInfo",
        "getNetAddress",
        "alluxio/wire/WorkerNetAddress");
    mNetAddress = new WorkerNetAddress(jNetAddress);
    mCapacityBytes = JniHelper::CallLongMethod(jBlockWorkerInfo,
        "alluxio/client/block/BlockWorkerInfo", "getCapacityBytes");
    mUsedBytes = JniHelper::CallLongMethod(jBlockWorkerInfo,
        "alluxio/client/block/BlockWorkerInfo", "getUsedBytes");
  }

  jobject ToJBlockWorkerInfo() {
    jobject jNetAddress = JniHelper::CreateObjectMethod(
        "alluxio/wire/WorkerNetAddress");
    JniHelper::CallObjectMethod(jNetAddress, "alluxio/wire/WorkerNetAddress",
        "setHost", "alluxio/wire/WorkerNetAddress", mNetAddress->getHost());
    JniHelper::CallObjectMethod(jNetAddress, "alluxio/wire/WorkerNetAddress",
        "setRpcPort", "alluxio/wire/WorkerNetAddress",
        mNetAddress->getRpcPort());
    JniHelper::CallObjectMethod(jNetAddress, "alluxio/wire/WorkerNetAddress",
        "setDataPort", "alluxio/wire/WorkerNetAddress",
        mNetAddress->getDataPort());
    JniHelper::CallObjectMethod(jNetAddress, "alluxio/wire/WorkerNetAddress",
        "setWebPort", "alluxio/wire/WorkerNetAddress",
        mNetAddress->getWebPort());
    JniHelper::CallObjectMethod(jNetAddress, "alluxio/wire/WorkerNetAddress",
        "setDomainSocketPath", "alluxio/wire/WorkerNetAddress",
        mNetAddress->getDomainSocketPath());
    jobject jBlockWorkerInfo = JniHelper::CreateObjectMethod(
        "alluxio/client/block/BlockWorkerInfo", jNetAddress,
        mCapacityBytes, mUsedBytes);
    return jBlockWorkerInfo;
  }

  WorkerNetAddress* getNetAddress();
  int64_t getCapacityBytes();
  int64_t getUsedBytes();

 private:
  WorkerNetAddress* mNetAddress;
  int64_t mCapacityBytes;
  int64_t mUsedBytes;
};

class CreateOptions : public JniObjectBase {
 public:
  static CreateOptions* getDefaultOptions() {
    jobject createOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/block/policy/options/CreateOptions", "defaults",
        "alluxio/client/block/policy/options/CreateOptions");
    return new CreateOptions(createOpt);
  }

  std::string getLocationPolicyClassName();
  int getDeterministicHashPolicyNumShards();
  CreateOptions* setLocationPolicyClassName(std::string name);
  CreateOptions* setDeterministicHashPolicyNumShards(int shards);

 private:
  explicit CreateOptions(jobject createOptions) :
    JniObjectBase(createOptions) {}
};

class GetWorkerOptions : public JniObjectBase {
 public:
  static GetWorkerOptions* getDefaultOptions() {
    jobject getWorkerOpt = JniHelper::CallStaticObjectMethod(
        "alluxio/client/block/policy/options/GetWorkerOptions", "defaults",
        "alluxio/client/block/policy/options/GetWorkerOptions");
    return new GetWorkerOptions(getWorkerOpt);
  }

  std::vector<BlockWorkerInfo>* getBlockWorkerInfos();
  int64_t getBlockId();
  int64_t getBlockSize();
  GetWorkerOptions* setBlockWorkerInfos(
      std::vector<BlockWorkerInfo>* blockWorkerInfos);
  GetWorkerOptions* setBlockId(int64_t blockId);
  GetWorkerOptions* setBlockSize(int64_t blockSize);

 private:
  explicit GetWorkerOptions(jobject getWorkerOptions) :
    JniObjectBase(getWorkerOptions) {}
  std::vector<BlockWorkerInfo> mBlockWorkerInfos;
};

class BlockLocationPolicy {
 public:
  virtual WorkerNetAddress* getWorker(GetWorkerOptions* wOptions) = 0;
};

class DeterministicHashPolicy :
    public BlockLocationPolicy, public JniObjectBase {
 public:
  explicit DeterministicHashPolicy(jobject policy) :
    JniObjectBase(policy) {}

  static DeterministicHashPolicy* getPolicy() {
    jobject policy = JniHelper::CreateObjectMethod(
        "alluxio/client/block/policy/DeterministicHashPolicy");
    return new DeterministicHashPolicy(policy);
  }

  virtual WorkerNetAddress* getWorker(GetWorkerOptions* wOptions);
};

class FileWriteLocationPolicy {
 public:
  virtual WorkerNetAddress* getWorkerForNextBlock(
    std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes) = 0;
};

class LocalFirstAvoidEvictionPolicy : public FileWriteLocationPolicy,
    public BlockLocationPolicy, public JniObjectBase {
 public:
  explicit LocalFirstAvoidEvictionPolicy(jobject policy) :
      JniObjectBase(policy) {
    jobject res = JniHelper::CallStaticObjectMethod(
        "alluxio/util/network/NetworkAddressUtils",
        "getClientHostName",
        "java/lang/String");
    mLocalHostName = JniHelper::JstringToString((jstring)res);
  }

  static LocalFirstAvoidEvictionPolicy* getPolicy() {
    jobject policy = JniHelper::CreateObjectMethod(
        "alluxio/client/file/policy/LocalFirstAvoidEvictionPolicy");
    return new LocalFirstAvoidEvictionPolicy(policy);
  }

  virtual WorkerNetAddress* getWorkerForNextBlock(
      std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes);

  virtual WorkerNetAddress* getWorker(GetWorkerOptions* wOptions) {
    return getWorkerForNextBlock(wOptions->getBlockWorkerInfos(),
        wOptions->getBlockSize());
  }

 private:
  std::string mLocalHostName;

  int64_t getAvailableBytes(BlockWorkerInfo workerInfo);
};

class LocalFirstPolicy : public FileWriteLocationPolicy,
    public BlockLocationPolicy, public JniObjectBase {
 public:
  explicit LocalFirstPolicy(jobject policy) :
      JniObjectBase(policy) {
    jobject res = JniHelper::CallStaticObjectMethod(
        "alluxio/util/network/NetworkAddressUtils",
        "getClientHostName",
        "java/lang/String");
    mLocalHostName = JniHelper::JstringToString((jstring)res);
  }

  static LocalFirstPolicy* getPolicy() {
    jobject policy = JniHelper::CreateObjectMethod(
        "alluxio/client/file/policy/LocalFirstPolicy");
    return new LocalFirstPolicy(policy);
  }

  virtual WorkerNetAddress* getWorkerForNextBlock(
      std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes);

  virtual WorkerNetAddress* getWorker(GetWorkerOptions* wOptions) {
    return getWorkerForNextBlock(wOptions->getBlockWorkerInfos(),
        wOptions->getBlockSize());
  }

 private:
  std::string mLocalHostName;
};

class MostAvailableFirstPolicy : public FileWriteLocationPolicy,
    public BlockLocationPolicy, public JniObjectBase {
 public:
  explicit MostAvailableFirstPolicy(jobject policy) :
      JniObjectBase(policy) {}

  static MostAvailableFirstPolicy* getPolicy() {
    jobject policy = JniHelper::CreateObjectMethod(
        "alluxio/client/file/policy/MostAvailableFirstPolicy");
    return new MostAvailableFirstPolicy(policy);
  }

  virtual WorkerNetAddress* getWorkerForNextBlock(
      std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes);

  virtual WorkerNetAddress* getWorker(GetWorkerOptions* wOptions) {
    return getWorkerForNextBlock(wOptions->getBlockWorkerInfos(),
        wOptions->getBlockSize());
  }
};

class RoundRobinPolicy : public FileWriteLocationPolicy,
    public BlockLocationPolicy, public JniObjectBase {
 public:
  explicit RoundRobinPolicy(jobject policy) :
      JniObjectBase(policy) {}

  static RoundRobinPolicy* getPolicy() {
    jobject policy = JniHelper::CreateObjectMethod(
        "alluxio/client/file/policy/RoundRobinPolicy");
    return new RoundRobinPolicy(policy);
  }

  virtual WorkerNetAddress* getWorkerForNextBlock(
      std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes);

  virtual WorkerNetAddress* getWorker(GetWorkerOptions* wOptions) {
    WorkerNetAddress* address = mBlockLocationCache.find(
        wOptions->getBlockId())->second;
    if (address != NULL) { return address; }
    address = getWorkerForNextBlock(wOptions->getBlockWorkerInfos(),
        wOptions->getBlockSize());
    mBlockLocationCache.insert(std::make_pair(wOptions->getBlockId(), address));
    return address;
  }

 private:
  std::vector<BlockWorkerInfo>* mWorkerInfoList;
  int mIndex;
  bool mInitialized = false;
  std::map<int64_t, WorkerNetAddress*> mBlockLocationCache;

  BlockWorkerInfo* findBlockWorkerInfo(
      std::vector<BlockWorkerInfo>* workerInfoList, WorkerNetAddress* address);
};

class SpecificHostPolicy : public FileWriteLocationPolicy,
    public BlockLocationPolicy, public JniObjectBase {
 public:
  explicit SpecificHostPolicy(jobject policy, std::string hostname) :
      JniObjectBase(policy) {
    mHostName = hostname;
  }

  static SpecificHostPolicy* getPolicy(std::string hostname) {
    jobject policy = JniHelper::CreateObjectMethod(
        "alluxio/client/file/policy/SpecificHostPolicy", hostname);
    return new SpecificHostPolicy(policy, hostname);
  }

  virtual WorkerNetAddress* getWorkerForNextBlock(
      std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes);

  virtual WorkerNetAddress* getWorker(GetWorkerOptions* wOptions) {
    return getWorkerForNextBlock(wOptions->getBlockWorkerInfos(),
        wOptions->getBlockSize());
  }

 private:
  std::string mHostName;
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
