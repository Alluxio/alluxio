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

#include <wire.h>
#include <algorithm>

using ::jnihelper::JniHelper;
using ::alluxio::Mode;
using ::alluxio::Bits;
using ::alluxio::WorkerNetAddress;
using ::alluxio::BlockWorkerInfo;
using ::alluxio::LocalFirstAvoidEvictionPolicy;
using ::alluxio::LocalFirstPolicy;
using ::alluxio::MostAvailableFirstPolicy;
using ::alluxio::RoundRobinPolicy;
using ::alluxio::CreateOptions;
using ::alluxio::GetWorkerOptions;
using ::alluxio::SpecificHostPolicy;
using ::alluxio::DeterministicHashPolicy;


Bits* Mode::getOwnerBits() {
  jobject jBits = JniHelper::CallObjectMethod(
      options, "alluxio/security/authorization/Mode",
      "getOwnerBits", "alluxio/security/authorization/Mode$Bits");
  std::string str = JniHelper::CallStringMethod(
      jBits, "alluxio/security/authorization/Mode$Bits", "toString");
  JniHelper::DeleteObjectRef(jBits);
  return new Bits(str);
}

void Mode::setOwnerBits(Bits* bits) {
  jobject jBits = bits->tojBits();
  JniHelper::CallVoidMethod(options, "alluxio/security/authorization/Mode",
                            "setOwnerBits", jBits);
  JniHelper::DeleteObjectRef(jBits);
}

Bits* Mode::getGroupBits() {
  jobject jBits = JniHelper::CallObjectMethod(
      options, "alluxio/security/authorization/Mode",
      "getGroupBits", "alluxio/security/authorization/Mode$Bits");
  std::string str = JniHelper::CallStringMethod(
      jBits, "alluxio/security/authorization/Mode$Bits", "toString");
  JniHelper::DeleteObjectRef(jBits);
  return new Bits(str);
}

void Mode::setGroupBits(Bits* bits) {
  jobject jBits = bits->tojBits();
  JniHelper::CallVoidMethod(options, "alluxio/security/authorization/Mode",
                            "setGroupBits", jBits);
  JniHelper::DeleteObjectRef(jBits);
}

Bits* Mode::getOtherBits() {
  jobject jBits = JniHelper::CallObjectMethod(
      options, "alluxio/security/authorization/Mode",
      "getOtherBits", "alluxio/security/authorization/Mode$Bits");
  std::string str = JniHelper::CallStringMethod(
      jBits, "alluxio/security/authorization/Mode$Bits", "toString");
  JniHelper::DeleteObjectRef(jBits);
  return new Bits(str);
}

void Mode::setOtherBits(Bits* bits) {
  jobject jBits = bits->tojBits();
  JniHelper::CallVoidMethod(options, "alluxio/security/authorization/Mode",
                            "setOtherBits", jBits);
  JniHelper::DeleteObjectRef(jBits);
}

std::string WorkerNetAddress::getHost() {
  return mHost;
}

int WorkerNetAddress::getRpcPort() {
  return mRpcPort;
}

int WorkerNetAddress::getDataPort() {
  return mDataPort;
}

int WorkerNetAddress::getWebPort() {
  return mWebPort;
}

std::string WorkerNetAddress::getDomainSocketPath() {
  return mDomainSocketPath;
}

WorkerNetAddress* WorkerNetAddress::setHost(std::string host) {
  mHost = host;
  return this;
}

WorkerNetAddress* WorkerNetAddress::setRpcPort(int rpcPort) {
  mRpcPort = rpcPort;
  return this;
}

WorkerNetAddress* WorkerNetAddress::setDataPort(int dataPort) {
  mDataPort = dataPort;
  return this;
}

WorkerNetAddress* WorkerNetAddress::setWebPort(int webPort) {
  mWebPort = webPort;
  return this;
}

WorkerNetAddress* WorkerNetAddress::setDomainSocketPath
(std::string domainSocketPath) {
  mDomainSocketPath = domainSocketPath;
  return this;
}

WorkerNetAddress* BlockWorkerInfo::getNetAddress() {
  return mNetAddress;
}

int64_t BlockWorkerInfo::getCapacityBytes() {
  return mCapacityBytes;
}

int64_t BlockWorkerInfo::getUsedBytes() {
  return mUsedBytes;
}

std::string CreateOptions::getLocationPolicyClassName() {
  return JniHelper::CallStringMethod(options,
      "alluxio/client/block/policy/options/CreateOptions",
      "getLocationPolicyClassName");
}

int CreateOptions::getDeterministicHashPolicyNumShards() {
  return JniHelper::CallIntMethod(options,
      "alluxio/client/block/policy/options/CreateOptions",
      "getDeterministicHashPolicyNumShards");
}

CreateOptions* CreateOptions::setLocationPolicyClassName(std::string name) {
  return reinterpret_cast<CreateOptions*>(SetMemberValue(
      "alluxio/client/block/policy/options/CreateOptions",
      "setLocationPolicyClassName", name));
}

CreateOptions* CreateOptions::setDeterministicHashPolicyNumShards(int shards) {
  return reinterpret_cast<CreateOptions*>(SetMemberValue(
      "alluxio/client/block/policy/options/CreateOptions",
      "setDeterministicHashPolicyNumShards", shards));
}

std::vector<BlockWorkerInfo>* GetWorkerOptions::getBlockWorkerInfos() {
  if (mBlockWorkerInfos.size() > 0) {
    return &mBlockWorkerInfos;
  } else {
    jobject jBlockWorkerInfos = JniHelper::CallObjectMethod(options,
      "alluxio/client/block/policy/options/GetWorkerOptions",
      "getBlockWorkerInfos",
      "java/lang/Iterable");
    JniHelper::DeleteClassName(jBlockWorkerInfos);
    JniHelper::CacheClassName(jBlockWorkerInfos, "java/util/List");
    int listSize = JniHelper::CallIntMethod(jBlockWorkerInfos,
        "java/util/List", "size");
    for (int i = 0; i < listSize; i++) {
      jobject item = JniHelper::CallObjectMethod(jBlockWorkerInfos,
          "java/util/List", "get", "java/lang/Object", i);
      BlockWorkerInfo* blockWorkerInfo = new BlockWorkerInfo(jBlockWorkerInfos);
      mBlockWorkerInfos.push_back(*blockWorkerInfo);
      JniHelper::DeleteObjectRef(item);
    }
    JniHelper::DeleteObjectRef(jBlockWorkerInfos);
  }
  return &mBlockWorkerInfos;
}

int64_t GetWorkerOptions::getBlockId() {
  return JniHelper::CallLongMethod(options,
      "alluxio/client/block/policy/options/GetWorkerOptions",
      "getBlockId");
}

int64_t GetWorkerOptions::getBlockSize() {
  return JniHelper::CallLongMethod(options,
      "alluxio/client/block/policy/options/GetWorkerOptions",
      "getBlockSize");
}

GetWorkerOptions* GetWorkerOptions::setBlockWorkerInfos(
    std::vector<BlockWorkerInfo>* blockWorkerInfos) {
  mBlockWorkerInfos.clear();
  mBlockWorkerInfos = *blockWorkerInfos;
  JNIEnv* env = JniHelper::GetEnv();
  jclass jList = env->FindClass("java/util/List");
  jmethodID add = env->GetMethodID(jList, "add", "(ILjava/lang/Object;)V");
  int listSize = mBlockWorkerInfos.size();
  jobject jBlockWorkerInfos = JniHelper::CreateObjectMethod(
    "java/util/ArrayList");
  for (int i = 0; i < listSize; i++) {
    BlockWorkerInfo workerInfo = mBlockWorkerInfos[i];
    jobject jBlockWorkerInfo = workerInfo.ToJBlockWorkerInfo();
    JniHelper::DeleteClassName(jBlockWorkerInfo);
    JniHelper::CacheClassName(jBlockWorkerInfo, "java/lang/Object");
    env->CallVoidMethod(jBlockWorkerInfos, add, i, jBlockWorkerInfo);
    JniHelper::DeleteObjectRef(jBlockWorkerInfo);
  }
  JniHelper::DeleteClassName(jBlockWorkerInfos);
  JniHelper::CacheClassName(jBlockWorkerInfos, "java/util/List");
  GetWorkerOptions* opt = reinterpret_cast<GetWorkerOptions*>(SetMemberValue(
      "alluxio/client/block/policy/options/GetWorkerOptions",
      "setBlockWorkerInfos", jBlockWorkerInfos));
  return opt;
}

GetWorkerOptions* GetWorkerOptions::setBlockId(int64_t blockId) {
  return reinterpret_cast<GetWorkerOptions*>(SetMemberValue(
      "alluxio/client/block/policy/options/GetWorkerOptions",
      "setBlockId", blockId));
}

GetWorkerOptions* GetWorkerOptions::setBlockSize(int64_t blockSize) {
  return reinterpret_cast<GetWorkerOptions*>(SetMemberValue(
      "alluxio/client/block/policy/options/GetWorkerOptions",
      "setBlockSize", blockSize));
}

int64_t LocalFirstAvoidEvictionPolicy::getAvailableBytes(
    BlockWorkerInfo workerInfo) {
  JNIEnv* env = JniHelper::GetEnv();
  jclass jClass = env->FindClass("alluxio/PropertyKey");
  jfieldID id = env->GetStaticFieldID(jClass,
      "USER_FILE_WRITE_AVOID_EVICTION_POLICY_RESERVED_BYTES",
      "Lalluxio/PropertyKey;");
  jobject tmp = env->GetStaticObjectField(jClass, id);
  int64_t mUserFileWriteCapacityReserved = JniHelper::CallStaticLongMethod(
      "alluxio/Configuration", "getBytes", tmp);
  int64_t mCapacityBytes = workerInfo.getCapacityBytes();
  int64_t mUsedBytes = workerInfo.getUsedBytes();
  int64_t res = mCapacityBytes - mUsedBytes - mUserFileWriteCapacityReserved;
  return res;
}

WorkerNetAddress* LocalFirstAvoidEvictionPolicy::getWorkerForNextBlock(
    std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes) {
  WorkerNetAddress* localWorkerNetAddress = NULL;
  std::vector<BlockWorkerInfo>::iterator it = workerInfoList->begin();
  for (; it != workerInfoList->end(); it++) {
    if (it->getNetAddress()->getHost().compare(mLocalHostName) == 0) {
      localWorkerNetAddress = it->getNetAddress();
      if (getAvailableBytes(*it) >= blockSizeBytes) {
        return localWorkerNetAddress;
      }
    }
  }
  std::vector<BlockWorkerInfo> mWorkerInfoList = *workerInfoList;
  std::random_shuffle(mWorkerInfoList.begin(), mWorkerInfoList.end());
  for (it = mWorkerInfoList.begin(); it != mWorkerInfoList.end(); it++) {
    if (getAvailableBytes(*it) >= blockSizeBytes) {
      return it->getNetAddress();
    }
  }
  if (localWorkerNetAddress == NULL && workerInfoList->size() > 0) {
    it = mWorkerInfoList.begin();
    return it->getNetAddress();
  }
  return localWorkerNetAddress;
}

WorkerNetAddress* LocalFirstPolicy::getWorkerForNextBlock(
    std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes) {
  WorkerNetAddress* localWorkerNetAddress = NULL;
  std::vector<BlockWorkerInfo>::iterator it = workerInfoList->begin();
  for (; it != workerInfoList->end(); it++) {
    if (it->getNetAddress()->getHost().compare(mLocalHostName) == 0) {
      localWorkerNetAddress = it->getNetAddress();
      if (it->getCapacityBytes() >= blockSizeBytes) {
        return localWorkerNetAddress;
      }
    }
  }
  std::vector<BlockWorkerInfo> mWorkerInfoList = *workerInfoList;
  std::random_shuffle(mWorkerInfoList.begin(), mWorkerInfoList.end());
  for (it = mWorkerInfoList.begin(); it != mWorkerInfoList.end(); it++) {
    if (it->getCapacityBytes() >= blockSizeBytes) {
      return it->getNetAddress();
    }
  }
  return NULL;
}

WorkerNetAddress* MostAvailableFirstPolicy::getWorkerForNextBlock(
    std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes) {
  int64_t mostAvailableBytes = -1;
  WorkerNetAddress* result = NULL;
  std::vector<BlockWorkerInfo>::iterator it = workerInfoList->begin();
  for (; it != workerInfoList->end(); it++) {
    if (it->getCapacityBytes() - it->getUsedBytes() > mostAvailableBytes) {
      mostAvailableBytes = it->getCapacityBytes() - it->getUsedBytes();
      result = it->getNetAddress();
    }
  }
  return result;
}

BlockWorkerInfo* RoundRobinPolicy::findBlockWorkerInfo(
    std::vector<BlockWorkerInfo>* workerInfoList, WorkerNetAddress* address) {
  std::vector<BlockWorkerInfo>::iterator it = workerInfoList->begin();
  for (; it != workerInfoList->end(); it++) {
      if (*(it->getNetAddress()) == *address) {
        return &(*it);
      }
    }
  return NULL;
}

WorkerNetAddress* RoundRobinPolicy::getWorkerForNextBlock(
    std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes) {
  std::vector<BlockWorkerInfo> mWorkerInfoList;
  if (!mInitialized) {
    mWorkerInfoList = *workerInfoList;
    std::random_shuffle(mWorkerInfoList.begin(), mWorkerInfoList.end());
    mIndex = 0;
    mInitialized = true;
  }

  for (int i = 0; i < mWorkerInfoList.size(); i++) {
    WorkerNetAddress* candidate = mWorkerInfoList[mIndex].getNetAddress();
    BlockWorkerInfo* workerInfo =
        findBlockWorkerInfo(workerInfoList, candidate);
    mIndex = (mIndex + 1) % mWorkerInfoList.size();
    if (workerInfo != NULL &&
        workerInfo->getCapacityBytes() >= blockSizeBytes) {
      return candidate;
    }
  }
  return NULL;
}

WorkerNetAddress* SpecificHostPolicy::getWorkerForNextBlock(
    std::vector<BlockWorkerInfo>* workerInfoList, int64_t blockSizeBytes) {
  std::vector<BlockWorkerInfo>::iterator it = workerInfoList->begin();
  for (; it != workerInfoList->end(); it++) {
    if (it->getNetAddress()->getHost().compare(mHostName) == 0) {
      return it->getNetAddress();
    }
  }
  return NULL;
}

WorkerNetAddress* DeterministicHashPolicy::getWorker(
    GetWorkerOptions* wOptions) {
  jobject jNetAddress = JniHelper::CallObjectMethod(options,
      "alluxio/client/block/policy/DeterministicHashPolicy",
      "getWorker", "alluxio/wire/WorkerNetAddress", wOptions->getOptions());
  try {
    WorkerNetAddress* address = new WorkerNetAddress(jNetAddress);
    return address;
  }
  catch(std::bad_alloc) {
    return NULL;
  }
  return NULL;
}
