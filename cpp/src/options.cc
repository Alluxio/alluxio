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

#include <options.h>

using ::jnihelper::JniHelper;
using ::alluxio::CreateDirectoryOptions;
using ::alluxio::CreateFileOptions;
using ::alluxio::DeleteOptions;
using ::alluxio::ExistsOptions;
using ::alluxio::SetAttributeOptions;
using ::alluxio::FreeOptions;
using ::alluxio::ListStatusOptions;
using ::alluxio::MountOptions;
using ::alluxio::OpenFileOptions;
using ::alluxio::GetStatusOptions;
using ::alluxio::WriteType;
using ::alluxio::ReadType;
using ::alluxio::TtlAction;
using ::alluxio::Mode;
using ::alluxio::LoadMetadataType;
using ::alluxio::BlockLocationPolicy;
using ::alluxio::FileWriteLocationPolicy;
using ::alluxio::DeterministicHashPolicy;
using ::alluxio::LocalFirstAvoidEvictionPolicy;
using ::alluxio::LocalFirstPolicy;
using ::alluxio::MostAvailableFirstPolicy;
using ::alluxio::RoundRobinPolicy;
using ::alluxio::SpecificHostPolicy;


WriteType* CreateDirectoryOptions::getWriteType() {
  jobject jWriteType = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/CreateDirectoryOptions",
      "getWriteType", "alluxio/client/WriteType");
  int value = JniHelper::CallIntMethod(
      jWriteType, "alluxio/client/WriteType", "getValue");
  JniHelper::DeleteObjectRef(jWriteType);
  return new WriteType(value);
}

Mode* CreateDirectoryOptions::getMode() {
  jobject jMode = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/CreateDirectoryOptions",
      "getMode", "alluxio/security/authorization/Mode");
  return new Mode(jMode);
}

TtlAction* CreateDirectoryOptions::getTtlAction() {
  jobject jTtlAction = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/CreateDirectoryOptions",
      "getTtlAction", "alluxio/wire/TtlAction");
  std::string str = JniHelper::CallStringMethod(jTtlAction,
      "alluxio/wire/TtlAction", "name");
  JniHelper::DeleteObjectRef(jTtlAction);
  return new TtlAction(str);
}

bool CreateDirectoryOptions::isAllowExists() {
  bool allowExists = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/CreateDirectoryOptions",
      "isAllowExists");
  return allowExists;
}

int64_t CreateDirectoryOptions::getTtl() {
  int64_t ttl = JniHelper::CallLongMethod(
      options, "alluxio/client/file/options/CreateDirectoryOptions",
      "getTtl");
  return ttl;
}

bool CreateDirectoryOptions::isRecursive() {
  bool recursive = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/CreateDirectoryOptions",
      "isRecursive");
  return recursive;
}

CreateDirectoryOptions* CreateDirectoryOptions::setMode(Mode* mode) {
  CreateDirectoryOptions* opt = reinterpret_cast<CreateDirectoryOptions*>(
    SetMemberValue("alluxio/client/file/options/CreateDirectoryOptions",
                   "setMode",
                   mode->getOptions()));
  return opt;
}

CreateDirectoryOptions* CreateDirectoryOptions::setWriteType(
    WriteType* writeType) {
  jobject jWriteType = writeType->tojWriteType();
  CreateDirectoryOptions* opt = reinterpret_cast<CreateDirectoryOptions*>(
    SetMemberValue("alluxio/client/file/options/CreateDirectoryOptions",
                   "setWriteType",
                   jWriteType));
  JniHelper::DeleteObjectRef(jWriteType);
  return opt;
}

CreateDirectoryOptions* CreateDirectoryOptions::setTtlAction(
    TtlAction* ttlAction) {
  jobject jTtlAction = ttlAction->tojTtlAction();
  CreateDirectoryOptions* opt = reinterpret_cast<CreateDirectoryOptions*>(
    SetMemberValue("alluxio/client/file/options/CreateDirectoryOptions",
                   "setTtlAction",
                   jTtlAction));
  JniHelper::DeleteObjectRef(jTtlAction);
  return opt;
}

CreateDirectoryOptions* CreateDirectoryOptions::setAllowExists
(bool allowExists) {
  return reinterpret_cast<CreateDirectoryOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateDirectoryOptions",
      "setAllowExists",
      allowExists));
}

CreateDirectoryOptions* CreateDirectoryOptions::setRecursive(bool recursive) {
  return reinterpret_cast<CreateDirectoryOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateDirectoryOptions",
      "setRecursive",
      recursive));
}

CreateDirectoryOptions* CreateDirectoryOptions::setTtl(int64_t ttl) {
  return reinterpret_cast<CreateDirectoryOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateDirectoryOptions",
      "setTtl",
      ttl));
}

int64_t CreateFileOptions::getBlockSizeBytes() {
  int64_t blockSizeBytes = JniHelper::CallLongMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "getBlockSizeBytes");
  return blockSizeBytes;
}

FileWriteLocationPolicy* CreateFileOptions::getLocationPolicy() {
  jobject jPolicy = JniHelper::CallObjectMethod(options,
      "alluxio/client/file/options/CreateFileOptions", "getLocationPolicy",
      "alluxio/client/file/policy/FileWriteLocationPolicy");
  std::string jName = JniHelper::GetObjectClassName(jPolicy);
  int index = jName.rfind("/", jName.length() - 1);
  std::string policyName = jName.substr(index + 1, jName.length() - index -1);
  FileWriteLocationPolicy* policy = NULL;
  if (policyName.compare("LocalFirstAvoidEvictionPolicy") == 0) {
    policy = new LocalFirstAvoidEvictionPolicy(jPolicy);
  }
  if (policyName.compare("LocalFirstPolicy") == 0) {
    policy = new LocalFirstPolicy(jPolicy);
  }
  if (policyName.compare("MostAvailableFirstPolicy") == 0) {
    policy = new MostAvailableFirstPolicy(jPolicy);
  }
  if (policyName.compare("RoundRobinPolicy") == 0) {
    policy = new RoundRobinPolicy(jPolicy);
  }
  if (policyName.compare("SpecificHostPolicy") == 0) {
    std::string hostname = JniHelper::CallStringMethod(jPolicy,
        "alluxio/client/file/policy/SpecificHostPolicy", "toString");
    policy = new SpecificHostPolicy(jPolicy, hostname);
  }
  return policy;
}

int64_t CreateFileOptions::getTtl() {
  int64_t ttl = JniHelper::CallLongMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "getTtl");
  return ttl;
}

TtlAction* CreateFileOptions::getTtlAction() {
  jobject jTtlAction = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "getTtlAction", "alluxio/wire/TtlAction");
  std::string str = JniHelper::CallStringMethod(jTtlAction,
      "alluxio/wire/TtlAction", "name");
  JniHelper::DeleteObjectRef(jTtlAction);
  return new TtlAction(str);
}

Mode* CreateFileOptions::getMode() {
  jobject jMode = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "getMode", "alluxio/security/authorization/Mode");
  return new Mode(jMode);
}

int CreateFileOptions::getWriteTier() {
  int writeTier = JniHelper::CallIntMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "getWriteTier");
  return writeTier;
}

WriteType* CreateFileOptions::getWriteType() {
  jobject jWriteType = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "getWriteType", "alluxio/client/WriteType");
  int value = JniHelper::CallIntMethod(
      jWriteType, "alluxio/client/WriteType", "getValue");
  JniHelper::DeleteObjectRef(jWriteType);
  return new WriteType(value);
}

std::string CreateFileOptions::getLocationPolicyClass() {
  std::string locationPolicyClass = JniHelper::CallStringMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "getLocationPolicyClass");
  return locationPolicyClass;
}

bool CreateFileOptions::isRecursive() {
  bool recursive = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/CreateFileOptions",
      "isRecursive");
  return recursive;
}

CreateFileOptions* CreateFileOptions::setBlockSizeBytes
    (int64_t blockSizeBytes) {
  return reinterpret_cast<CreateFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateFileOptions",
      "setBlockSizeBytes",
      blockSizeBytes));
}

CreateFileOptions* CreateFileOptions::setLocationPolicy(
    FileWriteLocationPolicy* locationPolicy) {
  LocalFirstPolicy* policy =
      reinterpret_cast<LocalFirstPolicy*>(locationPolicy);
  JniHelper::CacheClassName(policy->getOptions(),
      "alluxio/client/file/policy/FileWriteLocationPolicy");
  return reinterpret_cast<CreateFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateFileOptions",
      "setLocationPolicy",
      policy->getOptions()));
}

CreateFileOptions* CreateFileOptions::setLocationPolicyClass
    (std::string className) {
  return reinterpret_cast<CreateFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateFileOptions",
      "setLocationPolicyClass",
      className));
}

CreateFileOptions* CreateFileOptions::setRecursive(bool recursive) {
  return reinterpret_cast<CreateFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateFileOptions",
      "setRecursive",
      recursive));
}

CreateFileOptions* CreateFileOptions::setMode(Mode* mode) {
  CreateFileOptions* opt = reinterpret_cast<CreateFileOptions*>(
    SetMemberValue("alluxio/client/file/options/CreateFileOptions",
                   "setMode",
                   mode->getOptions()));
  return opt;
}

CreateFileOptions* CreateFileOptions::setTtl(int64_t ttl) {
  return reinterpret_cast<CreateFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateFileOptions",
      "setTtl",
      ttl));
}

CreateFileOptions* CreateFileOptions::setTtlAction(TtlAction* ttlAction) {
  jobject jTtlAction = ttlAction->tojTtlAction();
  CreateFileOptions* opt = reinterpret_cast<CreateFileOptions*>(
    SetMemberValue("alluxio/client/file/options/CreateFileOptions",
                   "setTtlAction",
                   jTtlAction));
  JniHelper::DeleteObjectRef(jTtlAction);
  return opt;
}

CreateFileOptions* CreateFileOptions::setWriteTier(int writeTier) {
  return reinterpret_cast<CreateFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/CreateFileOptions",
      "setWriteTier",
      writeTier));
}

CreateFileOptions* CreateFileOptions::setWriteType(WriteType* writeType) {
  jobject jWriteType = writeType->tojWriteType();
  CreateFileOptions* opt = reinterpret_cast<CreateFileOptions*>(
    SetMemberValue("alluxio/client/file/options/CreateFileOptions",
                   "setWriteType",
                   jWriteType));
  JniHelper::DeleteObjectRef(jWriteType);
  return opt;
}

bool DeleteOptions::isRecursive() {
  bool recursive = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/DeleteOptions",
      "isRecursive");
  return recursive;
}

bool DeleteOptions::isAlluxioOnly() {
  bool alluxioOnly = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/DeleteOptions",
      "isAlluxioOnly");
  return alluxioOnly;
}

bool DeleteOptions::isUnchecked() {
  bool unchecked = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/DeleteOptions",
      "isUnchecked");
  return unchecked;
}

DeleteOptions* DeleteOptions::setRecursive(bool recursive) {
  return reinterpret_cast<DeleteOptions*>(SetMemberValue(
      "alluxio/client/file/options/DeleteOptions",
      "setRecursive",
      recursive));
}

DeleteOptions* DeleteOptions::setUnchecked(bool unchecked) {
  return reinterpret_cast<DeleteOptions*>(SetMemberValue(
      "alluxio/client/file/options/DeleteOptions",
      "setUnchecked",
      unchecked));
}

DeleteOptions* DeleteOptions::setAlluxioOnly(bool alluxioOnly) {
  return reinterpret_cast<DeleteOptions*>(SetMemberValue(
      "alluxio/client/file/options/DeleteOptions",
      "setAlluxioOnly",
      alluxioOnly));
}

LoadMetadataType* ExistsOptions::getLoadMetadataType() {
  jobject jLoadMetadataType = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/ExistsOptions",
      "getLoadMetadataType", "alluxio/wire/LoadMetadataType");
  int value = JniHelper::CallIntMethod(
      jLoadMetadataType, "alluxio/wire/LoadMetadataType", "getValue");
  JniHelper::DeleteObjectRef(jLoadMetadataType);
  return new LoadMetadataType(value);
}

ExistsOptions* ExistsOptions::setLoadMetadataType(
    LoadMetadataType* loadMetadataType) {
  jobject jLoadMetadataType = loadMetadataType->tojLoadMetadataType();
  ExistsOptions* opt = reinterpret_cast<ExistsOptions*>(
    SetMemberValue("alluxio/client/file/options/ExistsOptions",
                   "setLoadMetadataType",
                   jLoadMetadataType));
  JniHelper::DeleteObjectRef(jLoadMetadataType);
  return opt;
}

bool FreeOptions::isForced() {
  bool forced = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/FreeOptions",
      "isForced");
  return forced;
}

bool FreeOptions::isRecursive() {
  bool recursive = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/FreeOptions",
      "isRecursive");
  return recursive;
}

FreeOptions* FreeOptions::setForced(bool forced) {
  return reinterpret_cast<FreeOptions*>(SetMemberValue(
      "alluxio/client/file/options/FreeOptions",
      "setForced",
      forced));
}

FreeOptions* FreeOptions::setRecursive(bool recursive) {
  return reinterpret_cast<FreeOptions*>(SetMemberValue(
      "alluxio/client/file/options/FreeOptions",
      "setRecursive",
      recursive));
}

LoadMetadataType* ListStatusOptions::getLoadMetadataType() {
  jobject jLoadMetadataType = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/ListStatusOptions",
      "getLoadMetadataType", "alluxio/wire/LoadMetadataType");
  int value = JniHelper::CallIntMethod(
      jLoadMetadataType, "alluxio/wire/LoadMetadataType", "getValue");
  JniHelper::DeleteObjectRef(jLoadMetadataType);
  return new LoadMetadataType(value);
}

ListStatusOptions* ListStatusOptions::setLoadMetadataType(
    LoadMetadataType* loadMetadataType) {
  jobject jLoadMetadataType = loadMetadataType->tojLoadMetadataType();
  ListStatusOptions* opt = reinterpret_cast<ListStatusOptions*>(
    SetMemberValue("alluxio/client/file/options/ListStatusOptions",
                   "setLoadMetadataType",
                   jLoadMetadataType));
  JniHelper::DeleteObjectRef(jLoadMetadataType);
  return opt;
}

bool MountOptions::isReadOnly() {
  bool readOnly = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/MountOptions",
      "isReadOnly");
  return readOnly;
}

std::map<std::string, std::string> MountOptions::getProperties() {
  jobject jProperties = JniHelper::CallObjectMethod(options,
      "alluxio/client/file/options/MountOptions",
      "getProperties", "java/util/Map");
  int mapSize = JniHelper::CallIntMethod(jProperties, "java/util/Map", "size");
  jobject keySet = JniHelper::CallObjectMethod(jProperties, "java/util/Map",
      "keySet", "java/util/Set");
  jobject keyArray = JniHelper::CallObjectMethod(keySet, "java/util/Set",
      "toArray", "[Ljava/lang/Object");
  std::map<std::string, std::string> result;
  for (int i = 0; i < mapSize; i ++) {
    jobject keyItem = JniHelper::GetEnv()->
        GetObjectArrayElement((jobjectArray)keyArray, i);
    std::string key = JniHelper::JstringToString((jstring)keyItem);
    JniHelper::CacheClassName(keyItem, "java/lang/Object");
    jobject valueItem = JniHelper::CallObjectMethod(jProperties,
        "java/util/Map", "get", "java/lang/Object", (jobject)keyItem);
    std::string value = JniHelper::JstringToString((jstring)valueItem);
    result.insert(std::make_pair(key, value));
    JniHelper::DeleteObjectRef(keyItem);
    JniHelper::DeleteObjectRef(valueItem);
  }
  JniHelper::DeleteObjectRef(keyArray);
  JniHelper::DeleteObjectRef(keySet);
  JniHelper::DeleteObjectRef(jProperties);
  return result;
}

bool MountOptions::isShared() {
  bool shared = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/MountOptions",
      "isShared");
  return shared;
}

MountOptions* MountOptions::setReadOnly(bool readOnly) {
  return reinterpret_cast<MountOptions*>(SetMemberValue(
      "alluxio/client/file/options/MountOptions",
      "setReadOnly",
      readOnly));
}


MountOptions* MountOptions::setProperties(
    std::map<std::string, std::string> properties) {
  JNIEnv* env = JniHelper::GetEnv();
  jobject jMap = JniHelper::CreateObjectMethod("java/util/HashMap");
  std::map<std::string, std::string>::iterator it;
  for (it = properties.begin(); it != properties.end(); it++) {
    jstring jkey = JniHelper::SringToJstring(env, it->first.c_str());
    jstring jvalue = JniHelper::SringToJstring(env, it->second.c_str());
    JniHelper::CacheClassName(jkey, "java/lang/Object");
    JniHelper::CacheClassName(jvalue, "java/lang/Object");
    JniHelper::CallObjectMethod(jMap, "java/util/Map", "put",
        "java/lang/Object", (jobject)jkey, (jobject)jvalue);
  }
  JniHelper::CacheClassName(jMap, "java/util/Map");
  return reinterpret_cast<MountOptions*>(SetMemberValue(
      "alluxio/client/file/options/MountOptions",
      "setProperties",
      jMap));
}

MountOptions* MountOptions::setShared(bool shared) {
  return reinterpret_cast<MountOptions*>(SetMemberValue(
      "alluxio/client/file/options/MountOptions",
      "setShared",
      shared));
}

FileWriteLocationPolicy* OpenFileOptions::getLocationPolicy() {
  jobject jPolicy = JniHelper::CallObjectMethod(options,
      "alluxio/client/file/options/OpenFileOptions", "getLocationPolicy",
      "alluxio/client/file/policy/FileWriteLocationPolicy");
  std::string jName = JniHelper::GetObjectClassName(jPolicy);
  int index = jName.rfind("/", jName.length() - 1);
  std::string policyName = jName.substr(index + 1, jName.length() - index -1);
  FileWriteLocationPolicy* policy = NULL;
  if (policyName.compare("LocalFirstAvoidEvictionPolicy") == 0) {
    policy = new LocalFirstAvoidEvictionPolicy(jPolicy);
  }
  if (policyName.compare("LocalFirstPolicy") == 0) {
    policy = new LocalFirstPolicy(jPolicy);
  }
  if (policyName.compare("MostAvailableFirstPolicy") == 0) {
    policy = new MostAvailableFirstPolicy(jPolicy);
  }
  if (policyName.compare("RoundRobinPolicy") == 0) {
    policy = new RoundRobinPolicy(jPolicy);
  }
  if (policyName.compare("SpecificHostPolicy") == 0) {
    std::string hostname = JniHelper::CallStringMethod(jPolicy,
        "alluxio/client/file/policy/SpecificHostPolicy", "toString");
    policy = new SpecificHostPolicy(jPolicy, hostname);
  }
  return policy;
}

FileWriteLocationPolicy* OpenFileOptions::getCacheLocationPolicy() {
  jobject jPolicy = JniHelper::CallObjectMethod(options,
      "alluxio/client/file/options/OpenFileOptions", "getCacheLocationPolicy",
      "alluxio/client/file/policy/FileWriteLocationPolicy");
  std::string jName = JniHelper::GetObjectClassName(jPolicy);
  int index = jName.rfind("/", jName.length() - 1);
  std::string policyName = jName.substr(index + 1, jName.length() - index -1);
  FileWriteLocationPolicy* policy = NULL;
  if (policyName.compare("LocalFirstAvoidEvictionPolicy") == 0) {
    policy = new LocalFirstAvoidEvictionPolicy(jPolicy);
  }
  if (policyName.compare("LocalFirstPolicy") == 0) {
    policy = new LocalFirstPolicy(jPolicy);
  }
  if (policyName.compare("MostAvailableFirstPolicy") == 0) {
    policy = new MostAvailableFirstPolicy(jPolicy);
  }
  if (policyName.compare("RoundRobinPolicy") == 0) {
    policy = new RoundRobinPolicy(jPolicy);
  }
  if (policyName.compare("SpecificHostPolicy") == 0) {
    std::string hostname = JniHelper::CallStringMethod(jPolicy,
        "alluxio/client/file/policy/SpecificHostPolicy", "toString");
    policy = new SpecificHostPolicy(jPolicy, hostname);
  }
  return policy;
}

std::string OpenFileOptions::getLocationPolicyClass() {
  std::string locationPolicyClass = JniHelper::CallStringMethod(
      options, "alluxio/client/file/options/OpenFileOptions",
      "getLocationPolicyClass");
  return locationPolicyClass;
}

std::string OpenFileOptions::getCacheLocationPolicyClass() {
  std::string cacheLocationPolicyClass = JniHelper::CallStringMethod(
      options, "alluxio/client/file/options/OpenFileOptions",
      "getCacheLocationPolicyClass");
  return cacheLocationPolicyClass;
}

BlockLocationPolicy* OpenFileOptions::getUfsReadLocationPolicy() {
  jobject jPolicy = JniHelper::CallObjectMethod(options,
      "alluxio/client/file/options/OpenFileOptions", "getUfsReadLocationPolicy",
      "alluxio/client/block/policy/BlockLocationPolicy");
  std::string jName = JniHelper::GetObjectClassName(jPolicy);
  int index = jName.rfind("/", jName.length() - 1);
  std::string policyName = jName.substr(index + 1, jName.length() - index -1);
  BlockLocationPolicy* policy = NULL;
  if (policyName.compare("LocalFirstAvoidEvictionPolicy") == 0) {
    policy = new LocalFirstAvoidEvictionPolicy(jPolicy);
  }
  if (policyName.compare("LocalFirstPolicy") == 0) {
    policy = new LocalFirstPolicy(jPolicy);
  }
  if (policyName.compare("MostAvailableFirstPolicy") == 0) {
    policy = new MostAvailableFirstPolicy(jPolicy);
  }
  if (policyName.compare("RoundRobinPolicy") == 0) {
    policy = new RoundRobinPolicy(jPolicy);
  }
  if (policyName.compare("SpecificHostPolicy") == 0) {
    std::string hostname = JniHelper::CallStringMethod(jPolicy,
        "alluxio/client/file/policy/SpecificHostPolicy", "toString");
    policy = new SpecificHostPolicy(jPolicy, hostname);
  }
  if (policyName.compare("DeterministicHashPolicy") == 0) {
    policy = new DeterministicHashPolicy(jPolicy);
  }
  return policy;
}

std::string OpenFileOptions::getUfsReadLocationPolicyClass() {
  std::string ufsReadLocationPolicyClass = JniHelper::CallStringMethod(
      options, "alluxio/client/file/options/OpenFileOptions",
      "getUfsReadLocationPolicyClass");
  return ufsReadLocationPolicyClass;
}

ReadType* OpenFileOptions::getReadType() {
  jobject jReadType = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/OpenFileOptions",
      "getReadType", "alluxio/client/ReadType");
  int value = JniHelper::CallIntMethod(
      jReadType, "alluxio/client/ReadType", "getValue");
  JniHelper::DeleteObjectRef(jReadType);
  return new ReadType(value);
}

int OpenFileOptions::getMaxUfsReadConcurrency() {
  int maxUfsReadConcurrency = JniHelper::CallIntMethod(
      options, "alluxio/client/file/options/OpenFileOptions",
      "getMaxUfsReadConcurrency");
  return maxUfsReadConcurrency;
}

OpenFileOptions* OpenFileOptions::setLocationPolicy(
    FileWriteLocationPolicy* locationPolicy) {
  LocalFirstPolicy* policy =
     reinterpret_cast<LocalFirstPolicy*>(locationPolicy);
  JniHelper::CacheClassName(policy->getOptions(),
      "alluxio/client/file/policy/FileWriteLocationPolicy");
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setLocationPolicy",
      policy->getOptions()));
}

OpenFileOptions* OpenFileOptions::setCacheLocationPolicy(
    FileWriteLocationPolicy* locationPolicy) {
  LocalFirstPolicy* policy =
      reinterpret_cast<LocalFirstPolicy*>(locationPolicy);
  JniHelper::CacheClassName(policy->getOptions(),
      "alluxio/client/file/policy/FileWriteLocationPolicy");
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setCacheLocationPolicy",
      policy->getOptions()));
}

OpenFileOptions* OpenFileOptions::setLocationPolicyClass(
    std::string className) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setLocationPolicyClass",
      className));
}

OpenFileOptions* OpenFileOptions::setCacheLocationPolicyClass(
    std::string className) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setCacheLocationPolicyClass",
      className));
}

OpenFileOptions* OpenFileOptions::setUfsReadLocationPolicy(
    BlockLocationPolicy* policy) {
  DeterministicHashPolicy* newPolicy =
      reinterpret_cast<DeterministicHashPolicy*>(policy);
  JniHelper::CacheClassName(newPolicy->getOptions(),
      "alluxio/client/block/policy/BlockLocationPolicy");
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setUfsReadLocationPolicy",
      newPolicy->getOptions()));
}

OpenFileOptions* OpenFileOptions::setUfsReadLocationPolicyClass(
    std::string className) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setUfsReadLocationPolicyClass",
      className));
}

OpenFileOptions* OpenFileOptions::setReadType(ReadType* readType) {
  jobject jReadType = readType->tojReadType();
  OpenFileOptions* opt = reinterpret_cast<OpenFileOptions*>(
    SetMemberValue("alluxio/client/file/options/OpenFileOptions",
                   "setReadType",
                   jReadType));
  JniHelper::DeleteObjectRef(jReadType);
  return opt;
}

OpenFileOptions* OpenFileOptions::setMaxUfsReadConcurrency(
    int maxUfsReadConcurrency) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setMaxUfsReadConcurrency",
      maxUfsReadConcurrency));
}

bool SetAttributeOptions::getPinned() {
  jobject tmp = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getPinned", "java/lang/Boolean");
  bool pinned = JniHelper::CallBooleanMethod(
      tmp, "java/lang/Boolean", "booleanValue");
  return pinned;
}

int64_t SetAttributeOptions::getTtl() {
  jobject tmp = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getTtl", "java/lang/Long");
  int64_t ttl = JniHelper::CallLongMethod(
      tmp, "java/lang/Long", "longValue");
  return ttl;
}

TtlAction* SetAttributeOptions::getTtlAction() {
  jobject jTtlAction = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getTtlAction", "alluxio/wire/TtlAction");
  std::string str = JniHelper::CallStringMethod(jTtlAction,
      "alluxio/wire/TtlAction", "name");
  JniHelper::DeleteObjectRef(jTtlAction);
  return new TtlAction(str);
}

bool SetAttributeOptions::getPersisted() {
  jobject tmp = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getPersisted", "java/lang/Boolean");
  bool persisted = JniHelper::CallBooleanMethod(
      tmp, "java/lang/Boolean", "booleanValue");
  return persisted;
}

Mode* SetAttributeOptions::getMode() {
  jobject jMode = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getMode", "alluxio/security/authorization/Mode");
  return new Mode(jMode);
}

std::string SetAttributeOptions::getOwner() {
  std::string owner = JniHelper::CallStringMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getOwner");
  return owner;
}

std::string SetAttributeOptions::getGroup() {
  std::string group = JniHelper::CallStringMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getGroup");
  return group;
}

bool SetAttributeOptions::isRecursive() {
  bool recursive = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "isRecursive");
  return recursive;
}

SetAttributeOptions* SetAttributeOptions::setPinned(bool pinned) {
  return reinterpret_cast<SetAttributeOptions*>(SetMemberValue(
      "alluxio/client/file/options/SetAttributeOptions",
      "setPinned",
      pinned));
}

SetAttributeOptions* SetAttributeOptions::setTtl(int64_t ttl) {
  return reinterpret_cast<SetAttributeOptions*>(SetMemberValue(
      "alluxio/client/file/options/SetAttributeOptions",
      "setTtl",
      ttl));
}

SetAttributeOptions* SetAttributeOptions::setTtlAction(TtlAction* ttlAction) {
  jobject jTtlAction = ttlAction->tojTtlAction();
  SetAttributeOptions* opt = reinterpret_cast<SetAttributeOptions*>(
    SetMemberValue("alluxio/client/file/options/SetAttributeOptions",
                   "setTtlAction",
                   jTtlAction));
  JniHelper::DeleteObjectRef(jTtlAction);
  return opt;
}

SetAttributeOptions* SetAttributeOptions::setPersisted(bool persisted) {
  return reinterpret_cast<SetAttributeOptions*>(SetMemberValue(
      "alluxio/client/file/options/SetAttributeOptions",
      "setPersisted",
      persisted));
}

SetAttributeOptions* SetAttributeOptions::setMode(Mode* mode) {
  SetAttributeOptions* opt = reinterpret_cast<SetAttributeOptions*>(
    SetMemberValue("alluxio/client/file/options/SetAttributeOptions",
                   "setMode",
                   mode->getOptions()));
  return opt;
}

Status SetAttributeOptions::setOwner(std::string owner) {
  try {
    SetMemberValue("alluxio/client/file/options/SetAttributeOptions",
                   "setOwner",
                   owner);
    Status status = JniHelper::AlluxioExceptionCheck();
    return status;
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status SetAttributeOptions::setGroup(std::string group) {
  try {
    SetMemberValue("alluxio/client/file/options/SetAttributeOptions",
                   "setGroup",
                   group);
    Status status = JniHelper::AlluxioExceptionCheck();
    return status;
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

SetAttributeOptions* SetAttributeOptions::setRecursive(bool recursive) {
  return reinterpret_cast<SetAttributeOptions*>(SetMemberValue(
      "alluxio/client/file/options/SetAttributeOptions",
      "setRecursive",
      recursive));
}

LoadMetadataType* GetStatusOptions::getLoadMetadataType() {
  jobject jLoadMetadataType = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/GetStatusOptions",
      "getLoadMetadataType", "alluxio/wire/LoadMetadataType");
  int value = JniHelper::CallIntMethod(
      jLoadMetadataType, "alluxio/wire/LoadMetadataType", "getValue");
  JniHelper::DeleteObjectRef(jLoadMetadataType);
  return new LoadMetadataType(value);
}

GetStatusOptions* GetStatusOptions::setLoadMetadataType(
    LoadMetadataType* loadMetadataType) {
  jobject jLoadMetadataType = loadMetadataType->tojLoadMetadataType();
  GetStatusOptions* opt = reinterpret_cast<GetStatusOptions*>(
    SetMemberValue("alluxio/client/file/options/GetStatusOptions",
                   "setLoadMetadataType",
                   jLoadMetadataType));
  JniHelper::DeleteObjectRef(jLoadMetadataType);
  return opt;
}
