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
// #include <stdio.h>

using ::jnihelper::JniHelper;
using ::alluxio::CreateDirectoryOptions;
using ::alluxio::CreateFileOptions;
using ::alluxio::DeleteOptions;
using ::alluxio::SetAttributeOptions;
using ::alluxio::FreeOptions;
using ::alluxio::MountOptions;
using ::alluxio::OpenFileOptions;
using ::alluxio::WriteType;
using ::alluxio::TtlAction;
using ::alluxio::Mode;

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
  JNIEnv* env = JniHelper::GetEnv();
  jclass clTtlAction = env->GetObjectClass(jTtlAction);
  jmethodID getVal = env->GetMethodID(clTtlAction, "name",
                                      "()Ljava/lang/String;");
  jstring value = (jstring)env->CallObjectMethod(jTtlAction, getVal);
  std::string str = JniHelper::JstringToString(value);
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
                   mode->getOpt()));
  return opt;
}

CreateDirectoryOptions* CreateDirectoryOptions::setWriteType
(WriteType* writeType) {
  jobject jWriteType = writeType->tojWriteType();
  CreateDirectoryOptions* opt = reinterpret_cast<CreateDirectoryOptions*>(
    SetMemberValue("alluxio/client/file/options/CreateDirectoryOptions",
                   "setWriteType",
                   jWriteType));
  JniHelper::DeleteObjectRef(jWriteType);
  return opt;
}

CreateDirectoryOptions* CreateDirectoryOptions::setTtlAction
(TtlAction* ttlAction) {
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
  JNIEnv* env = JniHelper::GetEnv();
  jclass clTtlAction = env->GetObjectClass(jTtlAction);
  jmethodID getVal = env->GetMethodID(clTtlAction, "name",
                                      "()Ljava/lang/String;");
  jstring value = (jstring)env->CallObjectMethod(jTtlAction, getVal);
  std::string str = JniHelper::JstringToString(value);
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
                   mode->getOpt()));
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

bool MountOptions::isReadOnly() {
  bool readOnly = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/MountOptions",
      "isReadOnly");
  return readOnly;
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

MountOptions* MountOptions::setShared(bool shared) {
  return reinterpret_cast<MountOptions*>(SetMemberValue(
      "alluxio/client/file/options/MountOptions",
      "setShared",
      shared));
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

std::string OpenFileOptions::getUfsReadLocationPolicyClass() {
  std::string ufsReadLocationPolicyClass = JniHelper::CallStringMethod(
      options, "alluxio/client/file/options/OpenFileOptions",
      "getUfsReadLocationPolicyClass");
  return ufsReadLocationPolicyClass;
}

int OpenFileOptions::getMaxUfsReadConcurrency() {
  int maxUfsReadConcurrency = JniHelper::CallIntMethod(
      options, "alluxio/client/file/options/OpenFileOptions",
      "getMaxUfsReadConcurrency");
  return maxUfsReadConcurrency;
}

OpenFileOptions* OpenFileOptions::setLocationPolicyClass
(std::string className) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setLocationPolicyClass",
      className));
}

OpenFileOptions* OpenFileOptions::setCacheLocationPolicyClass
(std::string className) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setCacheLocationPolicyClass",
      className));
}

OpenFileOptions* OpenFileOptions::setUfsReadLocationPolicyClass
(std::string className) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setUfsReadLocationPolicyClass",
      className));
}

OpenFileOptions* OpenFileOptions::setMaxUfsReadConcurrency
(int maxUfsReadConcurrency) {
  return reinterpret_cast<OpenFileOptions*>(SetMemberValue(
      "alluxio/client/file/options/OpenFileOptions",
      "setMaxUfsReadConcurrency",
      maxUfsReadConcurrency));
}

bool SetAttributeOptions::getPinned() {
  bool pinned = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getPinned");
  return pinned;
}

int64_t SetAttributeOptions::getTtl() {
  int64_t ttl = JniHelper::CallLongMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getTtl");
  return ttl;
}

TtlAction* SetAttributeOptions::getTtlAction() {
  jobject jTtlAction = JniHelper::CallObjectMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getTtlAction", "alluxio/wire/TtlAction");
  JNIEnv* env = JniHelper::GetEnv();
  jclass clTtlAction = env->GetObjectClass(jTtlAction);
  jmethodID getVal = env->GetMethodID(clTtlAction, "name",
                                      "()Ljava/lang/String;");
  jstring value = (jstring)env->CallObjectMethod(jTtlAction, getVal);
  std::string str = JniHelper::JstringToString(value);
  JniHelper::DeleteObjectRef(jTtlAction);
  return new TtlAction(str);
}

bool SetAttributeOptions::getPersisted() {
  bool persisted = JniHelper::CallBooleanMethod(
      options, "alluxio/client/file/options/SetAttributeOptions",
      "getPersisted");
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
      "recursive");
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
                   mode->getOpt()));
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
                   "setOwner",
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
