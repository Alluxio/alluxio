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

#include <string.h>
#include <assert.h>

#include "options.h"
#include "fileSystem.h"
#include "localAlluxioCluster.h"

namespace alluxio {

void CreateDirectoryOptionsTest() {
  CreateDirectoryOptions* createDirectoryOptions;
  createDirectoryOptions = CreateDirectoryOptions::getDefaultOptions();
  createDirectoryOptions->setAllowExists(false);
  bool allowExists = createDirectoryOptions->isAllowExists();
  assert(allowExists == false);
  createDirectoryOptions->setTtl(10);
  int64_t ttl = createDirectoryOptions->getTtl();
  assert(ttl == 10);
  createDirectoryOptions->setRecursive(false);
  bool recursive = createDirectoryOptions->isRecursive();
  assert(recursive == false);
  WriteType* writeType = new WriteType(5);
  createDirectoryOptions->setWriteType(writeType);
  delete writeType;
  writeType = createDirectoryOptions->getWriteType();
  assert(writeType->getValue() == 5);
  TtlAction* ttlAction = new TtlAction("FREE");
  createDirectoryOptions->setTtlAction(ttlAction);
  delete ttlAction;
  ttlAction = createDirectoryOptions->getTtlAction();
  assert(ttlAction->isFree() == true);
  Mode* mode = Mode::getDefaultMode();
  Bits* bits = new Bits("---");
  mode->setOwnerBits(bits);
  createDirectoryOptions->setMode(mode);
  Mode* newMode = createDirectoryOptions->getMode();
  assert(newMode->getOwnerBits()->toString().compare("---") == 0);
  delete bits;
  delete newMode;
  delete mode;
  delete ttlAction;
  delete writeType;
  delete createDirectoryOptions;
}

void CreateFileOptionsTest() {
  CreateFileOptions* createFileOptions = CreateFileOptions::getDefaultOptions();
  createFileOptions->setBlockSizeBytes(100);
  int64_t blockSizeBytes = createFileOptions->getBlockSizeBytes();
  createFileOptions->setTtl(50);
  int64_t ttl = createFileOptions->getTtl();
  createFileOptions->setWriteTier(1);
  int writeTier = createFileOptions->getWriteTier();
  createFileOptions->setRecursive(true);
  bool recursive = createFileOptions->isRecursive();
  assert(blockSizeBytes == 100);
  assert(writeTier == 1);
  assert(ttl == 50);
  assert(recursive == true);
  WriteType* writeType = new WriteType(3);
  createFileOptions->setWriteType(writeType);
  delete writeType;
  writeType = createFileOptions->getWriteType();
  assert(writeType->getValue() == 3);
  TtlAction* ttlAction = new TtlAction("FREE");
  createFileOptions->setTtlAction(ttlAction);
  delete ttlAction;
  ttlAction = createFileOptions->getTtlAction();
  assert(ttlAction->isFree() == true);
  Mode* mode = Mode::getDefaultMode();
  Bits* bits = new Bits("rwx");
  mode->setOtherBits(bits);
  createFileOptions->setMode(mode);
  Mode* newMode = createFileOptions->getMode();
  assert(newMode->getOtherBits()->toString().compare("rwx") == 0);
  FileWriteLocationPolicy* policy = NULL;
  policy = RoundRobinPolicy::getPolicy();
  createFileOptions->setLocationPolicy(policy);
  FileWriteLocationPolicy* newPolicy = createFileOptions->getLocationPolicy();
  std::string policyClass = createFileOptions->getLocationPolicyClass();
  assert(policyClass.compare(
      "alluxio.client.file.policy.RoundRobinPolicy") == 0);
  delete newPolicy;
  delete policy;
  delete bits;
  delete newMode;
  delete mode;
  delete ttlAction;
  delete writeType;
  delete createFileOptions;
}

void DeleteOptionsTest() {
  DeleteOptions* deleteOptions = DeleteOptions::getDefaultOptions();
  deleteOptions->setRecursive(true);
  bool recursive = deleteOptions->isRecursive();
  assert(recursive == true);
  deleteOptions->setAlluxioOnly(true);
  bool alluxioOnly = deleteOptions->isAlluxioOnly();
  assert(alluxioOnly == true);
  deleteOptions->setUnchecked(true);
  bool unchecked = deleteOptions->isUnchecked();
  assert(unchecked == true);
  delete deleteOptions;
}

void ExistsOptionsTest() {
  ExistsOptions* existsOptions = ExistsOptions::getDefaultOptions();
  LoadMetadataType* loadMetadataType = new LoadMetadataType(1);
  existsOptions->setLoadMetadataType(loadMetadataType);
  delete loadMetadataType;
  loadMetadataType = existsOptions->getLoadMetadataType();
  assert(loadMetadataType->getValue() == 1);
  delete loadMetadataType;
  delete existsOptions;
}

void FreeOptionsTest() {
  FreeOptions* freeOptions = FreeOptions::getDefaultOptions();
  freeOptions->setForced(true);
  bool forced = freeOptions->isForced();
  assert(forced == true);
  freeOptions->setRecursive(true);
  bool recursive = freeOptions->isRecursive();
  assert(recursive == true);
  delete freeOptions;
}

void ListStatusOptionsTest() {
  ListStatusOptions* listStatusOptions = ListStatusOptions::getDefaultOptions();
  LoadMetadataType* loadMetadataType = new LoadMetadataType(2);
  listStatusOptions->setLoadMetadataType(loadMetadataType);
  delete loadMetadataType;
  loadMetadataType = listStatusOptions->getLoadMetadataType();
  assert(loadMetadataType->getValue() == 2);
  delete loadMetadataType;
  delete listStatusOptions;
}

void MountOptionsTest() {
  MountOptions* mountOptions = MountOptions::getDefaultOptions();
  mountOptions->setReadOnly(true);
  bool readOnly = mountOptions->isReadOnly();
  mountOptions->setShared(false);
  bool shared = mountOptions->isShared();
  std::map<std::string, std::string> properties;
  properties.insert(std::make_pair("what", "none"));
  mountOptions->setProperties(properties);
  std::map<std::string, std::string> nProperties;
  nProperties = mountOptions->getProperties();
  std::map<std::string, std::string>::iterator it;
  it = nProperties.begin();
  assert(it->first.compare("what") == 0);
  assert(it->second.compare("none") == 0);
  delete mountOptions;
}

void OpenFileOptionsTest() {
  OpenFileOptions* openFileOptions = OpenFileOptions::getDefaultOptions();
  FileWriteLocationPolicy* policy = NULL;
  policy = LocalFirstPolicy::getPolicy();
  openFileOptions->setLocationPolicy(policy);
  FileWriteLocationPolicy* newPolicy = openFileOptions->getLocationPolicy();
  std::string policyClass = openFileOptions->getLocationPolicyClass();
  assert(policyClass.compare(
      "alluxio.client.file.policy.LocalFirstPolicy") == 0);
  openFileOptions->setLocationPolicyClass(
      "alluxio.client.file.policy.RoundRobinPolicy");
  policyClass = openFileOptions->getLocationPolicyClass();
  assert(policyClass.compare(
      "alluxio.client.file.policy.RoundRobinPolicy") == 0);
  delete policy;
  policy = MostAvailableFirstPolicy::getPolicy();
  openFileOptions->setCacheLocationPolicy(policy);
  newPolicy = openFileOptions->getCacheLocationPolicy();
  policyClass = openFileOptions->getCacheLocationPolicyClass();
  assert(policyClass.compare(
      "alluxio.client.file.policy.MostAvailableFirstPolicy") == 0);
  openFileOptions->setCacheLocationPolicyClass(
      "alluxio.client.file.policy.RoundRobinPolicy");
  policyClass = openFileOptions->getCacheLocationPolicyClass();
  assert(policyClass.compare(
      "alluxio.client.file.policy.RoundRobinPolicy") == 0);
  ReadType* readType = new ReadType(2);
  openFileOptions->setReadType(readType);
  delete readType;
  readType = openFileOptions->getReadType();
  assert(readType->getValue() == 2);
  openFileOptions->setMaxUfsReadConcurrency(3);
  int maxUfsReadConcurrency = openFileOptions->getMaxUfsReadConcurrency();
  assert(maxUfsReadConcurrency == 3);
  BlockLocationPolicy* bPolicy = NULL;
  bPolicy = DeterministicHashPolicy::getPolicy();
  openFileOptions->setUfsReadLocationPolicy(bPolicy);
  BlockLocationPolicy* nPolicy = openFileOptions->getUfsReadLocationPolicy();
  policyClass = openFileOptions->getUfsReadLocationPolicyClass();
  assert(policyClass.compare(
      "alluxio.client.block.policy.DeterministicHashPolicy") == 0);
  openFileOptions->setUfsReadLocationPolicyClass(
      "alluxio.client.file.policy.RoundRobinPolicy");
  policyClass = openFileOptions->getUfsReadLocationPolicyClass();
  assert(policyClass.compare(
      "alluxio.client.file.policy.RoundRobinPolicy") == 0);
  delete nPolicy;
  delete bPolicy;
  delete readType;
  delete newPolicy;
  delete policy;
  delete openFileOptions;
}

void SetAttributeOptionsTest() {
  SetAttributeOptions* setAttributeOptions =
      SetAttributeOptions::getDefaultOptions();
  setAttributeOptions->setPinned(true);
  bool pinned = setAttributeOptions->getPinned();
  assert(pinned == true);
  setAttributeOptions->setTtl(10);
  int64_t ttl = setAttributeOptions->getTtl();
  assert(ttl == 10);
  TtlAction* ttlAction = new TtlAction("FREE");
  setAttributeOptions->setTtlAction(ttlAction);
  delete ttlAction;
  ttlAction = setAttributeOptions->getTtlAction();
  assert(ttlAction->isFree() == true);
  setAttributeOptions->setPersisted(true);
  bool persisted = setAttributeOptions->getPersisted();
  assert(persisted == true);
  Mode* mode = Mode::getDefaultMode();
  Bits* bits = new Bits("rwx");
  mode->setOwnerBits(bits);
  setAttributeOptions->setMode(mode);
  Mode* newMode = setAttributeOptions->getMode();
  assert(newMode->getOwnerBits()->toString().compare("rwx") == 0);
  setAttributeOptions->setRecursive(true);
  bool recursive = setAttributeOptions->isRecursive();
  assert(recursive == true);
  Status status1 = setAttributeOptions->setOwner("user1");
  Status status2 = setAttributeOptions->setGroup("group1");
  assert(status1.ok());
  assert(status2.ok());
  std::string owner = setAttributeOptions->getOwner();
  std::string group = setAttributeOptions->getGroup();
  assert(owner.compare("user1") == 0);
  assert(group.compare("group1") == 0);
  delete bits;
  delete newMode;
  delete mode;
  delete ttlAction;
  delete setAttributeOptions;
}

void GetStatusOptionsTest() {
  GetStatusOptions* getStatusOptions = GetStatusOptions::getDefaultOptions();
  LoadMetadataType* loadMetadataType = new LoadMetadataType(2);
  getStatusOptions->setLoadMetadataType(loadMetadataType);
  delete loadMetadataType;
  loadMetadataType = getStatusOptions->getLoadMetadataType();
  assert(loadMetadataType->getValue() == 2);
  delete loadMetadataType;
  delete getStatusOptions;
}


}

int main(void) {
  alluxio::FileSystem* fileSystem;
  alluxio::LocalAlluxioCluster* miniCluster =
      new alluxio::LocalAlluxioCluster();
  miniCluster->start();
  miniCluster->getClient(&fileSystem);
  alluxio::CreateDirectoryOptionsTest();
  alluxio::CreateFileOptionsTest();
  alluxio::DeleteOptionsTest();
  alluxio::ExistsOptionsTest();
  alluxio::FreeOptionsTest();
  alluxio::ListStatusOptionsTest();
  alluxio::MountOptionsTest();
  alluxio::OpenFileOptionsTest();
  alluxio::SetAttributeOptionsTest();
  alluxio::GetStatusOptionsTest();
  delete miniCluster;
  delete fileSystem;
  return 0;
}

