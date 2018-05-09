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

#include<string.h>
#include<assert.h>
#include<iostream>

#include <options.h>
#include "fileSystem.h"
#include "localAlluxioCluster.h"

using ::alluxio::CreateDirectoryOptions;
using ::alluxio::CreateFileOptions;
using ::alluxio::MountOptions;
using ::alluxio::ExistsOptions;
using ::alluxio::FileSystem;
using ::alluxio::LocalAlluxioCluster;
using ::alluxio::TtlAction;
using ::alluxio::Mode;
using ::alluxio::Bits;
using ::alluxio::LoadMetadataType;

void CreateDirectoryOptionsTest() {
  CreateDirectoryOptions* createDirectoryOptions;
  createDirectoryOptions = CreateDirectoryOptions::getDefaultOptions();
  bool allowExists = createDirectoryOptions->isAllowExists();
  int64_t ttl = createDirectoryOptions->getTtl();
  bool recursive = createDirectoryOptions->isRecursive();
  createDirectoryOptions->setWriteType(new WriteType(5));
  WriteType* writeType = createDirectoryOptions->getWriteType();
  int wValue = writeType->getValue();
  assert(wValue == 5);
  createDirectoryOptions->setTtlAction(new TtlAction("FREE"));
  TtlAction* ttlAction = createDirectoryOptions->getTtlAction();
  assert(ttlAction->isFree() == true);
  Mode* mode = createDirectoryOptions->getMode();
  mode->setOwnerBits(new Bits("---"));
  createDirectoryOptions->setMode(mode);
  Mode* newMode = createDirectoryOptions->getMode();
  assert(newMode->getOwnerBits()->toString().compare("---") == 0);
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
}

void ExistsOptionsTest() {
  ExistsOptions* existsOptions = ExistsOptions::getDefaultOptions();
  existsOptions->setLoadMetadataType(new LoadMetadataType(1));
  LoadMetadataType* loadMetadataType = existsOptions->getLoadMetadataType();
  assert(loadMetadataType->getValue() == 1);
}

void MountOptionsTest() {
  MountOptions* mountOptions = MountOptions::getDefaultOptions();
  mountOptions->setReadOnly(true);
  bool readOnly = mountOptions->isReadOnly();
  mountOptions->setShared(false);
  bool shared = mountOptions->isShared();
  std::cout << "begin\n";
  std::map<std::string, std::string> properties;
  properties.insert(std::make_pair("what", "none"));
  mountOptions->setProperties(properties);
  std::cout << "end\n";
  std::map<std::string, std::string> nProperties;
  nProperties = mountOptions->getProperties();
  std::map<std::string, std::string>::iterator iter;
  std::cout << nProperties.size() << "\n";
  for (iter = nProperties.begin(); iter != nProperties.end(); iter ++) {
    std::cout << iter->first << ":" << iter->second << "\n";
  }
}

int main(void) {
  FileSystem* fileSystem;
  LocalAlluxioCluster* miniCluster = new LocalAlluxioCluster();
  miniCluster->start();
  miniCluster->getClient(&fileSystem);
  CreateDirectoryOptionsTest();
  CreateFileOptionsTest();
  ExistsOptionsTest();
  MountOptionsTest();
  delete miniCluster;
  return 0;
}
