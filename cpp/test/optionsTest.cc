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
#include<stdio.h>

#include <options.h>
#include "fileSystem.h"
#include "localAlluxioCluster.h"

using ::alluxio::CreateDirectoryOptions;
using ::alluxio::CreateFileOptions;
using ::alluxio::FileSystem;
using ::alluxio::LocalAlluxioCluster;
using ::alluxio::TtlAction;
using ::alluxio::Mode;
using ::alluxio::Bits;

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

int main(void) {
  FileSystem* fileSystem;
  LocalAlluxioCluster* miniCluster = new LocalAlluxioCluster();
  miniCluster->start();
  miniCluster->getClient(&fileSystem);
  CreateDirectoryOptionsTest();
  CreateFileOptionsTest();
  delete miniCluster;
  return 0;
}
