/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

#include <cassert>
#include <sys/stat.h>
#include "../fusecli.h"
#include "include/size.h"

std::string SizeMetaDataCache::getUsage() const {
  std::string usage = "Usage: fusecli metadatacache size <mount_point>\n";
  usage.append("\tSupport options:\n");
  usage.append("\tArguments:\n");
  usage.append("\t\tmount_point       Absolute path of the fuse mount point.\n");
  return usage;
}

void SizeMetaDataCache::parseOptions() {
  if (argc != 1) {
    cout << "Wrong number args!" << endl;
    cout << getUsage() << endl;
    exit(0);
  }
  mountPoint = argv[0];
}

void SizeMetaDataCache::run() {
  struct stat s_buf;
  int fd;

  // Get the absolute path
  assert(strlen(mountPoint) + 1 < MAX_FUSE_PATH_LEN);
  if (mountPoint[strlen(mountPoint) - 1] != '/' ) {
    strcat(mountPoint, "/");
  }

  if (stat(mountPoint, &s_buf)) {
    cout << "Invalid file path! path=" << mountPoint << endl;
    exit(0);
  }
  char *tmpFile = NULL;
  tmpFile = strcat(mountPoint, IOC_TMP_FILE);
  fd = open(tmpFile, O_CREAT | O_RDWR);

  if (fd < 0) {
    cout << "Open file failed!" << endl;
    exit(0);
  }

  void* data = malloc(IOC_DATA_MAX_LENGTH);
  if (ioctl(fd, FIOC_GET_METADATA_SIZE, data)) {
    cout << "Clear metadata command run failed!" << endl;
  }
  cout << "Metadata size: " << (char*)data << endl;
  if (tmpFile != NULL) {
    if (remove(tmpFile)) {
      cout << "Remove tmp file " << tmpFile << " failed, please remove it." << endl;
    }
  }
  free(data);
}