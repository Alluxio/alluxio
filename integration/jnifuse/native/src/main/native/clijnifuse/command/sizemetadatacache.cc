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

void SizeMetaDataCache::parseArgs() {
  if (argc != 1) {
    cout << "Wrong number args!" << endl;
    cout << getUsage() << endl;
    exit(0);
  }
  mountPoint = (char *) malloc(MAX_FUSE_PATH_LEN);
  strncpy(mountPoint, argv[0], strlen(argv[0]));
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

  ioctl_cmd_data_t* cmd_data = (ioctl_cmd_data_t *)malloc(sizeof(ioctl_cmd_data_t));
  cmd_data->cmd = (int) METADATACACHE_SIZE;
  if (ioctl(fd, FIOC_CMD, cmd_data)) {
    cout << "Clear metadata command run failed!" << endl;
  }
  cout << "Metadata size: " << (char*)cmd_data->data << endl;
  if (tmpFile != NULL) {
    unlink(tmpFile);
  }
  free(cmd_data);
  free(mountPoint);
}