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

#include <getopt.h>
#include <cassert>
#include <stdlib.h>
#include <sys/stat.h>
#include "../fusecli.h"
#include "include/clear.h"

std::string ClearMetaDataCache::getUsage() const {
  std::string usage = "Usage: fusecli metadatacache clear <mount_point> <relative_path> [-a]\n";
  usage.append("\tSupport options:\n");
  usage.append("\t\t-a                Clear all the metadata of the fuse client.\n");
  usage.append("\tArguments:\n");
  usage.append("\t\tmount_point       Absolute path of the fuse mount point.\n");
  usage.append("\t\trelative_path     Relative path of the fuse mount point that need to clear metadata. For example:\n"
                  "\t\t                  The path /mnt/alluxio-fuse is the mount point, and the relative path is /path/to/clear,\n"
                  "\t\t                  then the metadata of absolute path '/mnt/alluxio-fuse/path/to/clear' will be clear.");
  return usage;
}

void ClearMetaDataCache::parseArgs() {
  int o;
  const char *opt = "a";
  if (argc < 2 || argc > 3) {
    cout << "Wrong number args!" << endl;
    cout << getUsage() << endl;
    exit(0);
  }
  mountPoint = (char *) malloc(MAX_FUSE_PATH_LEN);
  path = (char *) malloc(MAX_FUSE_PATH_LEN);
  strncpy(mountPoint, argv[0], strlen(argv[0]));
  strncpy(path, argv[1], strlen(argv[1]));
  while ((o = getopt(argc, argv, opt)) != -1) {
    switch (o) {
      case 'a':
        all = true;
        break;
      default:
        cout << "Invalid arguments!" << endl;
        cout << getUsage() << endl;
        break;
    }
  }
}

void ClearMetaDataCache::run() {
  struct stat s_buf;
  char *absPath;
  char *tmpPath;
  int fd;

  // Get the absolute path
  assert(strlen(mountPoint) + strlen(path) + 1 < MAX_FUSE_PATH_LEN);
  if (mountPoint[strlen(mountPoint) - 1] != '/' ) {
    strcat(mountPoint, "/");
  }
  if (path[0] == '/') {
    tmpPath = path + 1;
  }

  absPath = strcat(mountPoint, tmpPath);
  if (stat(absPath,&s_buf)) {
    cout << "Invalid file path! path=" << absPath << endl;
    exit(0);
  }
  char *tmpFile = NULL;
  if (S_ISDIR(s_buf.st_mode)) {
    if (absPath[strlen(absPath)- 1] != '/') {
      strcat(absPath, "/");
    }
    tmpFile = strcat(absPath, IOC_TMP_FILE);
    fd = open(tmpFile, O_CREAT | O_RDWR);
  } else {
    fd = open(absPath, O_RDWR);
  }

  if (fd < 0) {
    cout << "Open file failed!" << endl;
    exit(0);
  }

  ioctl_cmd_data_t* cmd_data = (ioctl_cmd_data_t *)malloc(sizeof(ioctl_cmd_data_t));
  cmd_data->cmd = (int) METADATACACHE_CLEAR;
  if (all) {
    strncpy(cmd_data->data, "a", 2);
  }
  if (ioctl(fd, FIOC_CMD, cmd_data)) {
    cout << "Clear metadata command run failed!" << endl;
  }
  if (tmpFile != NULL) {
    unlink(tmpFile);
  }
  free(cmd_data);
  free(path);
  free(mountPoint);
}